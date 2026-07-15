from __future__ import annotations

import asyncio
import threading

import pytest

from app.auth.api_keys import ApiKeyAuthorizationError
from app.auth.permissions import PermissionDenied
from app.db.connection import connect_database


def _queued_message_id(result: str) -> str:
    return str(result).rsplit(" ", 1)[-1]


def _message_state(runtime, message_id: str) -> tuple[str, list[tuple[str, str]]]:
    with connect_database(runtime.settings.database_path) as connection:
        message = connection.execute(
            "SELECT parse_status FROM messages WHERE id = ?",
            (message_id,),
        ).fetchone()
        deliveries = connection.execute(
            """
            SELECT id, status
            FROM message_deliveries
            WHERE message_id = ?
            ORDER BY id ASC
            """,
            (message_id,),
        ).fetchall()
    assert message is not None
    return str(message["parse_status"]), [
        (str(row["id"]), str(row["status"])) for row in deliveries
    ]


async def _run_behind_message_writer_latch(runtime, call_factory, external_change):
    entered = threading.Event()
    release = threading.Event()
    queued = asyncio.Event()
    real_writer = runtime.writer
    real_service_runtime = runtime.messages._runtime

    class ObservedWriter:
        async def execute(self, operation):
            submitted = asyncio.create_task(real_writer.execute(operation))
            while not submitted.done():
                with real_writer._queue.mutex:
                    is_queued = any(
                        getattr(item, "operation", None) is operation
                        for item in real_writer._queue.queue
                    )
                if is_queued:
                    queued.set()
                    break
                await asyncio.sleep(0.001)
            return await submitted

    class MessageRuntimeProxy:
        writer = ObservedWriter()

        def __getattr__(self, name):
            return getattr(real_service_runtime, name)

    def blocker(_connection) -> None:
        entered.set()
        if not release.wait(timeout=5):
            raise TimeoutError("writer latch was not released")

    blocker_task = asyncio.create_task(real_writer.execute(blocker))
    assert await asyncio.wait_for(asyncio.to_thread(entered.wait, 2), timeout=3)
    runtime.messages._runtime = MessageRuntimeProxy()
    mutation_task = asyncio.create_task(call_factory())
    try:
        await asyncio.wait_for(queued.wait(), timeout=3)
        await asyncio.to_thread(external_change)
    finally:
        release.set()
        await asyncio.wait_for(blocker_task, timeout=3)
        runtime.messages._runtime = real_service_runtime
    return await asyncio.wait_for(mutation_task, timeout=3)


@pytest.mark.asyncio
async def test_reparse_reauthorizes_after_queued_cross_process_key_revocation(
    runtime,
    sample_email_bytes: bytes,
) -> None:
    await runtime.create_domain("reparse-auth.example")
    message_id = _queued_message_id(
        await runtime.accept_message(
            rcpt_tos=["box@reparse-auth.example"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
    )
    await runtime.drain_parser_queue()
    actor = await runtime.api_keys.create_key(
        name="queued-message-reparser",
        kind="admin",
        scopes=["messages.write"],
        domain_ids=[],
        domain_grant_mode="all",
        mailbox_patterns=[],
    )
    stale_actor = runtime.api_keys.authenticate_plain_text(actor["plain_text"])

    def revoke_actor() -> None:
        with connect_database(
            runtime.settings.database_path,
            durable_writes=True,
        ) as connection:
            connection.execute(
                "UPDATE api_keys SET status = 'revoked' WHERE id = ?",
                (actor["id"],),
            )

    with pytest.raises(ApiKeyAuthorizationError, match="no longer active"):
        await _run_behind_message_writer_latch(
            runtime,
            lambda: runtime.messages.reparse_message(
                message_id,
                authorization_principal=stale_actor,
            ),
            revoke_actor,
        )

    parse_status, deliveries = _message_state(runtime, message_id)
    assert parse_status == "parsed"
    assert all(status == "active" for _delivery_id, status in deliveries)
    assert runtime.parse_queue.contains(message_id) is False


@pytest.mark.asyncio
async def test_delivery_delete_reloads_removed_domain_and_mailbox_grants(
    runtime,
    sample_email_bytes: bytes,
) -> None:
    domain = await runtime.create_domain("delete-auth.example")
    message_id = _queued_message_id(
        await runtime.accept_message(
            rcpt_tos=["box@delete-auth.example"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
    )
    await runtime.drain_parser_queue()
    delivery_id = _message_state(runtime, message_id)[1][0][0]

    domain_actor = await runtime.api_keys.create_key(
        name="removed-domain-message-deleter",
        kind="admin",
        scopes=["messages.write"],
        domain_ids=[domain["id"]],
        domain_grant_mode="selected",
        mailbox_patterns=[],
    )
    stale_domain_actor = runtime.api_keys.authenticate_plain_text(
        domain_actor["plain_text"]
    )
    with connect_database(
        runtime.settings.database_path,
        durable_writes=True,
    ) as connection:
        connection.execute(
            "DELETE FROM api_key_domain_grants WHERE api_key_id = ?",
            (domain_actor["id"],),
        )

    with pytest.raises(PermissionDenied, match="domain grant missing"):
        await runtime.messages.soft_delete_delivery(
            delivery_id,
            authorization_principal=stale_domain_actor,
        )
    assert _message_state(runtime, message_id)[1] == [(delivery_id, "active")]

    mailbox_actor = await runtime.api_keys.create_key(
        name="narrowed-mailbox-message-deleter",
        kind="admin",
        scopes=["messages.write"],
        domain_ids=[domain["id"]],
        domain_grant_mode="selected",
        mailbox_patterns=["*@delete-auth.example"],
    )
    stale_mailbox_actor = runtime.api_keys.authenticate_plain_text(
        mailbox_actor["plain_text"]
    )
    with connect_database(
        runtime.settings.database_path,
        durable_writes=True,
    ) as connection:
        connection.execute(
            "DELETE FROM api_key_mailbox_grants WHERE api_key_id = ?",
            (mailbox_actor["id"],),
        )
        connection.execute(
            """
            INSERT INTO api_key_mailbox_grants (api_key_id, mailbox_pattern)
            VALUES (?, 'other@delete-auth.example')
            """,
            (mailbox_actor["id"],),
        )

    with pytest.raises(PermissionDenied, match="mailbox grant missing"):
        await runtime.messages.soft_delete_delivery(
            delivery_id,
            authorization_principal=stale_mailbox_actor,
        )
    assert _message_state(runtime, message_id)[1] == [(delivery_id, "active")]


@pytest.mark.asyncio
async def test_shared_message_and_mixed_bulk_delete_are_atomic_across_domain_grants(
    runtime,
    sample_email_bytes: bytes,
) -> None:
    allowed_domain = await runtime.create_domain("allowed-message.example")
    await runtime.create_domain("denied-message.example")
    message_id = _queued_message_id(
        await runtime.accept_message(
            rcpt_tos=[
                "one@allowed-message.example",
                "two@denied-message.example",
            ],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
    )
    await runtime.drain_parser_queue()
    actor = await runtime.api_keys.create_key(
        name="bounded-shared-message-writer",
        kind="admin",
        scopes=["messages.write"],
        domain_ids=[allowed_domain["id"]],
        domain_grant_mode="selected",
        mailbox_patterns=[],
    )
    principal = runtime.api_keys.authenticate_plain_text(actor["plain_text"])
    initial_status, initial_deliveries = _message_state(runtime, message_id)
    delivery_ids = [delivery_id for delivery_id, _status in initial_deliveries]

    with pytest.raises(PermissionDenied, match="domain grant missing"):
        await runtime.messages.reparse_message(
            message_id,
            authorization_principal=principal,
        )
    with pytest.raises(PermissionDenied, match="domain grant missing"):
        await runtime.messages.soft_delete_message(
            message_id,
            authorization_principal=principal,
        )
    with pytest.raises(PermissionDenied, match="domain grant missing"):
        await runtime.messages.soft_delete_deliveries(
            delivery_ids,
            authorization_principal=principal,
        )

    assert _message_state(runtime, message_id) == (
        initial_status,
        initial_deliveries,
    )
    assert runtime.parse_queue.contains(message_id) is False


@pytest.mark.asyncio
async def test_v2_reparse_returns_403_when_key_is_revoked_after_route_precheck(
    app_client,
    runtime,
    sample_email_bytes: bytes,
    monkeypatch,
) -> None:
    await runtime.create_domain("v2-message-race.example")
    message_id = _queued_message_id(
        await runtime.accept_message(
            rcpt_tos=["box@v2-message-race.example"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
    )
    await runtime.drain_parser_queue()
    actor = await runtime.api_keys.create_key(
        name="v2-message-race-writer",
        kind="admin",
        scopes=["messages.read", "messages.write"],
        domain_ids=[],
        domain_grant_mode="all",
        mailbox_patterns=[],
    )
    real_reparse = runtime.messages.reparse_message

    async def revoke_after_precheck(message_id: str, *, authorization_principal=None):
        def revoke() -> None:
            with connect_database(
                runtime.settings.database_path,
                durable_writes=True,
            ) as connection:
                connection.execute(
                    "UPDATE api_keys SET status = 'revoked' WHERE id = ?",
                    (actor["id"],),
                )

        await asyncio.to_thread(revoke)
        return await real_reparse(
            message_id,
            authorization_principal=authorization_principal,
        )

    monkeypatch.setattr(runtime.messages, "reparse_message", revoke_after_precheck)
    response = await app_client.post(
        f"/api/v2/messages/{message_id}/reparse",
        headers={"Authorization": f"Bearer {actor['plain_text']}"},
    )

    assert response.status_code == 403
    assert response.json()["code"] == "message_authorization_changed"
    assert _message_state(runtime, message_id)[0] == "parsed"
    assert runtime.parse_queue.contains(message_id) is False

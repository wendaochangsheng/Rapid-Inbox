from __future__ import annotations

import asyncio
import json
from pathlib import Path
import threading

import pytest

from app.auth.api_keys import set_active_permission_context
from app.auth.permissions import PermissionDenied
from app.config import Settings
from app.runtime import RapidInboxRuntime
from app.smtp.matcher import DomainMatcher, DomainRule
from conftest import connect_database


def test_domain_matcher_prefers_exact_rule_over_catch_all() -> None:
    matcher = DomainMatcher(
        [
            DomainRule(
                domain_id=1,
                root_domain_ascii="*",
                accept_exact=True,
                accept_subdomains=True,
            ),
            DomainRule(
                domain_id=2,
                root_domain_ascii="managed.example",
                accept_exact=True,
                accept_subdomains=True,
                plus_addressing_mode="strip",
            ),
        ]
    )

    exact = matcher.match_address("Foo+tag@managed.example")
    fallback = matcher.match_address("Foo+tag@unmanaged.example")

    assert exact is not None
    assert exact.domain_id == 2
    assert exact.root_domain_ascii == "managed.example"
    assert exact.address_canonical == "foo@managed.example"
    assert fallback is not None
    assert fallback.domain_id == 1
    assert fallback.root_domain_ascii == "*"
    assert fallback.address_canonical == "foo+tag@unmanaged.example"


@pytest.mark.asyncio
@pytest.mark.parametrize("policy_change", ["rename", "delete", "disable"])
async def test_data_commit_fails_closed_when_accepted_domain_policy_disappears(
    tmp_path,
    sample_email_bytes: bytes,
    monkeypatch,
    policy_change: str,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
        ingress_mode="managed_only",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    domain = await runtime.create_domain("commit-race.example")
    artifacts_written = threading.Event()
    release_commit = threading.Event()
    real_write_artifacts = runtime._write_accept_artifacts
    accept_task: asyncio.Task[str] | None = None

    def block_after_artifact_publish(*args, **kwargs) -> None:
        real_write_artifacts(*args, **kwargs)
        artifacts_written.set()
        if not release_commit.wait(timeout=5):
            raise TimeoutError("test did not release DATA commit")

    monkeypatch.setattr(runtime, "_write_accept_artifacts", block_after_artifact_publish)
    try:
        accept_task = asyncio.create_task(
            runtime.accept_message(
                rcpt_tos=["box@commit-race.example"],
                envelope_from="sender@example.com",
                content=sample_email_bytes,
            )
        )
        assert await asyncio.wait_for(
            asyncio.to_thread(artifacts_written.wait, 2),
            timeout=3,
        )
        assert len(list(settings.raw_dir.rglob("*.eml"))) == 1
        assert len(list(settings.manifests_dir.rglob("*.json"))) == 1

        if policy_change == "rename":
            await runtime.domains.update_domain(
                domain["id"],
                {"root_domain": "renamed-race.example"},
            )
        elif policy_change == "delete":
            await runtime.domains.delete_domain(domain["id"])
        else:
            await runtime.domains.update_domain(domain["id"], {"is_active": False})

        release_commit.set()
        response = await asyncio.wait_for(accept_task, timeout=5)
        assert response == "451 recipient policy changed; retry later"

        with connect_database(settings.database_path) as connection:
            counts = {
                table: int(
                    connection.execute(
                        f"SELECT COUNT(*) AS count FROM {table}"
                    ).fetchone()["count"]
                )
                for table in ("messages", "message_deliveries", "mailboxes")
            }
        assert counts == {"messages": 0, "message_deliveries": 0, "mailboxes": 0}
        assert list(settings.raw_dir.rglob("*.eml")) == []
        assert list(settings.manifests_dir.rglob("*.json")) == []
    finally:
        release_commit.set()
        if accept_task is not None and not accept_task.done():
            await asyncio.wait_for(accept_task, timeout=5)
        await runtime.stop()


@pytest.mark.asyncio
async def test_data_policy_rejection_returns_451_when_artifact_cleanup_fails(
    tmp_path,
    sample_email_bytes: bytes,
    monkeypatch,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    domain = await runtime.create_domain("cleanup-race.example")
    artifacts_written = threading.Event()
    release_commit = threading.Event()
    real_write_artifacts = runtime._write_accept_artifacts
    accept_task: asyncio.Task[str] | None = None

    def block_after_artifact_publish(*args, **kwargs) -> None:
        real_write_artifacts(*args, **kwargs)
        artifacts_written.set()
        if not release_commit.wait(timeout=5):
            raise TimeoutError("test did not release DATA commit")

    monkeypatch.setattr(runtime, "_write_accept_artifacts", block_after_artifact_publish)
    try:
        accept_task = asyncio.create_task(
            runtime.accept_message(
                rcpt_tos=["box@cleanup-race.example"],
                envelope_from="sender@example.com",
                content=sample_email_bytes,
            )
        )
        assert await asyncio.wait_for(
            asyncio.to_thread(artifacts_written.wait, 2),
            timeout=3,
        )
        await runtime.domains.update_domain(
            domain["id"],
            {"root_domain": "cleanup-race-renamed.example"},
        )

        real_unlink = Path.unlink

        def fail_accept_artifact_unlink(path: Path, *args, **kwargs) -> None:
            if path.suffix in {".eml", ".json"} and settings.storage_root in path.parents:
                raise PermissionError(f"cleanup denied: {path}")
            real_unlink(path, *args, **kwargs)

        with monkeypatch.context() as cleanup_patch:
            cleanup_patch.setattr(Path, "unlink", fail_accept_artifact_unlink)
            release_commit.set()
            response = await asyncio.wait_for(accept_task, timeout=5)

        assert response == "451 recipient policy changed; retry later"
        assert len(list(settings.raw_dir.rglob("*.eml"))) == 1
        assert len(list(settings.manifests_dir.rglob("*.json"))) == 1
        with connect_database(settings.database_path) as connection:
            assert connection.execute("SELECT 1 FROM messages").fetchone() is None
    finally:
        release_commit.set()
        if accept_task is not None and not accept_task.done():
            await asyncio.wait_for(accept_task, timeout=5)
        await runtime.stop()

    restarted = RapidInboxRuntime(settings)
    await restarted.start()
    try:
        assert list(settings.manifests_dir.rglob("*.json")) == []
        assert len(
            list((settings.storage_root / "quarantine" / "manifests").glob("*.json"))
        ) == 1
        assert len(list(settings.raw_dir.rglob("*.eml"))) == 1
    finally:
        await restarted.stop()


@pytest.mark.asyncio
async def test_ingress_mode_is_private_by_default_and_switches_without_restart(
    tmp_path,
    sample_email_bytes: bytes,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
        ingress_mode="managed_only",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        managed = await runtime.create_domain(
            "managed.example",
            plus_addressing_mode="strip",
            public_web_enabled=True,
            public_api_enabled=True,
        )
        assert runtime.domains.match_address("box@unmanaged.example") is None
        with pytest.raises(ValueError, match="recipient domain not allowed"):
            await runtime.accept_message(
                rcpt_tos=["box@unmanaged.example"],
                envelope_from="sender@example.com",
                content=sample_email_bytes,
            )

        updated = await runtime.system_settings.update_settings(
            {"ingress_mode": "managed_plus_catchall"}
        )
        assert updated["ingress_mode"] == "managed_plus_catchall"
        catch_all_match = runtime.domains.match_address("Box@unmanaged.example")
        exact_match = runtime.domains.match_address("Foo+tag@managed.example")
        assert catch_all_match is not None
        assert catch_all_match.root_domain_ascii == "*"
        assert exact_match is not None
        assert exact_match.domain_id == managed["id"]
        assert exact_match.address_canonical == "foo@managed.example"

        with connect_database(settings.database_path) as connection:
            catch_all = connection.execute(
                """
                SELECT id, is_active, public_web_enabled, public_api_enabled
                FROM domains
                WHERE root_domain_ascii = '*'
                """
            ).fetchone()
        assert dict(catch_all) == {
            "id": catch_all["id"],
            "is_active": 1,
            "public_web_enabled": 0,
            "public_api_enabled": 0,
        }

        accepted = await runtime.accept_message(
            rcpt_tos=["Box@unmanaged.example"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
        assert accepted.startswith("250 queued as ")
        await runtime.drain_parser_queue()
        with connect_database(settings.database_path) as connection:
            catch_all_mailbox = connection.execute(
                """
                SELECT mb.public_enabled, d.public_web_enabled, d.public_api_enabled
                FROM mailboxes AS mb
                JOIN domains AS d ON d.id = mb.domain_id
                WHERE mb.address_canonical = 'box@unmanaged.example'
                """
            ).fetchone()
        assert dict(catch_all_mailbox) == {
            "public_enabled": 1,
            "public_web_enabled": 0,
            "public_api_enabled": 0,
        }

        await runtime.system_settings.update_settings({"ingress_mode": "managed_only"})
        assert runtime.domains.match_address("box@another-unmanaged.example") is None
        assert runtime.domains.match_address("box@managed.example") is not None
        with connect_database(settings.database_path) as connection:
            catch_all_active = connection.execute(
                "SELECT is_active FROM domains WHERE root_domain_ascii = '*'"
            ).fetchone()["is_active"]
        assert catch_all_active == 0

        await runtime.system_settings.update_settings({"ingress_mode": "managed_plus_catchall"})
        assert runtime.domains.match_address("box@another-unmanaged.example") is not None
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_catch_all_domain_is_reserved_for_ingress_settings(tmp_path) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        with pytest.raises(ValueError, match="managed through ingress settings"):
            await runtime.create_domain("*")

        managed = await runtime.create_domain("managed-reserved.example")
        with pytest.raises(ValueError, match="managed through ingress settings"):
            await runtime.domains.update_domain(managed["id"], {"root_domain": "*"})

        await runtime.system_settings.update_settings({"ingress_mode": "managed_plus_catchall"})
        with connect_database(settings.database_path) as connection:
            catch_all_id = int(
                connection.execute(
                    "SELECT id FROM domains WHERE root_domain_ascii = '*'"
                ).fetchone()["id"]
            )

        with pytest.raises(ValueError, match="managed through ingress settings"):
            await runtime.domains.update_domain(catch_all_id, {"public_api_enabled": True})
        with pytest.raises(ValueError, match="managed through ingress settings"):
            await runtime.domains.delete_domain(catch_all_id)

        await runtime.system_settings.update_settings(
            {
                "catch_all_public_api_enabled": True,
                "ingress_mode": "managed_plus_catchall",
            }
        )
        assert runtime.domains.get_domain(catch_all_id)["public_api_enabled"] is True
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_managed_domain_creation_rehomes_and_merges_catch_all_mailboxes(
    tmp_path,
    sample_email_bytes: bytes,
    monkeypatch,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
        ingress_mode="managed_plus_catchall",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        with connect_database(settings.database_path) as connection:
            catch_all_id = int(
                connection.execute(
                    "SELECT id FROM domains WHERE root_domain_ascii = '*'"
                ).fetchone()["id"]
            )

        # The first message creates two fallback mailboxes that will collapse
        # to one canonical mailbox after plus stripping.  This also exercises
        # the UNIQUE(message_id, mailbox_id) conflict path during the merge.
        await runtime.accept_message(
            rcpt_tos=["Foo+tag@tenant.example", "foo@tenant.example"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
        await runtime.accept_message(
            rcpt_tos=["foo+other@tenant.example"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )

        with connect_database(settings.database_path) as connection:
            before = connection.execute(
                """
                SELECT COUNT(*) AS mailbox_count, SUM(message_count) AS message_count
                FROM mailboxes
                WHERE domain_id = ? AND rcpt_domain_ascii = 'tenant.example'
                """,
                (catch_all_id,),
            ).fetchone()
        assert dict(before) == {"mailbox_count": 3, "message_count": 3}

        stale_catch_all_matches = {
            address: runtime.domains.match_address(address)
            for address in ("race+tag@tenant.example", "race@tenant.example")
        }
        assert all(match is not None for match in stale_catch_all_matches.values())
        assert all(
            match.root_domain_ascii == "*"
            for match in stale_catch_all_matches.values()
            if match is not None
        )

        managed = await runtime.create_domain(
            "tenant.example",
            plus_addressing_mode="strip",
            public_web_enabled=True,
            public_api_enabled=True,
        )

        with connect_database(settings.database_path) as connection:
            mailboxes = connection.execute(
                """
                SELECT id, domain_id, local_part_canonical, rcpt_domain_ascii,
                       address_canonical, message_count
                FROM mailboxes
                WHERE rcpt_domain_ascii = 'tenant.example'
                ORDER BY id ASC
                """
            ).fetchall()
            deliveries = connection.execute(
                """
                SELECT d.id, d.message_id, d.mailbox_id
                FROM message_deliveries AS d
                JOIN mailboxes AS m ON m.id = d.mailbox_id
                WHERE m.rcpt_domain_ascii = 'tenant.example'
                ORDER BY d.message_id ASC, d.id ASC
                """
            ).fetchall()
            audit = connection.execute(
                """
                SELECT details_json
                FROM audit_logs
                WHERE action = 'mailboxes.rehome'
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()

        assert [dict(row) for row in mailboxes] == [
            {
                "id": mailboxes[0]["id"],
                "domain_id": managed["id"],
                "local_part_canonical": "foo",
                "rcpt_domain_ascii": "tenant.example",
                "address_canonical": "foo@tenant.example",
                "message_count": 2,
            }
        ]
        assert len(deliveries) == 2
        assert len({row["message_id"] for row in deliveries}) == 2
        assert all(int(row["mailbox_id"]) == int(mailboxes[0]["id"]) for row in deliveries)
        assert audit is not None
        assert '"deliveries_moved":1' in str(audit["details_json"])
        assert '"deliveries_deduplicated":1' in str(audit["details_json"])

        managed_key = await runtime.api_keys.create_key(
            name="managed-tenant-reader",
            kind="public",
            scopes=["public.read"],
            domain_ids=[managed["id"]],
            mailbox_patterns=[],
            domain_grant_mode="selected",
        )
        catch_all_key = await runtime.api_keys.create_key(
            name="catch-all-reader",
            kind="public",
            scopes=["public.read"],
            domain_ids=[catch_all_id],
            mailbox_patterns=[],
            domain_grant_mode="selected",
        )

        set_active_permission_context(runtime.api_keys.authenticate_plain_text(managed_key["plain_text"]))
        visible = await runtime.get_mailbox_view("Foo+new@tenant.example", surface="api")
        assert visible["mailbox"] == "foo@tenant.example"
        assert visible["message_count"] == 2

        set_active_permission_context(runtime.api_keys.authenticate_plain_text(catch_all_key["plain_text"]))
        with pytest.raises(PermissionDenied, match="domain grant missing"):
            await runtime.get_mailbox_view("foo@tenant.example")

        # Simulate an SMTP session that accepted RCPT with a stale catch-all
        # snapshot before the domain transaction committed.  The final write
        # transaction must resolve the current managed owner.
        current_match_address = runtime.domains.match_address
        with monkeypatch.context() as patch:
            patch.setattr(
                runtime.domains,
                "match_address",
                lambda address: (
                    stale_catch_all_matches.get(address.lower())
                    or current_match_address(address)
                ),
            )
            await runtime.accept_message(
                rcpt_tos=["race+tag@tenant.example", "race@tenant.example"],
                envelope_from="sender@example.com",
                content=sample_email_bytes,
            )

        with connect_database(settings.database_path) as connection:
            race_owner = connection.execute(
                """
                SELECT
                    m.domain_id,
                    d.root_domain_ascii,
                    m.message_count,
                    COUNT(delivery.id) AS delivery_count
                FROM mailboxes AS m
                JOIN domains AS d ON d.id = m.domain_id
                LEFT JOIN message_deliveries AS delivery ON delivery.mailbox_id = m.id
                WHERE m.address_canonical = 'race@tenant.example'
                GROUP BY m.id
                """
            ).fetchone()
        assert dict(race_owner) == {
            "domain_id": managed["id"],
            "root_domain_ascii": "tenant.example",
            "message_count": 1,
            "delivery_count": 1,
        }

        # Disabling the managed rule makes routing fall back to `*`, but the
        # fallback must not steal an already-managed mailbox on the next write.
        await runtime.domains.update_domain(managed["id"], {"is_active": False})
        fallback_again = runtime.domains.match_address("race@tenant.example")
        assert fallback_again is not None
        assert fallback_again.root_domain_ascii == "*"
        await runtime.accept_message(
            rcpt_tos=["race@tenant.example"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
        with connect_database(settings.database_path) as connection:
            still_managed = connection.execute(
                """
                SELECT m.domain_id, d.root_domain_ascii, m.message_count
                FROM mailboxes AS m
                JOIN domains AS d ON d.id = m.domain_id
                WHERE m.address_canonical = 'race@tenant.example'
                """
            ).fetchone()
        assert dict(still_managed) == {
            "domain_id": managed["id"],
            "root_domain_ascii": "tenant.example",
            "message_count": 2,
        }
        await runtime.domains.update_domain(managed["id"], {"is_active": True})
    finally:
        set_active_permission_context(None)
        await runtime.stop()


@pytest.mark.asyncio
async def test_more_specific_managed_domain_promotes_parent_mailboxes_and_permissions(
    tmp_path,
    sample_email_bytes: bytes,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        parent = await runtime.create_domain(
            "parent.example",
            accept_subdomains=True,
            plus_addressing_mode="keep",
            public_api_enabled=True,
        )
        await runtime.accept_message(
            rcpt_tos=["Box+tag@child.parent.example", "box@child.parent.example"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )

        child = await runtime.create_domain(
            "child.parent.example",
            plus_addressing_mode="strip",
            public_api_enabled=True,
        )
        with connect_database(settings.database_path) as connection:
            mailboxes = connection.execute(
                """
                SELECT m.id, m.domain_id, m.address_canonical, m.message_count,
                       d.root_domain_ascii
                FROM mailboxes AS m
                JOIN domains AS d ON d.id = m.domain_id
                WHERE m.rcpt_domain_ascii = 'child.parent.example'
                ORDER BY m.id ASC
                """
            ).fetchall()
            deliveries = connection.execute(
                """
                SELECT delivery.message_id, delivery.mailbox_id
                FROM message_deliveries AS delivery
                JOIN mailboxes AS mailbox ON mailbox.id = delivery.mailbox_id
                WHERE mailbox.rcpt_domain_ascii = 'child.parent.example'
                """
            ).fetchall()

        assert [dict(row) for row in mailboxes] == [
            {
                "id": mailboxes[0]["id"],
                "domain_id": child["id"],
                "address_canonical": "box@child.parent.example",
                "message_count": 1,
                "root_domain_ascii": "child.parent.example",
            }
        ]
        assert len(deliveries) == 1
        assert int(deliveries[0]["mailbox_id"]) == int(mailboxes[0]["id"])

        child_key = await runtime.api_keys.create_key(
            name="child-reader",
            kind="public",
            scopes=["public.read"],
            domain_ids=[child["id"]],
            mailbox_patterns=[],
            domain_grant_mode="selected",
        )
        parent_key = await runtime.api_keys.create_key(
            name="parent-reader",
            kind="public",
            scopes=["public.read"],
            domain_ids=[parent["id"]],
            mailbox_patterns=[],
            domain_grant_mode="selected",
        )

        set_active_permission_context(runtime.api_keys.authenticate_plain_text(child_key["plain_text"]))
        view = await runtime.get_mailbox_view("Box+new@child.parent.example", surface="api")
        assert view["mailbox"] == "box@child.parent.example"

        set_active_permission_context(runtime.api_keys.authenticate_plain_text(parent_key["plain_text"]))
        with pytest.raises(PermissionDenied, match="domain grant missing"):
            await runtime.get_mailbox_view("box@child.parent.example", surface="api")
    finally:
        set_active_permission_context(None)
        await runtime.stop()


@pytest.mark.asyncio
async def test_domain_canonical_policy_update_merges_existing_mailboxes(
    tmp_path,
    sample_email_bytes: bytes,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        domain = await runtime.create_domain("policy.example", plus_addressing_mode="keep")
        await runtime.accept_message(
            rcpt_tos=["User+tag@policy.example", "user@policy.example"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )

        await runtime.domains.update_domain(
            domain["id"],
            {"plus_addressing_mode": "strip"},
        )
        with connect_database(settings.database_path) as connection:
            mailboxes = connection.execute(
                """
                SELECT id, domain_id, address_canonical, message_count
                FROM mailboxes
                WHERE rcpt_domain_ascii = 'policy.example'
                """
            ).fetchall()
            deliveries = connection.execute(
                """
                SELECT delivery.message_id, delivery.mailbox_id
                FROM message_deliveries AS delivery
                JOIN mailboxes AS mailbox ON mailbox.id = delivery.mailbox_id
                WHERE mailbox.rcpt_domain_ascii = 'policy.example'
                """
            ).fetchall()

        assert [dict(row) for row in mailboxes] == [
            {
                "id": mailboxes[0]["id"],
                "domain_id": domain["id"],
                "address_canonical": "user@policy.example",
                "message_count": 1,
            }
        ]
        assert len(deliveries) == 1
        assert int(deliveries[0]["mailbox_id"]) == int(mailboxes[0]["id"])
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_manifest_recovery_promotes_existing_catch_all_mailbox(
    tmp_path,
    sample_email_bytes: bytes,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
        ingress_mode="managed_plus_catchall",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        managed = await runtime.create_domain("recovered.example")
        response = await runtime.accept_message(
            rcpt_tos=["box@recovered.example"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
        message_id = response.removeprefix("250 queued as ")
        with connect_database(settings.database_path) as connection:
            catch_all_id = int(
                connection.execute(
                    "SELECT id FROM domains WHERE root_domain_ascii = '*'"
                ).fetchone()["id"]
            )
            mailbox_id = int(
                connection.execute(
                    "SELECT id FROM mailboxes WHERE address_canonical = 'box@recovered.example'"
                ).fetchone()["id"]
            )
    finally:
        await runtime.stop()

    # Simulate an old deployment that left the mailbox under catch-all while
    # the durable manifest correctly records the managed recipient.  The
    # one-time startup sweep is already marked complete, so recovery itself
    # must perform this ownership repair.
    with connect_database(settings.database_path) as connection:
        connection.execute("DELETE FROM messages WHERE id = ?", (message_id,))
        connection.execute(
            """
            UPDATE mailboxes
            SET domain_id = ?, message_count = 0, latest_message_at = NULL
            WHERE id = ?
            """,
            (catch_all_id, mailbox_id),
        )
        connection.commit()

    recovered = RapidInboxRuntime(settings)
    await recovered.start()
    try:
        with connect_database(settings.database_path) as connection:
            owner = connection.execute(
                """
                SELECT m.id, m.domain_id, d.root_domain_ascii, m.message_count
                FROM mailboxes AS m
                JOIN domains AS d ON d.id = m.domain_id
                WHERE m.address_canonical = 'box@recovered.example'
                """
            ).fetchone()
            delivery_count = int(
                connection.execute(
                    "SELECT COUNT(*) AS count FROM message_deliveries WHERE message_id = ?",
                    (message_id,),
                ).fetchone()["count"]
            )
            audit = connection.execute(
                """
                SELECT details_json
                FROM audit_logs
                WHERE action = 'mailboxes.rehome' AND actor_ref = 'recovery'
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()

        assert dict(owner) == {
            "id": mailbox_id,
            "domain_id": managed["id"],
            "root_domain_ascii": "recovered.example",
            "message_count": 1,
        }
        assert delivery_count == 1
        assert audit is not None
        assert json.loads(str(audit["details_json"]))["reason"] == "manifest.recovery"
    finally:
        await recovered.stop()

from __future__ import annotations

import asyncio
import threading

import pytest

from conftest import connect_database


@pytest.mark.asyncio
async def test_mail_accept_gate_does_not_serialize_normal_ingest(runtime, monkeypatch) -> None:
    both_entered = asyncio.Event()
    release = asyncio.Event()
    entered = 0

    async def controlled_accept(**_kwargs) -> str:
        nonlocal entered
        entered += 1
        if entered == 2:
            both_entered.set()
        await release.wait()
        return f"250 queued as test-{entered}"

    monkeypatch.setattr(runtime, "_accept_message_operation", controlled_accept)
    first = asyncio.create_task(
        runtime.accept_message(rcpt_tos=["a@example.com"], envelope_from=None, content=b"first")
    )
    second = asyncio.create_task(
        runtime.accept_message(rcpt_tos=["b@example.com"], envelope_from=None, content=b"second")
    )

    await asyncio.wait_for(both_entered.wait(), timeout=1)
    assert runtime._active_mail_accepts == 2
    release.set()
    await asyncio.gather(first, second)
    assert runtime._active_mail_accepts == 0


@pytest.mark.asyncio
async def test_clear_mail_waits_for_active_accept_and_blocks_new_accept(
    runtime,
    sample_email_bytes: bytes,
    monkeypatch,
) -> None:
    await runtime.create_domain("maintenance-gate.example")
    real_accept = runtime._accept_message_operation
    first_entered = asyncio.Event()
    release_first = asyncio.Event()
    second_entered = asyncio.Event()
    accept_calls = 0

    async def controlled_accept(**kwargs) -> str:
        nonlocal accept_calls
        accept_calls += 1
        if accept_calls == 1:
            first_entered.set()
            await release_first.wait()
        else:
            second_entered.set()
        return await real_accept(**kwargs)

    real_clear_storage = runtime.storage.clear_mail_data
    clear_storage_entered = threading.Event()
    release_clear_storage = threading.Event()

    def controlled_clear_storage() -> int:
        clear_storage_entered.set()
        if not release_clear_storage.wait(timeout=5):
            raise TimeoutError("test did not release clear storage")
        return real_clear_storage()

    monkeypatch.setattr(runtime, "_accept_message_operation", controlled_accept)
    monkeypatch.setattr(runtime.storage, "clear_mail_data", controlled_clear_storage)

    first = asyncio.create_task(
        runtime.accept_message(
            rcpt_tos=["first@maintenance-gate.example"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
    )
    await first_entered.wait()

    clear = asyncio.create_task(runtime.clear_all_mail())
    for _ in range(100):
        if runtime._mail_maintenance_active:
            break
        await asyncio.sleep(0.001)
    assert runtime._mail_maintenance_active is True

    second = asyncio.create_task(
        runtime.accept_message(
            rcpt_tos=["second@maintenance-gate.example"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
    )
    await asyncio.sleep(0)
    assert second_entered.is_set() is False

    release_first.set()
    first_response = await asyncio.wait_for(first, timeout=5)
    await asyncio.wait_for(asyncio.to_thread(clear_storage_entered.wait), timeout=5)
    assert second_entered.is_set() is False
    assert second.done() is False

    release_clear_storage.set()
    clear_result = await asyncio.wait_for(clear, timeout=5)
    second_response = await asyncio.wait_for(second, timeout=5)
    await runtime.drain_parser_queue()

    first_message_id = first_response.removeprefix("250 queued as ")
    second_message_id = second_response.removeprefix("250 queued as ")
    assert clear_result["messages"] == 1
    with connect_database(runtime.settings.database_path) as connection:
        message_ids = {
            str(row["id"])
            for row in connection.execute("SELECT id FROM messages").fetchall()
        }
    assert first_message_id not in message_ids
    assert message_ids == {second_message_id}

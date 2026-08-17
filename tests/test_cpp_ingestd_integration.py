from __future__ import annotations

import asyncio
import os
import smtplib
import socket
import subprocess
from email.message import EmailMessage
from pathlib import Path

import pytest

from app.config import Settings
from app.db.connection import initialize_database
from app.http.live import smtp_live_snapshot
from app.runtime import RapidInboxRuntime
from app.services.attachments import AttachmentService


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _attachment_mail_bytes() -> EmailMessage:
    msg = EmailMessage()
    msg["Subject"] = "Attachment demo"
    msg["From"] = "sender@example.com"
    msg["To"] = "code@adb.com"
    msg.set_content("Please see the attached report.")
    msg.add_attachment(
        b"Quarterly report\n",
        maintype="text",
        subtype="plain",
        filename="report.txt",
    )
    return msg


@pytest.mark.asyncio
async def test_cpp_ingestd_accepts_mail_and_python_reads_it(tmp_path: Path) -> None:
    build_dir = Path("cpp/ingestd/build")
    binary = build_dir / "rapid-inbox-ingestd"
    if not binary.exists():
        pytest.skip("rapid-inbox-ingestd has not been built")

    settings = Settings(storage_root=tmp_path / "storage", database_path=tmp_path / "storage" / "app.db")
    settings.ensure_directories()
    initialize_database(settings.database_path)
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    process: subprocess.Popen[str] | None = None
    try:
        await runtime.create_domain("adb.com")
        _, live_cursor = runtime.live_state.snapshot_state()
        live_seq = int(live_cursor.rsplit(":", 1)[1])

        port = _free_port()
        env = {
            **os.environ,
            "SMTP_HOST": "127.0.0.1",
            "SMTP_PORT": str(port),
            "STORAGE_ROOT": str(settings.storage_root),
            "DATABASE_PATH": str(settings.database_path),
            "INGEST_FLUSH_INTERVAL_MS": "50",
            "INGEST_BATCH_MAX_MESSAGES": "10",
        }
        process = subprocess.Popen(
            [str(binary), "--base-dir", str(tmp_path)],
            cwd=Path.cwd(),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(50):
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                    break
            except OSError:
                await asyncio.sleep(0.05)
        else:
            stdout, stderr = process.communicate(timeout=1)
            raise AssertionError(f"ingestd did not listen\nstdout={stdout}\nstderr={stderr}")

        msg = EmailMessage()
        msg["Subject"] = "Hello"
        msg["From"] = "sender@example.com"
        msg["To"] = "code@adb.com"
        msg.set_content("Your code is 123456")
        with smtplib.SMTP("127.0.0.1", port, timeout=5) as smtp:
            smtp.send_message(msg)

        deadline = asyncio.get_running_loop().time() + 5
        while True:
            mailbox = await runtime.get_mailbox_view("code@adb.com")
            live_events = [
                event
                for event in runtime.live_state.snapshot_since(live_seq)
                if event.get("type") == "mailbox_delivery"
                and event.get("mailbox") == "code@adb.com"
            ]
            if mailbox["message_count"] == 1 and live_events:
                assert mailbox["items"][0]["parse_status"] == "parsed"
                assert mailbox["items"][0]["verification_code"] == "123456"
                detail = await runtime.get_delivery_detail(
                    "code@adb.com",
                    mailbox["items"][0]["delivery_id"],
                )
                assert detail["text_body"].strip() == "Your code is 123456"
                assert detail["verification_code"] == "123456"
                assert live_events[-1]["delivery_id"] == mailbox["items"][0]["delivery_id"]
                assert live_events[-1]["parse_status"] == "parsed"
                admin_events = smtp_live_snapshot(runtime, events=live_events)
                assert admin_events == [
                    {
                        "type": "delivery_committed",
                        "session_id": live_events[-1]["session_id"],
                        "delivery_id": mailbox["items"][0]["delivery_id"],
                        "message_id": mailbox["items"][0]["message_id"],
                        "rcpt_to": "code@adb.com",
                        "mail_from": "sender@example.com",
                        "parse_status": "parsed",
                        "ts": live_events[-1]["ts"],
                        "source": "committed_outbox",
                    }
                ]
                assert str(admin_events[0]["session_id"]).startswith("smtp_")
                break
            if asyncio.get_running_loop().time() >= deadline:
                raise AssertionError(
                    "C++ delivery was not visible through the HTTP runtime live outbox"
                )
            await asyncio.sleep(0.1)
    finally:
        if process is not None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        await runtime.stop()


@pytest.mark.asyncio
async def test_cpp_ingestd_sigterm_drains_returned_250_mail(tmp_path: Path) -> None:
    build_dir = Path("cpp/ingestd/build")
    binary = build_dir / "rapid-inbox-ingestd"
    if not binary.exists():
        pytest.skip("rapid-inbox-ingestd has not been built")

    settings = Settings(storage_root=tmp_path / "storage", database_path=tmp_path / "storage" / "app.db")
    settings.ensure_directories()
    initialize_database(settings.database_path)
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        await runtime.create_domain("adb.com")
    finally:
        await runtime.stop()

    port = _free_port()
    env = {
        **os.environ,
        "SMTP_HOST": "127.0.0.1",
        "SMTP_PORT": str(port),
        "STORAGE_ROOT": str(settings.storage_root),
        "DATABASE_PATH": str(settings.database_path),
        "INGEST_FLUSH_INTERVAL_MS": "1000",
    }
    process = subprocess.Popen(
        [str(binary), "--base-dir", str(tmp_path)],
        cwd=Path.cwd(),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        for _ in range(50):
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                    break
            except OSError:
                await asyncio.sleep(0.05)
        else:
            stdout, stderr = process.communicate(timeout=1)
            raise AssertionError(f"ingestd did not listen\nstdout={stdout}\nstderr={stderr}")

        msg = EmailMessage()
        msg["Subject"] = "Hello"
        msg["From"] = "sender@example.com"
        msg["To"] = "code@adb.com"
        msg.set_content("Your code is 123456")
        with smtplib.SMTP("127.0.0.1", port, timeout=5) as smtp:
            smtp.send_message(msg)

        process.terminate()
        process.wait(timeout=5)

        runtime = RapidInboxRuntime(settings)
        await runtime.start()
        try:
            mailbox = await runtime.get_mailbox_view("code@adb.com")
            assert mailbox["message_count"] == 1
        finally:
            await runtime.stop()
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)


@pytest.mark.asyncio
async def test_cpp_ingestd_attachment_visible_to_python_runtime(tmp_path: Path) -> None:
    build_dir = Path("cpp/ingestd/build")
    binary = build_dir / "rapid-inbox-ingestd"
    if not binary.exists():
        pytest.skip("rapid-inbox-ingestd has not been built")

    settings = Settings(storage_root=tmp_path / "storage", database_path=tmp_path / "storage" / "app.db")
    settings.ensure_directories()
    initialize_database(settings.database_path)
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        await runtime.create_domain("adb.com", public_web_enabled=True)
    finally:
        await runtime.stop()

    port = _free_port()
    env = {
        **os.environ,
        "SMTP_HOST": "127.0.0.1",
        "SMTP_PORT": str(port),
        "STORAGE_ROOT": str(settings.storage_root),
        "DATABASE_PATH": str(settings.database_path),
        "INGEST_FLUSH_INTERVAL_MS": "50",
        "INGEST_BATCH_MAX_MESSAGES": "10",
    }
    process = subprocess.Popen(
        [str(binary), "--base-dir", str(tmp_path)],
        cwd=Path.cwd(),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        for _ in range(50):
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                    break
            except OSError:
                await asyncio.sleep(0.05)
        else:
            stdout, stderr = process.communicate(timeout=1)
            raise AssertionError(f"ingestd did not listen\nstdout={stdout}\nstderr={stderr}")

        with smtplib.SMTP("127.0.0.1", port, timeout=5) as smtp:
            smtp.send_message(_attachment_mail_bytes())

        deadline = asyncio.get_running_loop().time() + 5
        while True:
            runtime = RapidInboxRuntime(settings)
            await runtime.start()
            try:
                mailbox = await runtime.get_mailbox_view("code@adb.com")
                if mailbox["message_count"] == 1:
                    detail = await runtime.get_delivery_detail(
                        "code@adb.com",
                        mailbox["items"][0]["delivery_id"],
                    )
                    assert detail["attachments"], "attachment metadata missing"
                    attachment = detail["attachments"][0]
                    assert attachment["filename"] == "report.txt"
                    assert attachment["storage_path"]
                    attachment_service = AttachmentService(runtime)
                    fetched = await attachment_service.get_delivery_attachment(
                        "code@adb.com",
                        mailbox["items"][0]["delivery_id"],
                        attachment["id"],
                    )
                    assert fetched["content"] == b"Quarterly report\n"
                    break
            finally:
                await runtime.stop()
            if asyncio.get_running_loop().time() >= deadline:
                raise AssertionError("attachment message was not visible to Python runtime")
            await asyncio.sleep(0.1)
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)

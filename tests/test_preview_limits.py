from __future__ import annotations

import asyncio
import threading

import pytest

from app.config import Settings
from app.db.connection import connect_database
from app.main import create_app
from app.runtime import RapidInboxRuntime


def test_preview_configuration_is_strict_and_declared_in_openapi(tmp_path) -> None:
    with pytest.raises(ValueError, match="MESSAGE_PREVIEW_BODY_BYTES"):
        Settings(
            storage_root=tmp_path / "storage-a",
            database_path=tmp_path / "storage-a" / "app.db",
            message_preview_body_bytes=0,
        )
    with pytest.raises(ValueError, match="must not exceed"):
        Settings(
            storage_root=tmp_path / "storage-b",
            database_path=tmp_path / "storage-b" / "app.db",
            message_preview_inline_item_bytes=9,
            message_preview_inline_total_bytes=8,
        )

    schema = create_app().openapi()
    admin_properties = schema["components"]["schemas"]["MessageDetailOut"]["properties"]
    public_properties = schema["components"]["schemas"]["PublicMessageDetailOut"]["properties"]
    for properties in (admin_properties, public_properties):
        assert "text_body_source_bytes" in properties
        assert "text_body_preview_bytes" in properties
        assert "text_body_truncated" in properties
        assert "html_body_source_bytes" in properties
        assert "html_body_preview_bytes" in properties
        assert "html_body_truncated" in properties
        assert "headers_source_bytes" in properties
        assert "headers_truncated" in properties
    assert "inline_preview_embedded_count" in admin_properties
    assert "inline_preview_total_limit_bytes" in admin_properties


@pytest.mark.asyncio
async def test_public_and_admin_previews_are_bounded_without_affecting_downloads(
    tmp_path,
    sample_email_bytes: bytes,
    monkeypatch,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
        message_preview_body_bytes=256,
        message_preview_headers_bytes=16,
        message_preview_inline_item_bytes=8,
        message_preview_inline_total_bytes=8,
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        await runtime.create_domain(
            "preview.example",
            public_web_enabled=True,
            public_api_enabled=True,
        )
        response = await runtime.accept_message(
            rcpt_tos=["box@preview.example"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
        await runtime.drain_parser_queue()
        message_id = response.removeprefix("250 queued as ")
        mailbox = await runtime.get_mailbox_view("box@preview.example")
        delivery_id = str(mailbox["items"][0]["delivery_id"])

        with connect_database(settings.database_path) as connection:
            message = connection.execute(
                "SELECT text_body_path, html_body_path FROM messages WHERE id = ?",
                (message_id,),
            ).fetchone()
        assert message is not None
        runtime.storage.resolve(str(message["text_body_path"])).write_text(
            "T" * 600,
            encoding="utf-8",
        )
        runtime.storage.resolve(str(message["html_body_path"])).write_text(
            '<img src="cid:first"><img src="cid:second">' + ("H" * 600),
            encoding="utf-8",
        )

        attachment_rows = []
        for part_index, (attachment_id, content_id, content) in enumerate(
            (("att_preview_1", "first", b"12345678"), ("att_preview_2", "second", b"ABCDEFGH")),
            start=1,
        ):
            storage_path, safe_filename = runtime.storage.write_attachment(
                message_id,
                attachment_id,
                f"{content_id}.png",
                content,
            )
            attachment_rows.append(
                (
                    attachment_id,
                    message_id,
                    part_index,
                    f"{content_id}.png",
                    safe_filename,
                    "image/png",
                    "inline",
                    content_id,
                    storage_path,
                    "test-sha256",
                    len(content),
                    1,
                    "2026-01-01T00:00:00Z",
                )
            )

        await runtime.writer.execute(
            lambda connection: (
                connection.executemany(
                    """
                    INSERT INTO attachments (
                        id, message_id, part_index, filename, safe_filename,
                        content_type, content_disposition, content_id, storage_path,
                        sha256, size_bytes, is_inline, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    attachment_rows,
                ),
                connection.execute(
                    """
                    UPDATE messages
                    SET headers_json = ?, has_attachments = 1, attachment_count = 2
                    WHERE id = ?
                    """,
                    ('[["X-Large", "' + ("V" * 200) + '"]]', message_id),
                ),
            )
        )

        main_thread = threading.current_thread()
        read_threads: list[threading.Thread] = []
        real_read_text_preview = runtime.storage.read_text_preview

        def tracked_read_text_preview(*args, **kwargs):
            read_threads.append(threading.current_thread())
            return real_read_text_preview(*args, **kwargs)

        monkeypatch.setattr(runtime.storage, "read_text_preview", tracked_read_text_preview)
        monkeypatch.setattr(
            runtime.storage,
            "read_bytes",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("preview must not use unbounded read_bytes")
            ),
        )

        public_detail = await runtime.messages.get_public_delivery_detail(
            "box@preview.example",
            delivery_id,
            surface="web",
        )
        assert public_detail["text_body"] == "T" * 256
        assert public_detail["text_body_source_bytes"] == 600
        assert public_detail["text_body_preview_bytes"] == 256
        assert public_detail["text_body_truncated"] is True
        assert public_detail["html_body_source_bytes"] > 600
        assert public_detail["html_body_preview_bytes"] <= 256
        assert public_detail["html_body_truncated"] is True
        assert public_detail["headers"] == []
        assert public_detail["headers_source_bytes"] > 16
        assert public_detail["headers_truncated"] is True
        assert read_threads and all(thread is not main_thread for thread in read_threads)

        runtime.storage.resolve(str(message["text_body_path"])).write_text("", encoding="utf-8")
        admin_detail = await asyncio.to_thread(
            runtime.messages.get_admin_message_detail,
            message_id,
            include_html_preview=True,
        )
        assert admin_detail["text_body_preview_bytes"] == 0
        assert admin_detail["inline_preview_embedded_count"] == 1
        assert admin_detail["inline_preview_skipped_count"] == 1
        assert admin_detail["inline_preview_embedded_source_bytes"] == 8
        assert admin_detail["inline_preview_embedded_encoded_bytes"] > 8
        assert "data:image/png;base64," in admin_detail["html_preview_srcdoc"]
        assert "cid:second" in admin_detail["html_preview_srcdoc"]

        public_preview = await runtime.messages.get_public_html_preview(
            "box@preview.example",
            delivery_id,
            surface="web",
        )
        assert public_preview["html_body_truncated"] is True
        assert public_preview["inline_preview_embedded_count"] == 1
        assert public_preview["inline_preview_skipped_count"] == 1

        admin_file = runtime.messages.get_admin_attachment_file(message_id, "att_preview_2")
        public_file = await runtime.get_public_attachment_file(
            "box@preview.example",
            delivery_id,
            "att_preview_2",
            surface="web",
        )
        assert admin_file["path"].read_bytes() == b"ABCDEFGH"
        assert public_file["path"].read_bytes() == b"ABCDEFGH"
    finally:
        await runtime.stop()

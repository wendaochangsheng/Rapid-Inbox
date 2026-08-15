from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pytest

from app.auth.permissions import PermissionContext
from app.db.connection import connect_database, initialize_database
from app.http import api_v2


def _principal(
    *,
    mode: str = "all",
    domain_ids: tuple[int, ...] = (),
    mailbox_patterns: tuple[str, ...] = (),
) -> PermissionContext:
    return PermissionContext(
        scopes=("messages.read",),
        domain_ids=domain_ids,
        mailbox_patterns=mailbox_patterns,
        domain_grant_mode=mode,
        kind="service",
    )


def _insert_domain(connection: sqlite3.Connection, domain_id: int, domain: str) -> None:
    connection.execute(
        """
        INSERT INTO domains (id, root_domain_ascii, created_at, updated_at)
        VALUES (?, ?, '2026-07-15T00:00:00Z', '2026-07-15T00:00:00Z')
        """,
        (domain_id, domain),
    )


def _insert_mailbox(
    connection: sqlite3.Connection,
    mailbox_id: int,
    domain_id: int,
    address: str,
) -> None:
    local_part, domain = address.split("@", 1)
    connection.execute(
        """
        INSERT INTO mailboxes (
            id, domain_id, local_part_canonical, rcpt_domain_ascii,
            address_canonical, address_display, first_seen_at, last_seen_at
        ) VALUES (?, ?, ?, ?, ?, ?, '2026-07-15T00:00:00Z', '2026-07-15T00:00:00Z')
        """,
        (mailbox_id, domain_id, local_part, domain, address, address),
    )


def _insert_message(
    connection: sqlite3.Connection,
    message_id: str,
    received_at: str,
    *,
    subject: str,
    from_addr: str,
    envelope_from: str,
    parse_status: str = "parsed",
    verification_code: str | None = None,
) -> None:
    connection.execute(
        """
        INSERT INTO messages (
            id, raw_path, raw_sha256, raw_size_bytes, envelope_from,
            subject, from_addr, received_at, parse_status, verification_code
        ) VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?)
        """,
        (
            message_id,
            f"raw/{message_id}.eml",
            f"sha-{message_id}",
            envelope_from,
            subject,
            from_addr,
            received_at,
            parse_status,
            verification_code,
        ),
    )


def _insert_delivery(
    connection: sqlite3.Connection,
    delivery_id: str,
    message_id: str,
    mailbox_id: int,
    rcpt_to: str,
) -> None:
    connection.execute(
        """
        INSERT INTO message_deliveries (
            id, message_id, mailbox_id, rcpt_to, delivered_at
        ) VALUES (?, ?, ?, ?, '2026-07-15T00:00:00Z')
        """,
        (delivery_id, message_id, mailbox_id, rcpt_to),
    )


def _seed_authorization_matrix(database_path: Path) -> None:
    initialize_database(database_path)
    with connect_database(database_path) as connection:
        _insert_domain(connection, 1, "a.example")
        _insert_domain(connection, 2, "b.example")
        _insert_mailbox(connection, 1, 1, "team1@a.example")
        _insert_mailbox(connection, 2, 1, "team2@a.example")
        _insert_mailbox(connection, 3, 1, "other@a.example")
        _insert_mailbox(connection, 4, 2, "team1@b.example")

        messages = (
            ("m-no-delivery", "2026-07-15T00:10:00Z", "No delivery", "none@example.net", "none"),
            ("m-cross", "2026-07-15T00:09:00Z", "Cross domain", "cross@example.net", "cross"),
            ("m-b", "2026-07-15T00:08:00Z", "Only B", "b@example.net", "b"),
            ("m-multi-a", "2026-07-15T00:07:00Z", "Multi A", "multi@example.net", "multi"),
            ("m-other-a", "2026-07-15T00:06:00Z", "Other A", "other@example.net", "other"),
            ("m-team2", "2026-07-15T00:05:00Z", "Team two", "two@example.net", "two"),
            ("m-team1", "2026-07-15T00:05:00Z", "Team one", "one@example.net", "one"),
        )
        for index, (message_id, received_at, subject, from_addr, envelope_from) in enumerate(messages):
            _insert_message(
                connection,
                message_id,
                received_at,
                subject=subject,
                from_addr=from_addr,
                envelope_from=envelope_from,
                parse_status="failed" if message_id == "m-other-a" else "parsed",
                verification_code=f"900{index}",
            )

        _insert_delivery(connection, "d-cross-a", "m-cross", 1, "team1@a.example")
        _insert_delivery(connection, "d-cross-b", "m-cross", 4, "team1@b.example")
        _insert_delivery(connection, "d-b", "m-b", 4, "team1@b.example")
        _insert_delivery(connection, "d-multi-1", "m-multi-a", 1, "team1@a.example")
        _insert_delivery(connection, "d-multi-2", "m-multi-a", 2, "team2@a.example")
        _insert_delivery(connection, "d-other", "m-other-a", 3, "other@a.example")
        _insert_delivery(connection, "d-team2", "m-team2", 2, "team2@a.example")
        _insert_delivery(connection, "d-team1", "m-team1", 1, "team1@a.example")


def _legacy_message_list_query(
    principal: PermissionContext,
    *,
    normalized_query: str | None,
    parse_status: str | None,
    mailbox_id: int | None,
    position: tuple[str, str] | None,
    limit: int,
) -> tuple[str, tuple[Any, ...]]:
    clauses, params = api_v2._message_access_sql(principal)
    if normalized_query:
        search = f"%{normalized_query}%"
        clauses.append(
            """
            (
                m.subject LIKE ? OR m.from_addr LIKE ? OR m.envelope_from LIKE ?
                OR EXISTS (
                    SELECT 1 FROM message_deliveries AS query_delivery
                    WHERE query_delivery.message_id = m.id AND query_delivery.rcpt_to LIKE ?
                )
            )
            """
        )
        params.extend([search, search, search, search])
    if parse_status is not None:
        clauses.append("m.parse_status = ?")
        params.append(parse_status)
    if mailbox_id is not None:
        clauses.append(
            "EXISTS (SELECT 1 FROM message_deliveries AS mailbox_delivery "
            "WHERE mailbox_delivery.message_id = m.id AND mailbox_delivery.mailbox_id = ?)"
        )
        params.append(mailbox_id)
    if position is not None:
        received_at, message_id = position
        clauses.append("(m.received_at < ? OR (m.received_at = ? AND m.id < ?))")
        params.extend([received_at, received_at, message_id])
    where_sql = "WHERE " + " AND ".join(f"({clause})" for clause in clauses)
    return (
        f"""
        SELECT
            m.id, m.subject, m.from_addr,
            COALESCE((
                SELECT GROUP_CONCAT(recipient.rcpt_to, ', ')
                FROM (
                    SELECT DISTINCT rcpt_to
                    FROM message_deliveries
                    WHERE message_id = m.id
                    ORDER BY rcpt_to ASC
                ) AS recipient
            ), '') AS recipients,
            m.received_at, m.parse_status, m.parse_error,
            m.has_attachments, m.attachment_count,
            (SELECT COUNT(*) FROM message_deliveries AS count_delivery
             WHERE count_delivery.message_id = m.id) AS delivery_count
        FROM messages AS m
        {where_sql}
        ORDER BY m.received_at DESC, m.id DESC
        LIMIT ?
        """,
        (*params, limit + 1),
    )


@pytest.mark.parametrize(
    ("principal", "normalized_query", "parse_status", "mailbox_id", "position"),
    [
        (_principal(), None, None, None, None),
        (_principal(), None, None, 1, None),
        (_principal(mode="selected", domain_ids=(1,)), None, None, None, None),
        (
            _principal(
                mode="selected",
                domain_ids=(1,),
                mailbox_patterns=("team?@a.example",),
            ),
            None,
            None,
            None,
            None,
        ),
        (_principal(mailbox_patterns=("team*@a.example",)), None, None, None, None),
        (_principal(mode="none"), None, None, None, None),
        (_principal(mode="selected", domain_ids=(1,)), "Team two", None, None, None),
        (_principal(mode="selected", domain_ids=(1,)), "one@example.net", None, None, None),
        (_principal(mode="selected", domain_ids=(1,)), "multi", None, None, None),
        (_principal(mode="selected", domain_ids=(1,)), "team2@a.example", None, None, None),
        (_principal(mode="selected", domain_ids=(1,)), None, "failed", None, None),
        (
            _principal(mode="selected", domain_ids=(1,)),
            None,
            None,
            None,
            ("2026-07-15T00:05:00Z", "m-team2"),
        ),
        (_principal(mode="selected", domain_ids=(1,)), None, None, 1, None),
    ],
)
def test_restricted_message_query_matches_legacy_results(
    tmp_path: Path,
    principal: PermissionContext,
    normalized_query: str | None,
    parse_status: str | None,
    mailbox_id: int | None,
    position: tuple[str, str] | None,
) -> None:
    database_path = tmp_path / "app.db"
    _seed_authorization_matrix(database_path)
    arguments = {
        "normalized_query": normalized_query,
        "parse_status": parse_status,
        "mailbox_id": mailbox_id,
        "position": position,
        "limit": 100,
    }
    optimized_sql, optimized_params = api_v2._message_list_query(principal, **arguments)
    legacy_sql, legacy_params = _legacy_message_list_query(principal, **arguments)

    with connect_database(database_path) as connection:
        optimized = [dict(row) for row in connection.execute(optimized_sql, optimized_params)]
        legacy = [dict(row) for row in connection.execute(legacy_sql, legacy_params)]

    assert optimized == legacy


def test_restricted_message_query_deduplicates_and_fails_closed_across_domains(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "app.db"
    _seed_authorization_matrix(database_path)
    selected = _principal(
        mode="selected",
        domain_ids=(1,),
        mailbox_patterns=("team?@a.example",),
    )
    selected_sql, selected_params = api_v2._message_list_query(
        selected,
        normalized_query=None,
        parse_status=None,
        mailbox_id=None,
        position=None,
        limit=100,
    )
    mailbox_sql, mailbox_params = api_v2._message_list_query(
        _principal(),
        normalized_query=None,
        parse_status=None,
        mailbox_id=1,
        position=None,
        limit=100,
    )
    cursor_sql, cursor_params = api_v2._message_list_query(
        selected,
        normalized_query=None,
        parse_status=None,
        mailbox_id=None,
        position=("2026-07-15T00:05:00Z", "m-team2"),
        limit=100,
    )
    with connect_database(database_path) as connection:
        selected_rows = [dict(row) for row in connection.execute(selected_sql, selected_params)]
        mailbox_ids = [str(row["id"]) for row in connection.execute(mailbox_sql, mailbox_params)]
        after_cursor_ids = [
            str(row["id"])
            for row in connection.execute(cursor_sql, cursor_params)
        ]

    selected_ids = [str(row["id"]) for row in selected_rows]
    assert selected_ids == ["m-multi-a", "m-team2", "m-team1"]
    assert selected_ids.count("m-multi-a") == 1
    multi = next(row for row in selected_rows if row["id"] == "m-multi-a")
    assert multi["recipients"] == "team1@a.example, team2@a.example"
    assert multi["delivery_count"] == 2
    assert "m-cross" not in selected_ids
    assert "m-b" not in selected_ids
    assert "m-cross" in mailbox_ids
    assert after_cursor_ids == ["m-team1"]


def _query_vm_steps(
    connection: sqlite3.Connection,
    query: str,
    params: tuple[Any, ...],
) -> tuple[int, list[sqlite3.Row]]:
    # SQLite 3.41+ may invoke the progress handler while preparing a statement.
    # Warm the read-only query so comparisons measure execution work only.
    connection.execute(query, params).fetchall()
    steps = 0

    def count_step() -> int:
        nonlocal steps
        steps += 1
        return 0

    connection.set_progress_handler(count_step, 1)
    try:
        rows = connection.execute(query, params).fetchall()
    finally:
        connection.set_progress_handler(None, 0)
    return steps, rows


def test_sparse_authorization_plans_do_not_scan_global_messages(tmp_path: Path) -> None:
    database_path = tmp_path / "app.db"
    initialize_database(database_path)
    all_principal = _principal()
    selected_principal = _principal(mode="selected", domain_ids=(1,))
    glob_principal = _principal(mailbox_patterns=("empty*@sparse.example",))
    scenarios = (
        ("mailbox", all_principal, 1, "idx_message_deliveries_mailbox_time"),
        ("selected", selected_principal, None, "idx_mailboxes_domain"),
        ("glob", glob_principal, None, "sqlite_autoindex_mailboxes_1"),
    )

    with connect_database(database_path) as connection:
        _insert_domain(connection, 1, "sparse.example")
        _insert_mailbox(connection, 1, 1, "empty@sparse.example")

        def add_messages(start: int, count: int) -> None:
            connection.executemany(
                """
                INSERT INTO messages (
                    id, raw_path, raw_sha256, raw_size_bytes, received_at, parse_status
                ) VALUES (?, ?, ?, 1, ?, 'parsed')
                """,
                (
                    (
                        f"global-{index:06d}",
                        f"raw/global-{index:06d}.eml",
                        f"sha-global-{index:06d}",
                        f"2026-07-15T00:{index % 60:02d}:00Z",
                    )
                    for index in range(start, start + count)
                ),
            )

        add_messages(0, 100)
        legacy_sparse_query = _legacy_message_list_query(
            all_principal,
            normalized_query="absent-recipient-or-header",
            parse_status="parsed",
            mailbox_id=1,
            position=("9999-12-31T23:59:59Z", "cursor-max"),
            limit=100,
        )
        legacy_before_steps, legacy_rows = _query_vm_steps(
            connection,
            *legacy_sparse_query,
        )
        assert legacy_rows == []
        before_steps: dict[str, int] = {}
        queries: dict[str, tuple[str, tuple[Any, ...]]] = {}
        for name, principal, mailbox_id, _expected_index in scenarios:
            query = api_v2._message_list_query(
                principal,
                normalized_query="absent-recipient-or-header",
                parse_status="parsed",
                mailbox_id=mailbox_id,
                position=("9999-12-31T23:59:59Z", "cursor-max"),
                limit=100,
            )
            queries[name] = query
            before_steps[name], rows = _query_vm_steps(connection, *query)
            assert rows == []

        add_messages(100, 20_000)
        legacy_after_steps, legacy_rows = _query_vm_steps(
            connection,
            *legacy_sparse_query,
        )
        assert legacy_rows == []
        assert legacy_after_steps > legacy_before_steps * 100
        for name, _principal_value, _mailbox_id, expected_index in scenarios:
            query, params = queries[name]
            after_steps, rows = _query_vm_steps(connection, query, params)
            plan = [
                str(row["detail"])
                for row in connection.execute(f"EXPLAIN QUERY PLAN {query}", params)
            ]
            assert rows == []
            assert after_steps == before_steps[name]
            assert after_steps < 200
            assert any(expected_index in detail for detail in plan)
            assert any(
                "SEARCH m USING INDEX sqlite_autoindex_messages_1" in detail
                for detail in plan
            )
            assert not any(detail.startswith(("SCAN m", "SCAN messages")) for detail in plan)

        global_query, global_params = api_v2._message_list_query(
            all_principal,
            normalized_query=None,
            parse_status=None,
            mailbox_id=None,
            position=None,
            limit=100,
        )
        global_plan = [
            str(row["detail"])
            for row in connection.execute(
                f"EXPLAIN QUERY PLAN {global_query}",
                global_params,
            )
        ]
        assert any("SCAN m USING INDEX idx_messages_received_id" in detail for detail in global_plan)

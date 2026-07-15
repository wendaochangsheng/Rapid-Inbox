from __future__ import annotations

from datetime import datetime

import pytest

from app.http.template_helpers import SHANGHAI_TZ, cn_datetime, cn_time, time_greeting


def test_cn_datetime_converts_utc_to_shanghai_time() -> None:
    assert cn_datetime("2026-04-18T20:00:00Z") == "2026-04-19 04:00:00"


def test_cn_time_converts_utc_to_shanghai_time() -> None:
    assert cn_time("2026-04-18T20:00:00Z") == "04:00:00"


@pytest.mark.parametrize(
    ("hour", "expected"),
    [
        (0, "夜深了"),
        (5, "早上好"),
        (11, "早上好"),
        (12, "下午好"),
        (17, "下午好"),
        (18, "晚上好"),
        (22, "晚上好"),
        (23, "夜深了"),
    ],
)
def test_time_greeting_uses_shanghai_day_parts(hour: int, expected: str) -> None:
    now = datetime(2026, 4, 19, hour, tzinfo=SHANGHAI_TZ)

    assert time_greeting(now) == expected

from __future__ import annotations

import asyncio
from typing import Any

import dns.resolver


class DnsCheckService:
    async def run_dns_check(self, root_domain: str) -> dict[str, Any]:
        return await asyncio.to_thread(self._run_dns_check_sync, root_domain)

    def _run_dns_check_sync(self, root_domain: str) -> dict[str, Any]:
        try:
            answers = dns.resolver.resolve(root_domain, "MX")
            records = sorted(str(answer.exchange).rstrip(".") for answer in answers)
            return {"status": "ok", "mx_records": records}
        except Exception as exc:  # noqa: BLE001
            return {"status": "warning", "error": str(exc), "mx_records": []}

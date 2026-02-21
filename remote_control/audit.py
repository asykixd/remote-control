from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass
class AuditRecord:
    ts: str
    user_id: int
    username: str
    action: str
    status: str
    details: str = ""


class AuditStore:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, user_id: int, username: str, action: str, status: str, details: str = "") -> None:
        record = AuditRecord(
            ts=datetime.now(timezone.utc).isoformat(),
            user_id=user_id,
            username=username,
            action=action,
            status=status,
            details=details,
        )
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record.__dict__, ensure_ascii=False) + "\n")

    def tail(self, limit: int = 15) -> list[AuditRecord]:
        if not self.path.exists():
            return []

        lines = self.path.read_text(encoding="utf-8").splitlines()
        rows = lines[-max(limit, 1) :]
        result: list[AuditRecord] = []
        for line in rows:
            try:
                raw = json.loads(line)
            except Exception:
                continue
            try:
                result.append(
                    AuditRecord(
                        ts=str(raw.get("ts", "")),
                        user_id=int(raw.get("user_id", 0)),
                        username=str(raw.get("username", "")),
                        action=str(raw.get("action", "")),
                        status=str(raw.get("status", "")),
                        details=str(raw.get("details", "")),
                    )
                )
            except Exception:
                continue
        return result


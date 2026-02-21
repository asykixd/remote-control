from __future__ import annotations

import hashlib
import hmac
import json
import secrets
from dataclasses import asdict, dataclass, field
from pathlib import Path


CONFIG_PATH = Path("config.json")


def _normalize_username(value: str) -> str:
    return value.strip().lstrip("@").lower()


@dataclass
class AppConfig:
    bot_token: str = ""
    allowed_usernames: list[str] = field(default_factory=list)
    allowed_user_ids: list[int] = field(default_factory=list)
    owner_user_id: int | None = None
    pin_salt: str = ""
    pin_hash: str = ""
    premium_emoji_id: str = ""
    message_effect_id: str = ""
    audit_log_path: str = "audit.log"
    autostart_enabled: bool = False

    @property
    def normalized_usernames(self) -> set[str]:
        return {name for name in (_normalize_username(v) for v in self.allowed_usernames) if name}

    @property
    def normalized_user_ids(self) -> set[int]:
        values: set[int] = set()
        for raw in self.allowed_user_ids:
            try:
                value = int(raw)
            except Exception:
                continue
            if value > 0:
                values.add(value)
        return values

    @property
    def has_pin(self) -> bool:
        return bool(self.pin_salt and self.pin_hash)

    def verify_pin(self, pin: str) -> bool:
        if not self.has_pin:
            return False
        expected = _hash_pin(self.pin_salt, pin)
        return hmac.compare_digest(expected, self.pin_hash)

    def set_pin(self, pin: str) -> None:
        self.pin_salt = secrets.token_hex(16)
        self.pin_hash = _hash_pin(self.pin_salt, pin)


def _hash_pin(salt: str, pin: str) -> str:
    payload = f"{salt}:{pin}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def load_config() -> AppConfig:
    if not CONFIG_PATH.exists():
        return AppConfig()

    try:
        raw = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return AppConfig()
    token = str(raw.get("bot_token", "")).strip()
    usernames = raw.get("allowed_usernames", [])
    user_ids = raw.get("allowed_user_ids", [])
    if not isinstance(usernames, list):
        usernames = []
    if not isinstance(user_ids, list):
        user_ids = []

    owner_user_id: int | None = None
    if raw.get("owner_user_id") is not None:
        try:
            parsed_owner = int(raw["owner_user_id"])
            if parsed_owner > 0:
                owner_user_id = parsed_owner
        except Exception:
            owner_user_id = None

    pin_salt = str(raw.get("pin_salt", "")).strip()
    pin_hash = str(raw.get("pin_hash", "")).strip()
    premium_emoji_id = str(raw.get("premium_emoji_id", "")).strip()
    message_effect_id = str(raw.get("message_effect_id", "")).strip()
    audit_log_path = str(raw.get("audit_log_path", "audit.log")).strip() or "audit.log"
    autostart_enabled = bool(raw.get("autostart_enabled", False))

    return AppConfig(
        bot_token=token,
        allowed_usernames=[str(x).strip() for x in usernames if str(x).strip()],
        allowed_user_ids=[int(x) for x in user_ids if str(x).strip().isdigit()],
        owner_user_id=owner_user_id,
        pin_salt=pin_salt,
        pin_hash=pin_hash,
        premium_emoji_id=premium_emoji_id,
        message_effect_id=message_effect_id,
        audit_log_path=audit_log_path,
        autostart_enabled=autostart_enabled,
    )


def save_config(config: AppConfig) -> None:
    config.allowed_usernames = sorted(config.normalized_usernames)
    config.allowed_user_ids = sorted(config.normalized_user_ids)
    if config.owner_user_id is not None and config.owner_user_id <= 0:
        config.owner_user_id = None
    config.premium_emoji_id = config.premium_emoji_id.strip()
    config.message_effect_id = config.message_effect_id.strip()
    config.audit_log_path = config.audit_log_path.strip() or "audit.log"
    config.autostart_enabled = bool(config.autostart_enabled)

    payload = asdict(config)
    CONFIG_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

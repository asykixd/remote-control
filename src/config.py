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
    monitor_enabled: bool = True
    temperature_alert_c: float = 85.0
    disk_free_alert_gb: float = 5.0
    internet_check_host: str = "1.1.1.1"
    internet_check_port: int = 53
    alert_cooldown_sec: int = 900
    scheduled_tasks: list[dict[str, str]] = field(default_factory=list)
    sleep_mode_commands: list[str] = field(default_factory=list)
    work_mode_commands: list[str] = field(default_factory=list)
    custom_scripts: list[dict[str, object]] = field(default_factory=list)

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
    monitor_enabled = bool(raw.get("monitor_enabled", True))
    temperature_alert_c = _safe_float(raw.get("temperature_alert_c"), default=85.0)
    disk_free_alert_gb = _safe_float(raw.get("disk_free_alert_gb"), default=5.0)
    internet_check_host = str(raw.get("internet_check_host", "1.1.1.1")).strip() or "1.1.1.1"
    internet_check_port = _safe_int(raw.get("internet_check_port"), default=53, min_value=1, max_value=65535)
    alert_cooldown_sec = _safe_int(raw.get("alert_cooldown_sec"), default=900, min_value=60, max_value=86400)
    scheduled_tasks = raw.get("scheduled_tasks", [])
    if not isinstance(scheduled_tasks, list):
        scheduled_tasks = []
    normalized_tasks: list[dict[str, str]] = []
    for item in scheduled_tasks:
        if not isinstance(item, dict):
            continue
        task_id = str(item.get("id", "")).strip()
        when_iso = str(item.get("when_iso", "")).strip()
        command = str(item.get("command", "")).strip()
        created_by = str(item.get("created_by", "")).strip()
        reason = str(item.get("reason", "")).strip()
        if not task_id or not when_iso or not command:
            continue
        normalized_tasks.append(
            {
                "id": task_id,
                "when_iso": when_iso,
                "command": command,
                "created_by": created_by,
                "reason": reason,
            }
        )

    sleep_mode_commands = _normalize_commands(raw.get("sleep_mode_commands", []))
    work_mode_commands = _normalize_commands(raw.get("work_mode_commands", []))
    custom_scripts = _normalize_custom_scripts(raw.get("custom_scripts", []))

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
        monitor_enabled=monitor_enabled,
        temperature_alert_c=temperature_alert_c,
        disk_free_alert_gb=disk_free_alert_gb,
        internet_check_host=internet_check_host,
        internet_check_port=internet_check_port,
        alert_cooldown_sec=alert_cooldown_sec,
        scheduled_tasks=normalized_tasks,
        sleep_mode_commands=sleep_mode_commands,
        work_mode_commands=work_mode_commands,
        custom_scripts=custom_scripts,
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
    config.monitor_enabled = bool(config.monitor_enabled)
    config.temperature_alert_c = min(max(float(config.temperature_alert_c), 40.0), 120.0)
    config.disk_free_alert_gb = min(max(float(config.disk_free_alert_gb), 0.5), 500.0)
    config.internet_check_host = config.internet_check_host.strip() or "1.1.1.1"
    config.internet_check_port = min(max(int(config.internet_check_port), 1), 65535)
    config.alert_cooldown_sec = min(max(int(config.alert_cooldown_sec), 60), 86400)
    config.scheduled_tasks = _normalize_tasks(config.scheduled_tasks)
    config.sleep_mode_commands = _normalize_commands(config.sleep_mode_commands)
    config.work_mode_commands = _normalize_commands(config.work_mode_commands)
    config.custom_scripts = _normalize_custom_scripts(config.custom_scripts)

    payload = asdict(config)
    CONFIG_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _safe_float(value: object, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _safe_int(value: object, default: int, min_value: int, max_value: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        return default
    return min(max(parsed, min_value), max_value)


def _normalize_tasks(raw_tasks: list[dict[str, str]]) -> list[dict[str, str]]:
    tasks: list[dict[str, str]] = []
    for item in raw_tasks:
        if not isinstance(item, dict):
            continue
        task_id = str(item.get("id", "")).strip()
        when_iso = str(item.get("when_iso", "")).strip()
        command = str(item.get("command", "")).strip()
        created_by = str(item.get("created_by", "")).strip()
        reason = str(item.get("reason", "")).strip()
        if not task_id or not when_iso or not command:
            continue
        tasks.append(
            {
                "id": task_id,
                "when_iso": when_iso,
                "command": command,
                "created_by": created_by,
                "reason": reason,
            }
        )
    return tasks


def _normalize_commands(value: object) -> list[str]:
    if isinstance(value, str):
        source = value.splitlines()
    elif isinstance(value, list):
        source = value
    else:
        source = []

    commands: list[str] = []
    for item in source:
        line = str(item).strip()
        if line:
            commands.append(line)
    return commands


def _normalize_custom_scripts(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []

    scripts: list[dict[str, object]] = []
    for item in value:
        if not isinstance(item, dict):
            continue

        script_id = str(item.get("id", "")).strip()
        name = str(item.get("name", "")).strip()
        description = str(item.get("description", "")).strip()
        commands = _normalize_commands(item.get("commands", []))

        if not name or not commands:
            continue
        if not script_id:
            script_id = secrets.token_hex(4)

        scripts.append(
            {
                "id": script_id[:24],
                "name": name[:80],
                "description": description[:240],
                "commands": commands,
            }
        )
    return scripts

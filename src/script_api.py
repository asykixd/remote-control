from __future__ import annotations

import json
import re
from pathlib import Path

SCRIPT_ID_MAX = 24
BUTTON_ID_MAX = 24

_ID_CLEAN_RE = re.compile(r"[^a-z0-9_-]+")


def ensure_scripts_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def load_scripts_from_directory(path: Path) -> tuple[list[dict[str, object]], list[str]]:
    scripts: list[dict[str, object]] = []
    warnings: list[str] = []
    used_ids: set[str] = set()

    if not path.exists():
        return scripts, [f"scripts directory not found: {path}"]
    if not path.is_dir():
        return scripts, [f"scripts path is not a directory: {path}"]

    for item in sorted(path.glob("*.json"), key=lambda value: value.name.lower()):
        try:
            payload = json.loads(item.read_text(encoding="utf-8-sig"))
        except Exception as exc:
            warnings.append(f"{item.name}: json parse error ({exc})")
            continue

        parsed, parse_warnings = _parse_script(payload, item.stem, used_ids)
        for warn in parse_warnings:
            warnings.append(f"{item.name}: {warn}")
        if parsed:
            scripts.append(parsed)

    return scripts, warnings


def convert_legacy_custom_scripts(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []

    result: list[dict[str, object]] = []
    used_ids: set[str] = set()
    for index, item in enumerate(value, start=1):
        if not isinstance(item, dict):
            continue

        script_id = _normalize_id(str(item.get("id", "")).strip(), fallback=f"legacy_{index}", max_len=SCRIPT_ID_MAX)
        if not script_id.startswith("legacy_"):
            script_id = f"legacy_{script_id}"
        script_id = _ensure_unique(script_id[:SCRIPT_ID_MAX], used_ids, SCRIPT_ID_MAX)

        name = str(item.get("name", "")).strip()[:80] or f"Legacy Script {index}"
        description = str(item.get("description", "")).strip()[:240]
        commands = _normalize_commands(item.get("commands", []))
        if not commands:
            continue

        result.append(
            {
                "id": script_id,
                "name": name,
                "description": description,
                "source": "config.json",
                "buttons": [
                    {
                        "id": "run",
                        "text": "Run",
                        "action": {
                            "type": "commands",
                            "commands": commands,
                            "timeout_sec": 90,
                            "stop_on_error": False,
                        },
                    }
                ],
            }
        )

    return result


def _parse_script(
    payload: object,
    fallback_stem: str,
    used_script_ids: set[str],
) -> tuple[dict[str, object] | None, list[str]]:
    warnings: list[str] = []
    if not isinstance(payload, dict):
        return None, ["root must be an object"]

    raw_id = str(payload.get("id", "")).strip()
    script_id = _normalize_id(raw_id, fallback=_normalize_id(fallback_stem, fallback="script"), max_len=SCRIPT_ID_MAX)
    script_id = _ensure_unique(script_id, used_script_ids, SCRIPT_ID_MAX)

    name = str(payload.get("name", "")).strip()[:80]
    if not name:
        name = script_id.replace("_", " ").replace("-", " ").strip().title() or "Script"
    description = str(payload.get("description", "")).strip()[:240]

    raw_buttons = payload.get("buttons")
    if raw_buttons is None:
        legacy_commands = _normalize_commands(payload.get("commands", []))
        if legacy_commands:
            raw_buttons = [
                {
                    "id": "run",
                    "text": "Run",
                    "action": {"type": "commands", "commands": legacy_commands},
                }
            ]
        else:
            raw_buttons = []

    if not isinstance(raw_buttons, list) or not raw_buttons:
        return None, ["buttons must be a non-empty list"]

    buttons: list[dict[str, object]] = []
    used_button_ids: set[str] = set()
    for index, raw_button in enumerate(raw_buttons, start=1):
        parsed_button, button_warnings = _parse_button(raw_button, index, used_button_ids)
        warnings.extend(button_warnings)
        if parsed_button:
            buttons.append(parsed_button)

    if not buttons:
        return None, ["no valid buttons in script"]

    return (
        {
            "id": script_id,
            "name": name,
            "description": description,
            "buttons": buttons,
            "source": f"{fallback_stem}.json",
        },
        warnings,
    )


def _parse_button(
    payload: object,
    index: int,
    used_button_ids: set[str],
) -> tuple[dict[str, object] | None, list[str]]:
    warnings: list[str] = []
    if not isinstance(payload, dict):
        return None, [f"button #{index}: must be an object"]

    button_id = _normalize_id(str(payload.get("id", "")).strip(), fallback=f"btn{index}", max_len=BUTTON_ID_MAX)
    button_id = _ensure_unique(button_id, used_button_ids, BUTTON_ID_MAX)

    text = str(payload.get("text", payload.get("label", ""))).strip()[:48]
    if not text:
        text = f"Button {index}"

    action_payload = payload.get("action")
    if not isinstance(action_payload, dict):
        if "command" in payload:
            action_payload = {"type": "command", "command": payload.get("command")}
        elif "commands" in payload:
            action_payload = {"type": "commands", "commands": payload.get("commands")}
        else:
            return None, [f"button '{button_id}': missing action"]

    action, action_error = _normalize_action(action_payload)
    if action_error:
        return None, [f"button '{button_id}': {action_error}"]

    return (
        {
            "id": button_id,
            "text": text,
            "description": str(payload.get("description", "")).strip()[:120],
            "action": action,
        },
        warnings,
    )


def _normalize_action(payload: dict[str, object]) -> tuple[dict[str, object] | None, str]:
    action_type = str(payload.get("type", "")).strip().lower()
    if not action_type:
        return None, "action.type is required"

    if action_type == "command":
        command = str(payload.get("command", "")).strip()
        if not command:
            return None, "command is required"
        return (
            {
                "type": "command",
                "command": command,
                "timeout_sec": _safe_int(payload.get("timeout_sec"), default=90, min_value=1, max_value=600),
            },
            "",
        )

    if action_type == "commands":
        commands = _normalize_commands(payload.get("commands", []))
        if not commands:
            return None, "commands must be a non-empty list"
        return (
            {
                "type": "commands",
                "commands": commands,
                "timeout_sec": _safe_int(payload.get("timeout_sec"), default=90, min_value=1, max_value=600),
                "stop_on_error": bool(payload.get("stop_on_error", False)),
            },
            "",
        )

    if action_type == "open_url":
        url = str(payload.get("url", "")).strip()
        if not url:
            return None, "url is required"
        return ({"type": "open_url", "url": url}, "")

    if action_type == "message":
        text = str(payload.get("text", "")).strip()
        if not text:
            return None, "text is required"
        return ({"type": "message", "text": text[:2000]}, "")

    if action_type == "mode":
        mode = str(payload.get("mode", "")).strip().lower()
        if not mode:
            return None, "mode is required"
        return ({"type": "mode", "mode": mode}, "")

    if action_type == "volume_set":
        return (
            {
                "type": "volume_set",
                "percent": _safe_int(payload.get("percent"), default=40, min_value=0, max_value=100),
            },
            "",
        )

    if action_type in {"volume_mute", "volume_unmute", "lock_screen", "logout", "shutdown", "reboot"}:
        return ({"type": action_type}, "")

    if action_type == "clipboard_set":
        text = str(payload.get("text", ""))
        if not text.strip():
            return None, "text is required"
        return ({"type": "clipboard_set", "text": text[:4000]}, "")

    if action_type == "wake_on_lan":
        mac = str(payload.get("mac", "")).strip()
        if not mac:
            return None, "mac is required"
        broadcast = str(payload.get("broadcast", "255.255.255.255")).strip() or "255.255.255.255"
        port = _safe_int(payload.get("port"), default=9, min_value=1, max_value=65535)
        return (
            {
                "type": "wake_on_lan",
                "mac": mac,
                "broadcast": broadcast,
                "port": port,
            },
            "",
        )

    return None, f"unsupported action type '{action_type}'"


def _normalize_commands(value: object) -> list[str]:
    if isinstance(value, str):
        source = value.splitlines()
    elif isinstance(value, list):
        source = value
    else:
        source = []
    return [str(item).strip() for item in source if str(item).strip()]


def _normalize_id(value: str, fallback: str, max_len: int = 24) -> str:
    lowered = value.strip().lower()
    lowered = lowered.replace(" ", "_")
    cleaned = _ID_CLEAN_RE.sub("_", lowered).strip("_")
    if not cleaned:
        cleaned = _ID_CLEAN_RE.sub("_", fallback.lower()).strip("_") or "item"
    return cleaned[:max_len]


def _ensure_unique(value: str, used: set[str], max_len: int) -> str:
    if value not in used:
        used.add(value)
        return value

    for index in range(2, 1000):
        suffix = f"_{index}"
        trimmed = value[: max(1, max_len - len(suffix))]
        candidate = f"{trimmed}{suffix}"
        if candidate not in used:
            used.add(candidate)
            return candidate

    fallback = value[: max(1, max_len - 4)] + "_999"
    used.add(fallback)
    return fallback


def _safe_int(value: object, default: int, min_value: int, max_value: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        return default
    return min(max(parsed, min_value), max_value)

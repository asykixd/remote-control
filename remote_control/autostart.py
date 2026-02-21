from __future__ import annotations

import os
import sys
from pathlib import Path


APP_RUN_KEY = "RemoteControlTelegramBot"
RUN_REG_PATH = r"Software\Microsoft\Windows\CurrentVersion\Run"


def _build_launch_command() -> str:
    if getattr(sys, "frozen", False):
        exe_path = Path(sys.executable).resolve()
        return f'"{exe_path}"'

    python_path = Path(sys.executable).resolve()
    script_path = (Path(__file__).resolve().parent.parent / "main.py").resolve()
    return f'"{python_path}" "{script_path}"'


def is_supported() -> bool:
    return os.name == "nt"


def is_enabled() -> bool:
    if not is_supported():
        return False

    import winreg

    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, RUN_REG_PATH, 0, winreg.KEY_READ) as key:
            value, _ = winreg.QueryValueEx(key, APP_RUN_KEY)
            return bool(str(value).strip())
    except FileNotFoundError:
        return False
    except OSError:
        return False


def set_enabled(enable: bool) -> None:
    if not is_supported():
        return

    import winreg

    with winreg.OpenKey(winreg.HKEY_CURRENT_USER, RUN_REG_PATH, 0, winreg.KEY_SET_VALUE) as key:
        if enable:
            winreg.SetValueEx(key, APP_RUN_KEY, 0, winreg.REG_SZ, _build_launch_command())
        else:
            try:
                winreg.DeleteValue(key, APP_RUN_KEY)
            except FileNotFoundError:
                pass


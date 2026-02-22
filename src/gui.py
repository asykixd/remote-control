from __future__ import annotations

import logging
import os
import random
import secrets
import subprocess
import threading
import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import messagebox, ttk
from tkinter.scrolledtext import ScrolledText

from src import autostart
from src.bot_service import RemoteControlBot
from src.config import AppConfig, load_config, save_config


class ControlPanelApp:
    def __init__(self, log_file_path: Path | None = None) -> None:
        self.root = tk.Tk()
        self.root.title("Remote Control Hub")
        self.root.geometry("1120x780")
        self.root.minsize(980, 700)

        self._logger = logging.getLogger("remote_control")
        self.log_file_path = log_file_path or Path("logs/app.log")
        self._config = load_config()
        self._bot: RemoteControlBot | None = None
        self._last_running_state = False

        self.status_var = tk.StringVar(value="● Остановлен")
        self.token_var = tk.StringVar(value=self._config.bot_token)
        self.owner_id_var = tk.StringVar(value=str(self._config.owner_user_id or ""))
        self.pin_var = tk.StringVar(value="")
        self.autostart_var = tk.BooleanVar(
            value=autostart.is_enabled() if autostart.is_supported() else self._config.autostart_enabled
        )
        self.premium_emoji_var = tk.StringVar(value=self._config.premium_emoji_id)
        self.effect_id_var = tk.StringVar(value=self._config.message_effect_id)
        self.audit_path_var = tk.StringVar(value=self._config.audit_log_path)

        self.monitor_enabled_var = tk.BooleanVar(value=self._config.monitor_enabled)
        self.temp_alert_var = tk.StringVar(value=f"{self._config.temperature_alert_c:.1f}")
        self.disk_alert_var = tk.StringVar(value=f"{self._config.disk_free_alert_gb:.1f}")
        self.internet_host_var = tk.StringVar(value=self._config.internet_check_host)
        self.internet_port_var = tk.StringVar(value=str(self._config.internet_check_port))
        self.cooldown_var = tk.StringVar(value=str(self._config.alert_cooldown_sec))
        self.sleep_mode_var = tk.StringVar(value="\n".join(self._config.sleep_mode_commands))
        self.work_mode_var = tk.StringVar(value="\n".join(self._config.work_mode_commands))
        self._custom_scripts = self._normalize_custom_scripts(self._config.custom_scripts)
        self.script_name_var = tk.StringVar(value="")
        self.script_description_var = tk.StringVar(value="")
        self._script_id_being_edited: str | None = None

        self._build_ui()
        self._fill_usernames(self._config.allowed_usernames)
        self._fill_user_ids(self._config.allowed_user_ids)
        self._refresh_autostart_state()

        self.root.after(900, self._poll_bot_state)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_ui(self) -> None:
        style = ttk.Style()
        style.theme_use("clam")

        palette = {
            "bg": "#F2F7FB",
            "surface": "#FFFFFF",
            "muted": "#5B6D7D",
            "text": "#162330",
            "accent": "#0A84FF",
            "accent_2": "#00A68E",
            "danger": "#C43B3B",
        }

        self.root.configure(bg=palette["bg"])
        style.configure("App.TFrame", background=palette["bg"])
        style.configure("Card.TFrame", background=palette["surface"])
        style.configure("Title.TLabel", background=palette["bg"], foreground=palette["text"], font=("Segoe UI", 24, "bold"))
        style.configure("Sub.TLabel", background=palette["bg"], foreground=palette["muted"], font=("Segoe UI", 10))
        style.configure("CardTitle.TLabel", background=palette["surface"], foreground=palette["text"], font=("Segoe UI", 11, "bold"))
        style.configure("Status.TLabel", background=palette["surface"], foreground=palette["accent_2"], font=("Segoe UI", 12, "bold"))
        style.configure("DangerStatus.TLabel", background=palette["surface"], foreground=palette["danger"], font=("Segoe UI", 12, "bold"))
        style.configure("Accent.TButton", font=("Segoe UI", 10, "bold"), padding=(10, 6))
        style.configure("Block.TLabelframe", padding=12)
        style.configure("Block.TLabelframe.Label", font=("Segoe UI", 10, "bold"))

        shell = ttk.Frame(self.root, style="App.TFrame", padding=16)
        shell.pack(fill=tk.BOTH, expand=True)

        header = ttk.Frame(shell, style="App.TFrame")
        header.pack(fill=tk.X, pady=(0, 10))
        ttk.Label(header, text="Telegram Remote Control", style="Title.TLabel").pack(anchor=tk.W)
        ttk.Label(
            header,
            text=(
                "Современная панель управления: авторизация по user_id, аудит, мониторинг, "
                "автозапуск, EXE-сборка и расширенное управление ПК через Telegram."
            ),
            style="Sub.TLabel",
        ).pack(anchor=tk.W, pady=(2, 0))

        cards = ttk.Frame(shell, style="App.TFrame")
        cards.pack(fill=tk.X, pady=(0, 12))

        card_status = ttk.Frame(cards, style="Card.TFrame", padding=12)
        card_status.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ttk.Label(card_status, text="Статус бота", style="CardTitle.TLabel").pack(anchor=tk.W)
        self.status_label = ttk.Label(card_status, textvariable=self.status_var, style="DangerStatus.TLabel")
        self.status_label.pack(anchor=tk.W, pady=(6, 0))

        card_log = ttk.Frame(cards, style="Card.TFrame", padding=12)
        card_log.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(10, 0))
        ttk.Label(card_log, text="Логи", style="CardTitle.TLabel").pack(anchor=tk.W)
        ttk.Label(card_log, text=f"Файл: {self.log_file_path}", style="Sub.TLabel").pack(anchor=tk.W, pady=(6, 0))

        notebook = ttk.Notebook(shell)
        notebook.pack(fill=tk.BOTH, expand=True)

        launch_tab = ttk.Frame(notebook, padding=12)
        security_tab = ttk.Frame(notebook, padding=12)
        api_tab = ttk.Frame(notebook, padding=12)
        scripts_tab = ttk.Frame(notebook, padding=12)
        logs_tab = ttk.Frame(notebook, padding=12)
        build_tab = ttk.Frame(notebook, padding=12)

        notebook.add(launch_tab, text="Запуск")
        notebook.add(security_tab, text="Безопасность")
        notebook.add(api_tab, text="API и мониторинг")
        notebook.add(scripts_tab, text="Скрипты")
        notebook.add(logs_tab, text="Логи")
        notebook.add(build_tab, text="EXE")

        self._build_launch_tab(launch_tab)
        self._build_security_tab(security_tab)
        self._build_api_tab(api_tab)
        self._build_scripts_tab(scripts_tab)
        self._build_logs_tab(logs_tab)
        self._build_build_tab(build_tab)

    def _build_launch_tab(self, parent: ttk.Frame) -> None:
        main_box = ttk.LabelFrame(parent, text="Основные параметры", style="Block.TLabelframe")
        main_box.pack(fill=tk.X)

        ttk.Label(main_box, text="Telegram Bot Token").grid(row=0, column=0, sticky="w")
        ttk.Entry(main_box, textvariable=self.token_var, show="*", width=100).grid(row=1, column=0, sticky="we", pady=(2, 10))

        ttk.Label(main_box, text="Owner Telegram ID (установится после /pair)").grid(row=2, column=0, sticky="w")
        ttk.Entry(main_box, textvariable=self.owner_id_var, state="readonly", width=40).grid(row=3, column=0, sticky="w", pady=(2, 10))

        self.autostart_check = ttk.Checkbutton(
            main_box,
            text="Запускать вместе с Windows (HKCU\\...\\Run)",
            variable=self.autostart_var,
        )
        self.autostart_check.grid(row=4, column=0, sticky="w", pady=(2, 8))

        button_bar = ttk.Frame(main_box)
        button_bar.grid(row=5, column=0, sticky="w", pady=(4, 2))
        self.save_btn = ttk.Button(button_bar, text="Сохранить", style="Accent.TButton", command=self._save)
        self.save_btn.pack(side=tk.LEFT)
        self.start_btn = ttk.Button(button_bar, text="Запустить бота", style="Accent.TButton", command=self._start_bot)
        self.start_btn.pack(side=tk.LEFT, padx=8)
        self.stop_btn = ttk.Button(button_bar, text="Остановить бота", command=self._stop_bot, state=tk.DISABLED)
        self.stop_btn.pack(side=tk.LEFT)

        main_box.grid_columnconfigure(0, weight=1)

    def _build_security_tab(self, parent: ttk.Frame) -> None:
        bootstrap_box = ttk.LabelFrame(parent, text="Авторизация и PIN", style="Block.TLabelframe")
        bootstrap_box.pack(fill=tk.BOTH, expand=True)

        ttk.Label(
            bootstrap_box,
            text="Разрешенные username для первой команды /pair (если user_id еще не указан)",
        ).grid(row=0, column=0, sticky="w")
        self.usernames_text = ScrolledText(bootstrap_box, height=6, wrap=tk.WORD)
        self.usernames_text.grid(row=1, column=0, sticky="nsew", pady=(2, 8))

        ttk.Label(
            bootstrap_box,
            text="Разрешенные user_id (основная авторизация, по одному в строке)",
        ).grid(row=2, column=0, sticky="w")
        self.user_ids_text = ScrolledText(bootstrap_box, height=8, wrap=tk.WORD)
        self.user_ids_text.grid(row=3, column=0, sticky="nsew", pady=(2, 8))

        pin_bar = ttk.Frame(bootstrap_box)
        pin_bar.grid(row=4, column=0, sticky="we", pady=(2, 0))
        ttk.Label(pin_bar, text="PIN для /pair и опасных команд").pack(side=tk.LEFT)
        ttk.Entry(pin_bar, textvariable=self.pin_var, show="*", width=18).pack(side=tk.LEFT, padx=(8, 8))
        ttk.Button(pin_bar, text="Сгенерировать PIN", command=self._generate_pin).pack(side=tk.LEFT)

        ttk.Label(
            bootstrap_box,
            text="Если PIN оставить пустым, сохранится старый. Рекомендуем 6+ цифр.",
            style="Sub.TLabel",
        ).grid(row=5, column=0, sticky="w", pady=(8, 0))

        bootstrap_box.grid_columnconfigure(0, weight=1)
        bootstrap_box.grid_rowconfigure(1, weight=1)
        bootstrap_box.grid_rowconfigure(3, weight=1)

    def _build_api_tab(self, parent: ttk.Frame) -> None:
        top = ttk.LabelFrame(parent, text="Telegram API и аудит", style="Block.TLabelframe")
        top.pack(fill=tk.X)

        ttk.Label(top, text="Premium Emoji ID (опционально)").grid(row=0, column=0, sticky="w")
        ttk.Entry(top, textvariable=self.premium_emoji_var, width=64).grid(row=1, column=0, sticky="w", pady=(2, 10))

        ttk.Label(top, text="Message Effect ID (опционально)").grid(row=2, column=0, sticky="w")
        ttk.Entry(top, textvariable=self.effect_id_var, width=64).grid(row=3, column=0, sticky="w", pady=(2, 10))

        ttk.Label(top, text="Путь к audit-логу").grid(row=4, column=0, sticky="w")
        ttk.Entry(top, textvariable=self.audit_path_var, width=64).grid(row=5, column=0, sticky="w", pady=(2, 8))

        monitor = ttk.LabelFrame(parent, text="Мониторинг уведомлений", style="Block.TLabelframe")
        monitor.pack(fill=tk.X, pady=(10, 0))

        ttk.Checkbutton(monitor, text="Включить мониторинг (температура / диск / интернет)", variable=self.monitor_enabled_var).grid(
            row=0, column=0, columnspan=4, sticky="w", pady=(0, 8)
        )

        ttk.Label(monitor, text="Температура > (C)").grid(row=1, column=0, sticky="w")
        ttk.Entry(monitor, textvariable=self.temp_alert_var, width=10).grid(row=1, column=1, sticky="w", padx=(6, 14))

        ttk.Label(monitor, text="Свободно на диске < (GB)").grid(row=1, column=2, sticky="w")
        ttk.Entry(monitor, textvariable=self.disk_alert_var, width=10).grid(row=1, column=3, sticky="w", padx=(6, 0))

        ttk.Label(monitor, text="Хост проверки сети").grid(row=2, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(monitor, textvariable=self.internet_host_var, width=20).grid(row=2, column=1, sticky="w", padx=(6, 14), pady=(8, 0))

        ttk.Label(monitor, text="Порт").grid(row=2, column=2, sticky="w", pady=(8, 0))
        ttk.Entry(monitor, textvariable=self.internet_port_var, width=10).grid(row=2, column=3, sticky="w", padx=(6, 0), pady=(8, 0))

        ttk.Label(monitor, text="Cooldown алертов (сек)").grid(row=3, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(monitor, textvariable=self.cooldown_var, width=10).grid(row=3, column=1, sticky="w", padx=(6, 0), pady=(8, 0))

        modes = ttk.LabelFrame(parent, text="Режимы для Scripts API (mode)", style="Block.TLabelframe")
        modes.pack(fill=tk.BOTH, expand=True, pady=(10, 0))

        ttk.Label(
            modes,
            text=(
                "По одной команде в строке. Команды выполняются по порядку.\n"
                "Используются кнопкой скрипта с action.type = mode (sleep/work)."
            ),
            style="Sub.TLabel",
        ).grid(row=0, column=0, columnspan=2, sticky="w")

        ttk.Label(modes, text="Sleep команды").grid(row=1, column=0, sticky="w", pady=(8, 2))
        ttk.Label(modes, text="Work команды").grid(row=1, column=1, sticky="w", pady=(8, 2), padx=(12, 0))

        self.sleep_mode_text = ScrolledText(modes, height=7, wrap=tk.WORD)
        self.sleep_mode_text.grid(row=2, column=0, sticky="nsew")
        if self.sleep_mode_var.get().strip():
            self.sleep_mode_text.insert("1.0", self.sleep_mode_var.get().strip())

        self.work_mode_text = ScrolledText(modes, height=7, wrap=tk.WORD)
        self.work_mode_text.grid(row=2, column=1, sticky="nsew", padx=(12, 0))
        if self.work_mode_var.get().strip():
            self.work_mode_text.insert("1.0", self.work_mode_var.get().strip())

        modes.grid_columnconfigure(0, weight=1)
        modes.grid_columnconfigure(1, weight=1)
        modes.grid_rowconfigure(2, weight=1)

    def _build_scripts_tab(self, parent: ttk.Frame) -> None:
        shell = ttk.Frame(parent)
        shell.pack(fill=tk.BOTH, expand=True)

        left = ttk.LabelFrame(shell, text="Список скриптов", style="Block.TLabelframe")
        left.pack(side=tk.LEFT, fill=tk.BOTH, expand=False)

        self.scripts_listbox = tk.Listbox(left, height=18)
        self.scripts_listbox.pack(fill=tk.BOTH, expand=True)
        self.scripts_listbox.bind("<<ListboxSelect>>", self._on_script_select)

        left_buttons = ttk.Frame(left)
        left_buttons.pack(fill=tk.X, pady=(8, 0))
        ttk.Button(left_buttons, text="Новый", command=self._clear_script_editor).pack(side=tk.LEFT)
        ttk.Button(left_buttons, text="Удалить", command=self._delete_selected_script).pack(side=tk.LEFT, padx=8)

        right = ttk.LabelFrame(shell, text="Редактор скрипта", style="Block.TLabelframe")
        right.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(12, 0))

        ttk.Label(right, text="Название").grid(row=0, column=0, sticky="w")
        ttk.Entry(right, textvariable=self.script_name_var, width=56).grid(row=1, column=0, sticky="we", pady=(2, 8))

        ttk.Label(right, text="Описание (опционально)").grid(row=2, column=0, sticky="w")
        ttk.Entry(right, textvariable=self.script_description_var, width=56).grid(row=3, column=0, sticky="we", pady=(2, 8))

        ttk.Label(right, text="Команды (по одной в строке)").grid(row=4, column=0, sticky="w")
        self.script_commands_text = ScrolledText(right, height=14, wrap=tk.WORD)
        self.script_commands_text.grid(row=5, column=0, sticky="nsew", pady=(2, 8))

        right_buttons = ttk.Frame(right)
        right_buttons.grid(row=6, column=0, sticky="w")
        ttk.Button(right_buttons, text="Сохранить скрипт", style="Accent.TButton", command=self._save_script_from_editor).pack(side=tk.LEFT)
        ttk.Button(right_buttons, text="Очистить форму", command=self._clear_script_editor).pack(side=tk.LEFT, padx=8)

        right.grid_columnconfigure(0, weight=1)
        right.grid_rowconfigure(5, weight=1)
        self._refresh_scripts_listbox()

    def _refresh_scripts_listbox(self) -> None:
        self.scripts_listbox.delete(0, tk.END)
        for item in self._custom_scripts:
            name = str(item.get("name", "Script")).strip()
            if name:
                self.scripts_listbox.insert(tk.END, name)

    def _on_script_select(self, _event: object | None = None) -> None:
        selection = self.scripts_listbox.curselection()
        if not selection:
            return
        index = int(selection[0])
        if index < 0 or index >= len(self._custom_scripts):
            return

        item = self._custom_scripts[index]
        self._script_id_being_edited = str(item.get("id", "")).strip() or None
        self.script_name_var.set(str(item.get("name", "")).strip())
        self.script_description_var.set(str(item.get("description", "")).strip())
        commands = item.get("commands", [])
        commands_text = "\n".join(str(v).strip() for v in commands if str(v).strip()) if isinstance(commands, list) else ""
        self.script_commands_text.delete("1.0", tk.END)
        if commands_text:
            self.script_commands_text.insert("1.0", commands_text)

    def _save_script_from_editor(self) -> None:
        name = self.script_name_var.get().strip()
        description = self.script_description_var.get().strip()
        commands = [
            line.strip()
            for line in self.script_commands_text.get("1.0", tk.END).splitlines()
            if line.strip()
        ]
        if not name:
            messagebox.showerror("Скрипты", "Укажите название скрипта.")
            return
        if not commands:
            messagebox.showerror("Скрипты", "Добавьте хотя бы одну команду.")
            return

        target_id = (self._script_id_being_edited or "").strip()
        updated = False
        for item in self._custom_scripts:
            if str(item.get("id", "")).strip() == target_id and target_id:
                item["name"] = name
                item["description"] = description
                item["commands"] = commands
                updated = True
                break

        if not updated:
            self._custom_scripts.append(
                {
                    "id": self._new_script_id(),
                    "name": name,
                    "description": description,
                    "commands": commands,
                }
            )

        self._custom_scripts = self._normalize_custom_scripts(self._custom_scripts)
        self._refresh_scripts_listbox()
        self._persist_scripts_only()
        self._log(f"Скрипт сохранен: {name}")

    def _delete_selected_script(self) -> None:
        selection = self.scripts_listbox.curselection()
        if not selection:
            messagebox.showinfo("Скрипты", "Выберите скрипт в списке.")
            return
        index = int(selection[0])
        if index < 0 or index >= len(self._custom_scripts):
            return

        removed = self._custom_scripts.pop(index)
        self._refresh_scripts_listbox()
        self._clear_script_editor()
        self._persist_scripts_only()
        self._log(f"Скрипт удален: {removed.get('name', 'unknown')}")

    def _clear_script_editor(self) -> None:
        self._script_id_being_edited = None
        self.script_name_var.set("")
        self.script_description_var.set("")
        self.script_commands_text.delete("1.0", tk.END)

    def _persist_scripts_only(self) -> None:
        cfg = load_config()
        cfg.custom_scripts = list(self._normalize_custom_scripts(self._custom_scripts))
        save_config(cfg)
        self._config = load_config()

    def _build_logs_tab(self, parent: ttk.Frame) -> None:
        logs_box = ttk.LabelFrame(parent, text="Живой лог приложения", style="Block.TLabelframe")
        logs_box.pack(fill=tk.BOTH, expand=True)

        top = ttk.Frame(logs_box)
        top.pack(fill=tk.X, pady=(0, 8))
        ttk.Button(top, text="Открыть папку логов", command=self._open_logs_folder).pack(side=tk.LEFT)
        ttk.Button(top, text="Очистить окно", command=self._clear_log_view).pack(side=tk.LEFT, padx=8)

        self.logs = ScrolledText(logs_box, state=tk.DISABLED, wrap=tk.WORD)
        self.logs.pack(fill=tk.BOTH, expand=True)

    def _build_build_tab(self, parent: ttk.Frame) -> None:
        build_box = ttk.LabelFrame(parent, text="Сборка в EXE", style="Block.TLabelframe")
        build_box.pack(fill=tk.BOTH, expand=True)

        info_text = (
            "Сборка выполняется через PyInstaller и скрипт build_exe.ps1.\n"
            "Результат: dist/RemoteControlHub/RemoteControlHub.exe\n\n"
            "Перед сборкой убедитесь, что установлены зависимости: pip install -r requirements.txt"
        )
        ttk.Label(build_box, text=info_text, style="Sub.TLabel").pack(anchor=tk.W, pady=(0, 12))

        row = ttk.Frame(build_box)
        row.pack(anchor=tk.W)
        ttk.Button(row, text="Собрать EXE", style="Accent.TButton", command=self._build_exe).pack(side=tk.LEFT)
        ttk.Button(row, text="Открыть папку dist", command=self._open_dist_folder).pack(side=tk.LEFT, padx=8)

    def _generate_pin(self) -> None:
        self.pin_var.set("".join(str(random.randint(0, 9)) for _ in range(6)))

    def _fill_usernames(self, usernames: list[str]) -> None:
        self.usernames_text.delete("1.0", tk.END)
        if usernames:
            self.usernames_text.insert("1.0", "\n".join(usernames))

    def _fill_user_ids(self, user_ids: list[int]) -> None:
        self.user_ids_text.delete("1.0", tk.END)
        if user_ids:
            self.user_ids_text.insert("1.0", "\n".join(str(v) for v in user_ids))

    def _collect_config(self) -> AppConfig:
        token = self.token_var.get().strip()
        usernames_raw = self.usernames_text.get("1.0", tk.END).replace(",", "\n")
        user_ids_raw = self.user_ids_text.get("1.0", tk.END).replace(",", "\n").replace(";", "\n")
        usernames = [line.strip() for line in usernames_raw.splitlines() if line.strip()]

        user_ids: list[int] = []
        for line in user_ids_raw.splitlines():
            value = line.strip()
            if value.isdigit():
                user_ids.append(int(value))

        owner_user_id: int | None = None
        owner_raw = self.owner_id_var.get().strip()
        if owner_raw.isdigit():
            owner_user_id = int(owner_raw)

        sleep_mode_commands = [
            line.strip()
            for line in self.sleep_mode_text.get("1.0", tk.END).splitlines()
            if line.strip()
        ]
        work_mode_commands = [
            line.strip()
            for line in self.work_mode_text.get("1.0", tk.END).splitlines()
            if line.strip()
        ]

        config = AppConfig(
            bot_token=token,
            allowed_usernames=usernames,
            allowed_user_ids=user_ids,
            owner_user_id=owner_user_id,
            pin_salt=self._config.pin_salt,
            pin_hash=self._config.pin_hash,
            premium_emoji_id=self.premium_emoji_var.get().strip(),
            message_effect_id=self.effect_id_var.get().strip(),
            audit_log_path=self.audit_path_var.get().strip() or "audit.log",
            autostart_enabled=bool(self.autostart_var.get()),
            monitor_enabled=bool(self.monitor_enabled_var.get()),
            temperature_alert_c=self._safe_float(self.temp_alert_var.get(), 85.0),
            disk_free_alert_gb=self._safe_float(self.disk_alert_var.get(), 5.0),
            internet_check_host=self.internet_host_var.get().strip() or "1.1.1.1",
            internet_check_port=self._safe_int(self.internet_port_var.get(), 53),
            alert_cooldown_sec=self._safe_int(self.cooldown_var.get(), 900),
            scheduled_tasks=list(self._config.scheduled_tasks),
            sleep_mode_commands=sleep_mode_commands,
            work_mode_commands=work_mode_commands,
            custom_scripts=list(self._normalize_custom_scripts(self._custom_scripts)),
        )

        pin_raw = self.pin_var.get().strip()
        if pin_raw:
            config.set_pin(pin_raw)

        return config

    def _refresh_autostart_state(self) -> None:
        if not autostart.is_supported():
            self.autostart_check.configure(state=tk.DISABLED)

    def _save(self) -> None:
        config = self._collect_config()
        if not config.bot_token:
            messagebox.showerror("Ошибка", "Введите токен Telegram-бота.")
            return
        if not config.has_pin:
            messagebox.showerror("Ошибка", "Укажите PIN (минимум один раз).")
            return
        if config.owner_user_id is None and not config.normalized_usernames and not config.normalized_user_ids:
            messagebox.showerror("Ошибка", "Добавьте user_id или username для первой привязки.")
            return

        save_config(config)
        self._config = load_config()
        self.owner_id_var.set(str(self._config.owner_user_id or ""))
        self._fill_user_ids(self._config.allowed_user_ids)
        self._custom_scripts = self._normalize_custom_scripts(self._config.custom_scripts)
        self._refresh_scripts_listbox()

        if autostart.is_supported():
            try:
                autostart.set_enabled(config.autostart_enabled)
            except Exception as exc:
                self._log(f"Не удалось изменить автозапуск: {exc}")
                messagebox.showwarning("Автозапуск", f"Не удалось изменить автозапуск: {exc}")

        self._log("Настройки сохранены.")
        messagebox.showinfo("Сохранено", "Настройки успешно сохранены.")

    def _start_bot(self) -> None:
        config = self._collect_config()
        if not config.bot_token:
            messagebox.showerror("Ошибка", "Введите токен Telegram-бота.")
            return
        if not config.has_pin:
            messagebox.showerror("Ошибка", "Укажите PIN для безопасности.")
            return

        save_config(config)
        self._config = load_config()

        if self._bot and self._bot.is_running:
            messagebox.showinfo("Информация", "Бот уже запущен.")
            return

        self._bot = RemoteControlBot(
            config=self._config,
            log=self._log,
            persist_config=self._persist_config_from_bot,
        )
        try:
            self._bot.start()
        except Exception as exc:
            self._log(f"Не удалось запустить бота: {exc}")
            messagebox.showerror("Ошибка запуска", str(exc))
            return

        self._set_running_state(True)
        self._log("Бот запущен.")

    def _stop_bot(self) -> None:
        if self._bot:
            self._bot.stop()
        self._set_running_state(False)
        self._log("Бот остановлен вручную.")

    def _set_running_state(self, running: bool) -> None:
        self._last_running_state = running
        if running:
            self.status_var.set("● Запущен")
            self.status_label.configure(style="Status.TLabel")
        else:
            self.status_var.set("● Остановлен")
            self.status_label.configure(style="DangerStatus.TLabel")
        self.start_btn.configure(state=tk.DISABLED if running else tk.NORMAL)
        self.stop_btn.configure(state=tk.NORMAL if running else tk.DISABLED)

    def _poll_bot_state(self) -> None:
        running = bool(self._bot and self._bot.is_running)
        if running != self._last_running_state:
            self._set_running_state(running)
            if not running:
                self._log("Бот остановлен.")
        self.root.after(900, self._poll_bot_state)

    def _persist_config_from_bot(self, config: AppConfig) -> None:
        save_config(config)
        self.root.after(0, self._reload_security_view)

    def _reload_security_view(self) -> None:
        self._config = load_config()
        self.owner_id_var.set(str(self._config.owner_user_id or ""))
        self._fill_user_ids(self._config.allowed_user_ids)
        self._custom_scripts = self._normalize_custom_scripts(self._config.custom_scripts)
        self._refresh_scripts_listbox()
        self._log("Конфигурация обновлена из бота (/pair).")

    def _build_exe(self) -> None:
        def worker() -> None:
            self._log("Запуск сборки EXE...")
            try:
                result = subprocess.run(
                    [
                        "powershell",
                        "-NoProfile",
                        "-ExecutionPolicy",
                        "Bypass",
                        "-File",
                        "build_exe.ps1",
                    ],
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="ignore",
                    timeout=900,
                )
            except Exception as exc:
                self._log(f"Сборка EXE завершилась с ошибкой запуска: {exc}")
                return

            output = (result.stdout or "") + "\n" + (result.stderr or "")
            for line in output.splitlines():
                if line.strip():
                    self._log(f"[build] {line.strip()}")

            if result.returncode == 0:
                self._log("Сборка EXE завершена успешно.")
                self.root.after(0, lambda: messagebox.showinfo("EXE", "Сборка завершена. Проверьте папку dist."))
            else:
                self._log("Сборка EXE завершилась с ошибкой.")
                self.root.after(0, lambda: messagebox.showerror("EXE", "Сборка не удалась. См. лог."))

        threading.Thread(target=worker, daemon=True).start()

    def _open_dist_folder(self) -> None:
        folder = Path("dist").resolve()
        if not folder.exists():
            messagebox.showwarning("dist", "Папка dist пока не создана.")
            return
        try:
            if os.name == "nt":
                os.startfile(folder)  # type: ignore[attr-defined]
        except Exception as exc:
            messagebox.showerror("Ошибка", f"Не удалось открыть папку dist: {exc}")

    def _open_logs_folder(self) -> None:
        folder = self.log_file_path.parent.resolve()
        try:
            if os.name == "nt":
                os.startfile(folder)  # type: ignore[attr-defined]
            elif os.name == "posix":
                subprocess.Popen(["xdg-open", str(folder)])
        except Exception as exc:
            messagebox.showerror("Ошибка", f"Не удалось открыть папку логов: {exc}")

    def _clear_log_view(self) -> None:
        self.logs.configure(state=tk.NORMAL)
        self.logs.delete("1.0", tk.END)
        self.logs.configure(state=tk.DISABLED)

    def _log(self, message: str) -> None:
        self.root.after(0, self._append_log, message)

    def _append_log(self, message: str) -> None:
        now = datetime.now().strftime("%H:%M:%S")
        line = f"[{now}] {message}\n"
        self.logs.configure(state=tk.NORMAL)
        self.logs.insert(tk.END, line)
        self.logs.see(tk.END)
        self.logs.configure(state=tk.DISABLED)
        self._logger.info(message)

    def _on_close(self) -> None:
        try:
            if self._bot:
                self._bot.stop()
        finally:
            self.root.destroy()

    def run(self) -> None:
        self.root.mainloop()

    @staticmethod
    def _normalize_custom_scripts(value: object) -> list[dict[str, object]]:
        if not isinstance(value, list):
            return []

        scripts: list[dict[str, object]] = []
        for item in value:
            if not isinstance(item, dict):
                continue

            script_id = str(item.get("id", "")).strip()[:24]
            name = str(item.get("name", "")).strip()[:80]
            description = str(item.get("description", "")).strip()[:240]
            commands_raw = item.get("commands", [])

            if isinstance(commands_raw, str):
                commands_source = commands_raw.splitlines()
            elif isinstance(commands_raw, list):
                commands_source = commands_raw
            else:
                commands_source = []

            commands = [str(v).strip() for v in commands_source if str(v).strip()]
            if not name or not commands:
                continue
            if not script_id:
                script_id = secrets.token_hex(4)

            scripts.append(
                {
                    "id": script_id,
                    "name": name,
                    "description": description,
                    "commands": commands,
                }
            )
        return scripts

    @staticmethod
    def _new_script_id() -> str:
        return secrets.token_hex(4)

    @staticmethod
    def _safe_float(raw: str, default: float) -> float:
        try:
            return float(raw)
        except Exception:
            return default

    @staticmethod
    def _safe_int(raw: str, default: int) -> int:
        try:
            return int(raw)
        except Exception:
            return default

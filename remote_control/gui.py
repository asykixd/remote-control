from __future__ import annotations

import logging
import os
import random
import subprocess
import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import messagebox, ttk
from tkinter.scrolledtext import ScrolledText

from remote_control import autostart
from remote_control.bot_service import RemoteControlBot
from remote_control.config import AppConfig, load_config, save_config


class ControlPanelApp:
    def __init__(self, log_file_path: Path | None = None) -> None:
        self.root = tk.Tk()
        self.root.title("Remote Control Hub")
        self.root.geometry("980x720")
        self.root.minsize(880, 640)

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

        self._build_ui()
        self._fill_usernames(self._config.allowed_usernames)
        self._fill_user_ids(self._config.allowed_user_ids)
        self._refresh_autostart_state()

        self.root.after(900, self._poll_bot_state)
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_ui(self) -> None:
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("Title.TLabel", font=("Segoe UI", 20, "bold"))
        style.configure("Sub.TLabel", foreground="#5A5A5A")
        style.configure("Block.TLabelframe", padding=12)
        style.configure("Status.TLabel", font=("Segoe UI", 11, "bold"))
        style.configure("Accent.TButton", font=("Segoe UI", 10, "bold"))

        shell = ttk.Frame(self.root, padding=16)
        shell.pack(fill=tk.BOTH, expand=True)

        header = ttk.Frame(shell)
        header.pack(fill=tk.X, pady=(0, 10))
        ttk.Label(header, text="Telegram Remote Control", style="Title.TLabel").pack(anchor=tk.W)
        ttk.Label(
            header,
            text="Безопасное управление ПК через бота: owner/pair/PIN, аудит, автозапуск и логи",
            style="Sub.TLabel",
        ).pack(anchor=tk.W, pady=(2, 0))

        top_status = ttk.Frame(shell)
        top_status.pack(fill=tk.X, pady=(4, 10))
        ttk.Label(top_status, text="Статус:", style="Sub.TLabel").pack(side=tk.LEFT)
        ttk.Label(top_status, textvariable=self.status_var, style="Status.TLabel").pack(side=tk.LEFT, padx=(6, 16))
        ttk.Label(top_status, text=f"Лог файла: {self.log_file_path}", style="Sub.TLabel").pack(side=tk.LEFT)

        notebook = ttk.Notebook(shell)
        notebook.pack(fill=tk.BOTH, expand=True)

        launch_tab = ttk.Frame(notebook, padding=12)
        security_tab = ttk.Frame(notebook, padding=12)
        api_tab = ttk.Frame(notebook, padding=12)
        logs_tab = ttk.Frame(notebook, padding=12)

        notebook.add(launch_tab, text="Запуск")
        notebook.add(security_tab, text="Безопасность")
        notebook.add(api_tab, text="API/Оформление")
        notebook.add(logs_tab, text="Логи")

        self._build_launch_tab(launch_tab)
        self._build_security_tab(security_tab)
        self._build_api_tab(api_tab)
        self._build_logs_tab(logs_tab)

    def _build_launch_tab(self, parent: ttk.Frame) -> None:
        main_box = ttk.LabelFrame(parent, text="Основные параметры", style="Block.TLabelframe")
        main_box.pack(fill=tk.X)

        ttk.Label(main_box, text="Telegram Bot Token").grid(row=0, column=0, sticky="w")
        ttk.Entry(main_box, textvariable=self.token_var, show="*", width=90).grid(
            row=1, column=0, sticky="we", pady=(2, 10)
        )

        ttk.Label(main_box, text="Owner Telegram ID (авто после /pair)").grid(row=2, column=0, sticky="w")
        ttk.Entry(main_box, textvariable=self.owner_id_var, state="readonly", width=40).grid(
            row=3, column=0, sticky="w", pady=(2, 10)
        )

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
        bootstrap_box = ttk.LabelFrame(parent, text="Первичная привязка (/pair)", style="Block.TLabelframe")
        bootstrap_box.pack(fill=tk.BOTH, expand=True)

        ttk.Label(
            bootstrap_box,
            text="Разрешенные username для первой команды /pair (по одному в строке, можно с @)",
        ).grid(row=0, column=0, sticky="w")
        self.usernames_text = ScrolledText(bootstrap_box, height=8, wrap=tk.WORD)
        self.usernames_text.grid(row=1, column=0, sticky="nsew", pady=(2, 8))

        ttk.Label(bootstrap_box, text="Разрешенные user_id (по одному в строке)").grid(row=2, column=0, sticky="w")
        self.user_ids_text = ScrolledText(bootstrap_box, height=6, wrap=tk.WORD)
        self.user_ids_text.grid(row=3, column=0, sticky="nsew", pady=(2, 8))

        pin_bar = ttk.Frame(bootstrap_box)
        pin_bar.grid(row=4, column=0, sticky="we", pady=(2, 0))
        ttk.Label(pin_bar, text="PIN для /pair и опасных команд").pack(side=tk.LEFT)
        ttk.Entry(pin_bar, textvariable=self.pin_var, show="*", width=18).pack(side=tk.LEFT, padx=(8, 8))
        ttk.Button(pin_bar, text="Сгенерировать PIN", command=self._generate_pin).pack(side=tk.LEFT)

        ttk.Label(
            bootstrap_box,
            text="Если PIN оставить пустым, сохранится старый PIN. Минимум 4 символа.",
            style="Sub.TLabel",
        ).grid(row=5, column=0, sticky="w", pady=(8, 0))

        bootstrap_box.grid_columnconfigure(0, weight=1)
        bootstrap_box.grid_rowconfigure(1, weight=1)
        bootstrap_box.grid_rowconfigure(3, weight=1)

    def _build_api_tab(self, parent: ttk.Frame) -> None:
        api_box = ttk.LabelFrame(parent, text="Новые возможности Telegram API", style="Block.TLabelframe")
        api_box.pack(fill=tk.BOTH, expand=True)

        ttk.Label(
            api_box,
            text="Premium Emoji ID (опционально, для заголовка). Пример: 5368324170671202286",
        ).grid(row=0, column=0, sticky="w")
        ttk.Entry(api_box, textvariable=self.premium_emoji_var, width=64).grid(row=1, column=0, sticky="w", pady=(2, 10))

        ttk.Label(
            api_box,
            text="Message Effect ID (опционально, Telegram эффект сообщения в личных чатах)",
        ).grid(row=2, column=0, sticky="w")
        ttk.Entry(api_box, textvariable=self.effect_id_var, width=64).grid(row=3, column=0, sticky="w", pady=(2, 10))

        ttk.Label(api_box, text="Путь к audit-логу").grid(row=4, column=0, sticky="w")
        ttk.Entry(api_box, textvariable=self.audit_path_var, width=64).grid(row=5, column=0, sticky="w", pady=(2, 10))

        ttk.Label(
            api_box,
            text="Примечание: Telegram не поддерживает нативную покраску inline-кнопок, "
            "поэтому используется цветовое кодирование эмодзи.",
            style="Sub.TLabel",
            wraplength=760,
        ).grid(row=6, column=0, sticky="w")

    def _build_logs_tab(self, parent: ttk.Frame) -> None:
        logs_box = ttk.LabelFrame(parent, text="Живой лог приложения", style="Block.TLabelframe")
        logs_box.pack(fill=tk.BOTH, expand=True)

        top = ttk.Frame(logs_box)
        top.pack(fill=tk.X, pady=(0, 8))
        ttk.Button(top, text="Открыть папку логов", command=self._open_logs_folder).pack(side=tk.LEFT)
        ttk.Button(top, text="Очистить окно", command=self._clear_log_view).pack(side=tk.LEFT, padx=8)

        self.logs = ScrolledText(logs_box, state=tk.DISABLED, wrap=tk.WORD)
        self.logs.pack(fill=tk.BOTH, expand=True)

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
            if not value:
                continue
            if value.isdigit():
                user_ids.append(int(value))

        owner_user_id: int | None = None
        owner_raw = self.owner_id_var.get().strip()
        if owner_raw.isdigit():
            owner_user_id = int(owner_raw)

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
            messagebox.showerror("Ошибка", "Добавьте username или user_id для первой привязки.")
            return

        save_config(config)
        self._config = load_config()
        self.owner_id_var.set(str(self._config.owner_user_id or ""))
        self._fill_user_ids(self._config.allowed_user_ids)

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
        self.status_var.set("● Запущен" if running else "● Остановлен")
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
        self._log("Конфигурация обновлена из бота (/pair).")

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


from __future__ import annotations

import asyncio
import ctypes
import html
import platform
import secrets
import shutil
import socket
import subprocess
import threading
import time
import urllib.parse
import webbrowser
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from string import hexdigits
from typing import Callable

import mss
import mss.tools
import psutil
from telegram import Bot, CopyTextButton, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import TelegramError
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

from remote_control import autostart
from remote_control.audit import AuditStore
from remote_control.config import AppConfig, load_config, save_config
from remote_control.script_api import convert_legacy_custom_scripts, ensure_scripts_dir, load_scripts_from_directory


LogFn = Callable[[str], None]
PersistConfigFn = Callable[[AppConfig], None]


@dataclass
class ScheduledTask:
    task_id: str
    when_iso: str
    command: str
    created_by: str = ""
    reason: str = ""

    def when_utc(self) -> datetime | None:
        try:
            return datetime.fromisoformat(self.when_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            return None


class RemoteControlBot:
    def __init__(
        self,
        config: AppConfig,
        log: LogFn,
        persist_config: PersistConfigFn | None = None,
    ) -> None:
        self.config = config
        self.token = config.bot_token.strip()
        self.allowed_usernames = set(config.normalized_usernames)
        self.allowed_user_ids = set(config.normalized_user_ids)
        self.owner_user_id = config.owner_user_id
        self._log = log
        self._persist_config = persist_config
        self._audit = AuditStore(config.audit_log_path)

        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop_event = threading.Event()
        self._application: Application | None = None
        self._pending_actions: dict[int, dict[str, str]] = {}

        self._scheduled_tasks: dict[str, ScheduledTask] = {}
        for raw_task in self.config.scheduled_tasks:
            if not isinstance(raw_task, dict):
                continue
            task_id = str(raw_task.get("id", "")).strip()
            when_iso = str(raw_task.get("when_iso", "")).strip()
            command = str(raw_task.get("command", "")).strip()
            created_by = str(raw_task.get("created_by", "")).strip()
            reason = str(raw_task.get("reason", "")).strip()
            if task_id and when_iso and command:
                self._scheduled_tasks[task_id] = ScheduledTask(
                    task_id=task_id,
                    when_iso=when_iso,
                    command=command,
                    created_by=created_by,
                    reason=reason,
                )

        self._alert_state: dict[str, bool] = {
            "internet_down": False,
            "disk_low": False,
            "temp_high": False,
        }
        self._last_alert_sent_at: dict[str, float] = {}
        self._volume_before_mute: int | None = None
        self._scripts_dir = Path("Scripts")
        self._custom_scripts: list[dict[str, object]] = self._normalize_custom_scripts(self.config.custom_scripts)
        self._script_items: list[dict[str, object]] = []
        self._script_warnings: list[str] = []
        self._sync_scripts()

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        if self.is_running:
            return
        if not self.token:
            raise ValueError("Пустой токен Telegram-бота.")
        if not self.config.has_pin:
            raise ValueError("PIN не задан. Укажите PIN в интерфейсе и сохраните настройки.")
        if self.owner_user_id is None and not self.allowed_usernames and not self.allowed_user_ids:
            raise ValueError("Для первой привязки добавьте хотя бы один разрешенный user_id или username.")

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_thread, name="telegram-bot", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if not self.is_running:
            return
        self._stop_event.set()
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(lambda: None)

        if self._thread:
            self._thread.join(timeout=12)
        self._thread = None
        self._loop = None
        self._application = None
        self._pending_actions.clear()

    def _run_thread(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._run())
        except Exception as exc:
            self._log(f"[Ошибка] Бот остановлен: {exc}")
        finally:
            self._loop.close()
            self._log("Бот остановлен.")

    async def _run(self) -> None:
        application = Application.builder().token(self.token).build()
        self._application = application

        application.add_handler(CommandHandler("start", self._on_start))
        application.add_handler(CommandHandler("help", self._on_start))
        application.add_handler(CommandHandler("menu", self._on_start))
        application.add_handler(CommandHandler("pair", self._on_pair))
        application.add_handler(CommandHandler("id", self._on_id))
        application.add_handler(CommandHandler("history", self._on_history))
        application.add_handler(CommandHandler("tasks", self._on_tasks))
        application.add_handler(CommandHandler("scripts", self._on_scripts))
        application.add_handler(CommandHandler("cancel", self._on_cancel))
        application.add_handler(CallbackQueryHandler(self._on_button))
        application.add_handler(MessageHandler(filters.Document.ALL, self._on_document))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self._on_text))

        await application.initialize()
        await application.start()
        if application.updater is None:
            raise RuntimeError("Updater не инициализирован.")
        await application.updater.start_polling(drop_pending_updates=True)

        scheduler_task = asyncio.create_task(self._scheduler_loop(application.bot), name="scheduler-loop")
        monitor_task = asyncio.create_task(self._monitor_loop(application.bot), name="monitor-loop")

        self._log("Бот запущен и ожидает команды.")
        try:
            while not self._stop_event.is_set():
                await asyncio.sleep(0.3)
        finally:
            for task in (scheduler_task, monitor_task):
                task.cancel()
            await asyncio.gather(scheduler_task, monitor_task, return_exceptions=True)
            await application.updater.stop()
            await application.stop()
            await application.shutdown()

    def _main_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("📸 Скриншот", callback_data="screenshot"),
                    InlineKeyboardButton("📊 Статус", callback_data="stats"),
                ],
                [
                    InlineKeyboardButton("🖥 Рабочий стол", callback_data="menu_desktop"),
                    InlineKeyboardButton("⚙️ Система", callback_data="menu_system"),
                ],
                [
                    InlineKeyboardButton("📁 Файлы", callback_data="menu_files"),
                    InlineKeyboardButton("🔊 Звук и буфер", callback_data="menu_media"),
                ],
                [
                    InlineKeyboardButton("🧠 Процессы", callback_data="menu_process"),
                    InlineKeyboardButton("⏰ Задачи", callback_data="menu_tasks"),
                ],
                [
                    InlineKeyboardButton("🌐 Сеть и WoL", callback_data="menu_network"),
                    InlineKeyboardButton("🔐 Питание", callback_data="menu_power"),
                ],
                [InlineKeyboardButton("📜 Скрипты", callback_data="menu_scripts")],
                [
                    InlineKeyboardButton("📜 История", callback_data="history"),
                    InlineKeyboardButton("🆔 Мой ID", callback_data="show_id"),
                ],
            ]
        )

    def _desktop_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("📝 Показать сообщение", callback_data="prompt_message"),
                    InlineKeyboardButton("🌍 Открыть ссылку", callback_data="prompt_link"),
                ],
                [InlineKeyboardButton("🔒 Заблокировать экран", callback_data="lock_screen")],
                [InlineKeyboardButton("⬅️ Главное меню", callback_data="menu_main")],
            ]
        )

    def _system_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("🛠 Службы: список", callback_data="list_services"),
                    InlineKeyboardButton("▶️ Запустить службу", callback_data="prompt_service_start"),
                ],
                [InlineKeyboardButton("⏹ Остановить службу", callback_data="prompt_service_stop")],
                [
                    InlineKeyboardButton("🚀 Автозагрузка: список", callback_data="list_startup"),
                    InlineKeyboardButton("➕ Добавить", callback_data="prompt_startup_add"),
                ],
                [
                    InlineKeyboardButton("➖ Удалить", callback_data="prompt_startup_remove"),
                    InlineKeyboardButton("🔁 Наш автозапуск", callback_data="toggle_autostart"),
                ],
                [InlineKeyboardButton("⬅️ Главное меню", callback_data="menu_main")],
            ]
        )

    def _files_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("⬇️ Скачать с ПК", callback_data="prompt_file_download"),
                    InlineKeyboardButton("⬆️ Загрузить на ПК", callback_data="prompt_file_upload"),
                ],
                [
                    InlineKeyboardButton("📦 Переместить", callback_data="prompt_file_move"),
                    InlineKeyboardButton("🗑 Удалить", callback_data="prompt_file_delete"),
                ],
                [InlineKeyboardButton("⬅️ Главное меню", callback_data="menu_main")],
            ]
        )

    def _media_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("🔉 Текущая громкость", callback_data="volume_get"),
                    InlineKeyboardButton("🎚 Установить", callback_data="prompt_volume_set"),
                ],
                [
                    InlineKeyboardButton("🔇 Мут", callback_data="volume_mute"),
                    InlineKeyboardButton("🔊 Анмут", callback_data="volume_unmute"),
                ],
                [
                    InlineKeyboardButton("📋 Получить буфер", callback_data="clipboard_get"),
                    InlineKeyboardButton("📝 Вставить в буфер", callback_data="prompt_clipboard_set"),
                ],
                [InlineKeyboardButton("⬅️ Главное меню", callback_data="menu_main")],
            ]
        )

    def _process_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("📈 Список процессов", callback_data="list_processes"),
                    InlineKeyboardButton("⛔ Завершить PID", callback_data="prompt_proc_kill"),
                ],
                [InlineKeyboardButton("▶️ Запустить процесс", callback_data="prompt_proc_start")],
                [InlineKeyboardButton("⬅️ Главное меню", callback_data="menu_main")],
            ]
        )

    def _tasks_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("📅 Список задач", callback_data="tasks_list"),
                    InlineKeyboardButton("➕ Добавить", callback_data="prompt_task_add"),
                ],
                [
                    InlineKeyboardButton("➖ Удалить", callback_data="prompt_task_remove"),
                    InlineKeyboardButton("🚨 Мониторинг", callback_data="toggle_monitoring"),
                ],
                [InlineKeyboardButton("🧪 Проверить сейчас", callback_data="run_monitor_now")],
                [InlineKeyboardButton("⬅️ Главное меню", callback_data="menu_main")],
            ]
        )

    def _network_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("🌍 Проверка интернета", callback_data="internet_check"),
                    InlineKeyboardButton("🟢 Wake on LAN", callback_data="prompt_wol_send"),
                ],
                [InlineKeyboardButton("⬅️ Главное меню", callback_data="menu_main")],
            ]
        )

    def _power_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("🔒 Блокировка экрана", callback_data="lock_screen"),
                    InlineKeyboardButton("🚪 Выход из учетки", callback_data="confirm_logout"),
                ],
                [
                    InlineKeyboardButton("🔄 Перезагрузка", callback_data="confirm_reboot"),
                    InlineKeyboardButton("⛔ Выключение", callback_data="confirm_shutdown"),
                ],
                [InlineKeyboardButton("⬅️ Главное меню", callback_data="menu_main")],
            ]
        )

    def _scripts_keyboard(self) -> InlineKeyboardMarkup:
        rows: list[list[InlineKeyboardButton]] = []
        for script in self._script_items[:24]:
            script_id = str(script.get("id", "")).strip()
            name = str(script.get("name", "Script")).strip()
            if not script_id:
                continue
            rows.append([InlineKeyboardButton(f"🧩 {name[:42]}", callback_data=f"script_open:{script_id}")])

        rows.append([InlineKeyboardButton("⬅️ Главное меню", callback_data="menu_main")])
        return InlineKeyboardMarkup(rows)

    def _script_detail_keyboard(self, script: dict[str, object]) -> InlineKeyboardMarkup:
        rows: list[list[InlineKeyboardButton]] = []
        script_id = str(script.get("id", "")).strip()
        buttons = script.get("buttons", [])
        row: list[InlineKeyboardButton] = []
        if isinstance(buttons, list):
            for item in buttons[:24]:
                if not isinstance(item, dict):
                    continue
                button_id = str(item.get("id", "")).strip()
                label = str(item.get("text", "Action")).strip() or "Action"
                if not script_id or not button_id:
                    continue
                row.append(InlineKeyboardButton(label[:32], callback_data=f"script_btn:{script_id}:{button_id}"))
                if len(row) == 2:
                    rows.append(row)
                    row = []
        if row:
            rows.append(row)

        rows.append([InlineKeyboardButton("⬅️ К списку скриптов", callback_data="menu_scripts")])
        rows.append([InlineKeyboardButton("⬅️ Главное меню", callback_data="menu_main")])
        return InlineKeyboardMarkup(rows)

    def _danger_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="cancel_pending")]])

    def _premium_icon(self, fallback: str) -> str:
        custom_id = self.config.premium_emoji_id.strip()
        if custom_id.isdigit():
            return f'<tg-emoji emoji-id="{custom_id}">{fallback}</tg-emoji>'
        return fallback

    def _is_allowed(self, user_id: int) -> bool:
        if self.owner_user_id is not None and user_id == self.owner_user_id:
            return True
        return user_id in self.allowed_user_ids

    def _remember_pending(self, chat_id: int, mode: str, payload: str = "") -> None:
        self._pending_actions[chat_id] = {"mode": mode, "payload": payload}

    def _pop_pending(self, chat_id: int) -> dict[str, str]:
        return self._pending_actions.pop(chat_id, {})

    async def _send_message(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        chat_id: int,
        text: str,
        reply_markup: InlineKeyboardMarkup | None = None,
        parse_html: bool = True,
    ) -> None:
        await self._send_message_with_bot(
            context.bot,
            chat_id=chat_id,
            text=text,
            reply_markup=reply_markup,
            parse_html=parse_html,
        )

    async def _send_message_with_bot(
        self,
        bot: Bot,
        chat_id: int,
        text: str,
        reply_markup: InlineKeyboardMarkup | None = None,
        parse_html: bool = True,
    ) -> None:
        kwargs: dict[str, object] = {
            "chat_id": chat_id,
            "text": text,
            "reply_markup": reply_markup,
        }
        if parse_html:
            kwargs["parse_mode"] = ParseMode.HTML

        effect_id = self.config.message_effect_id.strip()
        if effect_id:
            kwargs["message_effect_id"] = effect_id

        try:
            await bot.send_message(**kwargs)
        except TelegramError:
            if "message_effect_id" in kwargs:
                kwargs.pop("message_effect_id")
                await bot.send_message(**kwargs)
            else:
                raise

    async def _send_photo(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        chat_id: int,
        photo: bytes,
        caption: str,
    ) -> None:
        kwargs: dict[str, object] = {
            "chat_id": chat_id,
            "photo": photo,
            "caption": caption,
            "parse_mode": ParseMode.HTML,
            "show_caption_above_media": True,
        }
        effect_id = self.config.message_effect_id.strip()
        if effect_id:
            kwargs["message_effect_id"] = effect_id

        try:
            await context.bot.send_photo(**kwargs)
        except TelegramError:
            if "message_effect_id" in kwargs:
                kwargs.pop("message_effect_id")
                await context.bot.send_photo(**kwargs)
            else:
                raise

    def _audit_action(self, update: Update, action: str, status: str, details: str = "") -> None:
        user = update.effective_user
        user_id = user.id if user else 0
        username = (user.username or "") if user else ""
        self._audit.append(user_id=user_id, username=username, action=action, status=status, details=details)

    async def _check_access(self, update: Update, action: str) -> bool:
        user = update.effective_user
        if user and self._is_allowed(user.id):
            return True

        if update.effective_message:
            await update.effective_message.reply_text(
                "⛔ Доступ запрещен.\n"
                "Для привязки владельца используйте: /pair <PIN>\n"
                "Узнать свой ID: /id"
            )

        username = (user.username or "").strip() if user else ""
        user_id = user.id if user else 0
        self._log(f"[Безопасность] Отказано в доступе: action={action}, id={user_id}, username=@{username}")
        self._audit_action(update, action=action, status="denied", details="access_denied")
        return False

    async def _on_id(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_user or not update.effective_chat:
            return
        user = update.effective_user
        text = (
            "<b>🆔 Ваш Telegram ID</b>\n"
            f"<code>{user.id}</code>\n"
            f"Username: @{html.escape(user.username or 'нет')}"
        )
        await self._send_message(context, update.effective_chat.id, text, parse_html=True)

    async def _on_pair(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_user or not update.effective_chat:
            return

        user = update.effective_user
        chat_id = update.effective_chat.id
        username = (user.username or "").strip().lstrip("@").lower()

        if self.owner_user_id is not None:
            if user.id == self.owner_user_id:
                await self._send_message(context, chat_id, "✅ Этот аккаунт уже привязан как владелец.")
                return
            await self._send_message(context, chat_id, "⛔ Владелец уже привязан. Обратитесь к администратору.")
            self._audit_action(update, action="pair", status="denied", details="owner_exists")
            return

        if self.allowed_user_ids and user.id not in self.allowed_user_ids:
            await self._send_message(
                context,
                chat_id,
                "⛔ Для первичной привязки ваш <b>user_id</b> отсутствует в разрешенном списке.",
            )
            self._audit_action(update, action="pair", status="denied", details="user_id_not_allowed")
            return

        if not self.allowed_user_ids and self.allowed_usernames and username not in self.allowed_usernames:
            await self._send_message(
                context,
                chat_id,
                "⛔ Ваш username отсутствует в bootstrap-списке."
                " Добавьте user_id или username в приложении и попробуйте снова.",
            )
            self._audit_action(update, action="pair", status="denied", details="username_not_allowed")
            return

        if not self.config.has_pin:
            await self._send_message(context, chat_id, "⛔ PIN не задан в приложении. Сначала задайте PIN в GUI.")
            self._audit_action(update, action="pair", status="denied", details="pin_missing")
            return

        if not context.args:
            hint_markup = InlineKeyboardMarkup(
                [[InlineKeyboardButton("📋 Скопировать шаблон", copy_text=CopyTextButton(text="/pair 1234"))]]
            )
            await self._send_message(
                context,
                chat_id,
                "<b>🔐 Первая привязка владельца</b>\n"
                "Отправьте команду в формате:\n"
                "<code>/pair 1234</code>",
                reply_markup=hint_markup,
            )
            return

        pin = context.args[0].strip()
        if not self.config.verify_pin(pin):
            await self._send_message(context, chat_id, "⛔ Неверный PIN.")
            self._audit_action(update, action="pair", status="denied", details="bad_pin")
            return

        self.owner_user_id = user.id
        self.allowed_user_ids.add(user.id)
        self.config.owner_user_id = user.id
        self.config.allowed_user_ids = sorted(self.allowed_user_ids)
        self._save_config()

        self._audit_action(update, action="pair", status="ok", details="owner_linked")
        await self._send_message(
            context,
            chat_id,
            "✅ <b>Владелец успешно привязан.</b>\n"
            f"Ваш ID: <code>{user.id}</code>\n"
            "Теперь используйте /start",
        )
        self._log(f"[Безопасность] Привязан владелец id={user.id} username=@{username or 'none'}")

    async def _on_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._check_access(update, action="start"):
            return
        if not update.effective_chat:
            return

        self._audit_action(update, action="start", status="ok")
        icon = self._premium_icon("⚙️")
        text = (
            f"<b>{icon} Панель управления ПК</b>\n"
            "Управление устройством через Telegram.\n\n"
            "<i>Подсказка:</i> для отмены текущего ввода используйте /cancel"
        )
        await self._send_message(context, update.effective_chat.id, text, reply_markup=self._main_keyboard())

    async def _on_history(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._check_access(update, action="history"):
            return
        if not update.effective_chat:
            return

        records = self._audit.tail(limit=16)
        if not records:
            await self._send_message(context, update.effective_chat.id, "📜 История пока пустая.")
            return

        lines = ["<b>📜 Последние действия</b>"]
        for item in records:
            stamp = self._short_ts(item.ts)
            user = f"@{html.escape(item.username)}" if item.username else str(item.user_id)
            details = f"\n<code>{html.escape(item.details[:180])}</code>" if item.details else ""
            lines.append(
                f"• <code>{stamp}</code> {html.escape(item.action)}: <b>{html.escape(item.status)}</b> by {user}{details}"
            )

        await self._send_message(context, update.effective_chat.id, "\n".join(lines))

    async def _on_tasks(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._check_access(update, action="tasks"):
            return
        if not update.effective_chat:
            return
        await self._send_message(
            context,
            update.effective_chat.id,
            self._format_tasks(),
            reply_markup=self._tasks_keyboard(),
        )

    async def _on_scripts(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._check_access(update, action="scripts"):
            return
        if not update.effective_chat:
            return

        self._sync_scripts()
        self._audit_action(update, action="scripts", status="ok")
        await self._send_message(
            context,
            update.effective_chat.id,
            self._format_scripts_overview(),
            reply_markup=self._scripts_keyboard(),
        )

    async def _on_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._check_access(update, action="cancel"):
            return
        if not update.effective_chat:
            return

        self._pending_actions.pop(update.effective_chat.id, None)
        await self._send_message(
            context,
            update.effective_chat.id,
            "❎ Текущее действие отменено.",
            reply_markup=self._main_keyboard(),
        )

    async def _on_button(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        if query is None:
            return
        await query.answer()

        if not await self._check_access(update, action=f"button:{query.data or ''}"):
            return
        if query.message is None:
            return

        chat_id = query.message.chat_id
        action = query.data or ""

        if action == "menu_main":
            await self._send_message(context, chat_id, "<b>Главное меню</b>", reply_markup=self._main_keyboard())
            return
        if action == "menu_desktop":
            await self._send_message(context, chat_id, "<b>🖥 Рабочий стол</b>", reply_markup=self._desktop_keyboard())
            return
        if action == "menu_system":
            await self._send_message(context, chat_id, "<b>⚙️ Система</b>", reply_markup=self._system_keyboard())
            return
        if action == "menu_files":
            await self._send_message(context, chat_id, "<b>📁 Файловые операции</b>", reply_markup=self._files_keyboard())
            return
        if action == "menu_media":
            await self._send_message(context, chat_id, "<b>🔊 Звук и буфер обмена</b>", reply_markup=self._media_keyboard())
            return
        if action == "menu_process":
            await self._send_message(context, chat_id, "<b>🧠 Управление процессами</b>", reply_markup=self._process_keyboard())
            return
        if action == "menu_tasks":
            await self._send_message(context, chat_id, self._format_tasks(), reply_markup=self._tasks_keyboard())
            return
        if action == "menu_network":
            await self._send_message(context, chat_id, "<b>🌐 Сеть и Wake-on-LAN</b>", reply_markup=self._network_keyboard())
            return
        if action == "menu_power":
            await self._send_message(context, chat_id, "<b>🔐 Питание и сессия</b>", reply_markup=self._power_keyboard())
            return
        if action == "menu_scripts":
            self._sync_scripts()
            await self._send_message(
                context,
                chat_id,
                self._format_scripts_overview(),
                reply_markup=self._scripts_keyboard(),
            )
            return

        if action.startswith("script_open:"):
            self._sync_scripts()
            script_id = action.split(":", 1)[1].strip()
            await self._open_script(update, context, chat_id, script_id)
            return

        if action.startswith("script_btn:"):
            self._sync_scripts()
            payload = action.split(":", 2)
            if len(payload) != 3:
                await self._send_message(context, chat_id, "⚠️ Некорректный callback для кнопки скрипта.")
                return
            script_id = payload[1].strip()
            button_id = payload[2].strip()
            await self._run_script_button(update, context, chat_id, script_id, button_id)
            return

        if action == "screenshot":
            await self._send_screenshot(chat_id, context, update)
            return

        if action == "stats":
            self._audit_action(update, action="stats", status="ok")
            await self._send_message(context, chat_id, self._format_stats())
            return

        if action == "history":
            await self._on_history(update, context)
            return

        if action == "show_id":
            await self._on_id(update, context)
            return

        if action == "prompt_message":
            self._remember_pending(chat_id, "message")
            await self._send_message(
                context,
                chat_id,
                "📝 Введите текст, который нужно показать на экране.",
                reply_markup=self._danger_keyboard(),
            )
            return

        if action == "prompt_link":
            self._remember_pending(chat_id, "link")
            await self._send_message(
                context,
                chat_id,
                "🌍 Введите ссылку (только <code>http/https</code>).",
                reply_markup=self._danger_keyboard(),
            )
            return

        if action == "lock_screen":
            try:
                self._lock_screen()
                self._audit_action(update, action="lock_screen", status="ok")
                await self._send_message(context, chat_id, "✅ Экран заблокирован.")
            except Exception as exc:
                self._audit_action(update, action="lock_screen", status="error", details=str(exc))
                await self._send_message(context, chat_id, f"⚠️ Ошибка: {html.escape(str(exc))}")
            return

        if action == "confirm_logout":
            self._remember_pending(chat_id, "pin", "logout")
            await self._send_message(
                context,
                chat_id,
                "🔐 Для выхода из учетной записи отправьте PIN.",
                reply_markup=self._danger_keyboard(),
            )
            return

        if action == "confirm_shutdown":
            self._remember_pending(chat_id, "pin", "shutdown")
            await self._send_message(
                context,
                chat_id,
                "🔐 Для подтверждения выключения отправьте PIN.",
                reply_markup=self._danger_keyboard(),
            )
            return

        if action == "confirm_reboot":
            self._remember_pending(chat_id, "pin", "reboot")
            await self._send_message(
                context,
                chat_id,
                "🔐 Для подтверждения перезагрузки отправьте PIN.",
                reply_markup=self._danger_keyboard(),
            )
            return

        if action == "list_processes":
            self._audit_action(update, action="list_processes", status="ok")
            await self._send_message(context, chat_id, self._format_processes())
            return

        if action == "prompt_proc_kill":
            self._remember_pending(chat_id, "proc_kill")
            await self._send_message(
                context,
                chat_id,
                "⛔ Введите PID процесса для завершения.",
                reply_markup=self._danger_keyboard(),
            )
            return

        if action == "prompt_proc_start":
            self._remember_pending(chat_id, "proc_start")
            await self._send_message(
                context,
                chat_id,
                "▶️ Введите команду для запуска процесса.\n"
                "Пример: <code>notepad.exe</code> или <code>powershell -NoProfile</code>",
                reply_markup=self._danger_keyboard(),
            )
            return

        if action == "list_services":
            try:
                text = self._format_services()
                self._audit_action(update, action="list_services", status="ok")
            except Exception as exc:
                text = f"⚠️ Ошибка: {html.escape(str(exc))}"
                self._audit_action(update, action="list_services", status="error", details=str(exc))
            await self._send_message(context, chat_id, text)
            return

        if action == "prompt_service_start":
            self._remember_pending(chat_id, "service_start")
            await self._send_message(context, chat_id, "▶️ Введите имя службы для запуска.", reply_markup=self._danger_keyboard())
            return

        if action == "prompt_service_stop":
            self._remember_pending(chat_id, "service_stop")
            await self._send_message(context, chat_id, "⏹ Введите имя службы для остановки.", reply_markup=self._danger_keyboard())
            return

        if action == "list_startup":
            try:
                text = self._format_startup_entries()
                self._audit_action(update, action="list_startup", status="ok")
            except Exception as exc:
                text = f"⚠️ Ошибка: {html.escape(str(exc))}"
                self._audit_action(update, action="list_startup", status="error", details=str(exc))
            await self._send_message(context, chat_id, text)
            return

        if action == "prompt_startup_add":
            self._remember_pending(chat_id, "startup_add")
            await self._send_message(
                context,
                chat_id,
                "➕ Введите в формате:\n<code>Название | Команда</code>",
                reply_markup=self._danger_keyboard(),
            )
            return

        if action == "prompt_startup_remove":
            self._remember_pending(chat_id, "startup_remove")
            await self._send_message(
                context,
                chat_id,
                "➖ Введите название записи автозагрузки для удаления.",
                reply_markup=self._danger_keyboard(),
            )
            return

        if action == "toggle_autostart":
            try:
                enabled = not autostart.is_enabled()
                autostart.set_enabled(enabled)
                self.config.autostart_enabled = enabled
                self._save_config()
                self._audit_action(update, action="toggle_autostart", status="ok", details=f"enabled={enabled}")
                state = "включен" if enabled else "выключен"
                await self._send_message(context, chat_id, f"✅ Автозапуск приложения {state}.")
            except Exception as exc:
                self._audit_action(update, action="toggle_autostart", status="error", details=str(exc))
                await self._send_message(context, chat_id, f"⚠️ Ошибка: {html.escape(str(exc))}")
            return

        if action == "prompt_file_download":
            self._remember_pending(chat_id, "file_download")
            await self._send_message(
                context,
                chat_id,
                "⬇️ Введите путь к файлу на ПК для отправки в Telegram.",
                reply_markup=self._danger_keyboard(),
            )
            return

        if action == "prompt_file_upload":
            self._remember_pending(chat_id, "file_upload_dir")
            await self._send_message(
                context,
                chat_id,
                "⬆️ Введите папку на ПК, куда сохранить файл из Telegram.",
                reply_markup=self._danger_keyboard(),
            )
            return

        if action == "prompt_file_move":
            self._remember_pending(chat_id, "file_move")
            await self._send_message(
                context,
                chat_id,
                "📦 Введите в формате:\n<code>источник | назначение</code>",
                reply_markup=self._danger_keyboard(),
            )
            return

        if action == "prompt_file_delete":
            self._remember_pending(chat_id, "file_delete")
            await self._send_message(
                context,
                chat_id,
                "🗑 Введите путь к файлу или папке для удаления.",
                reply_markup=self._danger_keyboard(),
            )
            return

        if action == "volume_get":
            try:
                level = self._get_system_volume()
                self._audit_action(update, action="volume_get", status="ok", details=f"level={level}")
                await self._send_message(context, chat_id, f"🔉 Текущая громкость: <b>{level}%</b>")
            except Exception as exc:
                self._audit_action(update, action="volume_get", status="error", details=str(exc))
                await self._send_message(context, chat_id, f"⚠️ Ошибка: {html.escape(str(exc))}")
            return

        if action == "prompt_volume_set":
            self._remember_pending(chat_id, "volume_set")
            await self._send_message(
                context,
                chat_id,
                "🎚 Введите громкость от 0 до 100.",
                reply_markup=self._danger_keyboard(),
            )
            return

        if action == "volume_mute":
            try:
                self._mute_system_volume()
                self._audit_action(update, action="volume_mute", status="ok")
                await self._send_message(context, chat_id, "🔇 Звук приглушен.")
            except Exception as exc:
                self._audit_action(update, action="volume_mute", status="error", details=str(exc))
                await self._send_message(context, chat_id, f"⚠️ Ошибка: {html.escape(str(exc))}")
            return

        if action == "volume_unmute":
            try:
                self._unmute_system_volume()
                self._audit_action(update, action="volume_unmute", status="ok")
                await self._send_message(context, chat_id, "🔊 Звук восстановлен.")
            except Exception as exc:
                self._audit_action(update, action="volume_unmute", status="error", details=str(exc))
                await self._send_message(context, chat_id, f"⚠️ Ошибка: {html.escape(str(exc))}")
            return

        if action == "clipboard_get":
            try:
                text = self._get_clipboard_text()
                preview = text[:3800] if text else "(пусто)"
                self._audit_action(update, action="clipboard_get", status="ok", details=f"len={len(text)}")
                await self._send_message(context, chat_id, f"📋 Буфер обмена:\n<code>{html.escape(preview)}</code>")
            except Exception as exc:
                self._audit_action(update, action="clipboard_get", status="error", details=str(exc))
                await self._send_message(context, chat_id, f"⚠️ Ошибка: {html.escape(str(exc))}")
            return

        if action == "prompt_clipboard_set":
            self._remember_pending(chat_id, "clipboard_set")
            await self._send_message(
                context,
                chat_id,
                "📝 Отправьте текст для вставки в буфер обмена.",
                reply_markup=self._danger_keyboard(),
            )
            return

        if action == "tasks_list":
            await self._send_message(context, chat_id, self._format_tasks(), reply_markup=self._tasks_keyboard())
            return

        if action == "prompt_task_add":
            self._remember_pending(chat_id, "task_add")
            await self._send_message(
                context,
                chat_id,
                "➕ Введите задачу в формате:\n"
                "<code>YYYY-MM-DD HH:MM | команда | зачем (опционально)</code>",
                reply_markup=self._danger_keyboard(),
            )
            return

        if action == "prompt_task_remove":
            self._remember_pending(chat_id, "task_remove")
            await self._send_message(
                context,
                chat_id,
                "➖ Введите ID задачи для удаления.",
                reply_markup=self._danger_keyboard(),
            )
            return

        if action == "toggle_monitoring":
            self.config.monitor_enabled = not self.config.monitor_enabled
            self._save_config()
            state = "включен" if self.config.monitor_enabled else "выключен"
            self._audit_action(update, action="toggle_monitoring", status="ok", details=f"enabled={self.config.monitor_enabled}")
            await self._send_message(context, chat_id, f"🚨 Мониторинг {state}.")
            return

        if action == "run_monitor_now":
            alerts = await self._check_monitor_alerts(context.bot, force_send=True)
            self._audit_action(update, action="run_monitor_now", status="ok", details=f"alerts={alerts}")
            await self._send_message(context, chat_id, "✅ Проверка мониторинга выполнена.")
            return

        if action == "internet_check":
            online = self._check_internet_available(self.config.internet_check_host, self.config.internet_check_port)
            self._audit_action(update, action="internet_check", status="ok", details=f"online={online}")
            status_text = "🟢 Интернет доступен" if online else "🔴 Интернет недоступен"
            await self._send_message(context, chat_id, status_text)
            return

        if action == "prompt_wol_send":
            self._remember_pending(chat_id, "wol_send")
            await self._send_message(
                context,
                chat_id,
                "🟢 Введите:\n<code>MAC [BROADCAST_IP] [PORT]</code>\n"
                "Пример: <code>AA:BB:CC:DD:EE:FF 192.168.0.255 9</code>",
                reply_markup=self._danger_keyboard(),
            )
            return

        if action == "cancel_pending":
            self._pending_actions.pop(chat_id, None)
            await self._send_message(context, chat_id, "❎ Действие отменено.", reply_markup=self._main_keyboard())
            return

        await self._send_message(context, chat_id, "ℹ️ Неизвестное действие. Используйте /start.")

    async def _on_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._check_access(update, action="text"):
            return
        if not update.effective_message or not update.effective_chat:
            return

        chat_id = update.effective_chat.id
        state = self._pop_pending(chat_id)
        text = update.effective_message.text.strip()
        mode = state.get("mode", "")

        if not mode:
            await self._send_message(context, chat_id, "ℹ️ Выберите действие через меню /start.", reply_markup=self._main_keyboard())
            return

        if mode == "message":
            self._show_message_async(text)
            self._audit_action(update, action="show_message", status="ok", details=f"text={text[:120]}")
            self._log("[Команда] Сообщение на экран отправлено.")
            await self._send_message(context, chat_id, "✅ Сообщение показано на экране.", reply_markup=self._desktop_keyboard())
            return

        if mode == "link":
            ok, result = self._open_link(text)
            if ok:
                self._audit_action(update, action="open_link", status="ok", details=result)
                self._log(f"[Команда] Открыта ссылка: {result}")
                await self._send_message(context, chat_id, "✅ Ссылка открыта в браузере.", reply_markup=self._desktop_keyboard())
            else:
                self._audit_action(update, action="open_link", status="denied", details=result)
                self._remember_pending(chat_id, "link")
                await self._send_message(context, chat_id, f"⚠️ Ошибка: {html.escape(result)}\nПопробуйте снова.")
            return

        if mode == "pin":
            action = state.get("payload", "")
            if not self.config.verify_pin(self._extract_pin(text)):
                self._audit_action(update, action=f"{action}_pin", status="denied", details="bad_pin")
                self._remember_pending(chat_id, "pin", action)
                await self._send_message(context, chat_id, "⛔ Неверный PIN. Попробуйте еще раз.")
                return

            if action == "shutdown":
                try:
                    self._shutdown_pc(reboot=False)
                    self._audit_action(update, action="shutdown", status="ok")
                    await self._send_message(context, chat_id, "✅ Команда на выключение отправлена.")
                except Exception as exc:
                    self._audit_action(update, action="shutdown", status="error", details=str(exc))
                    await self._send_message(context, chat_id, f"⚠️ Ошибка выключения: {html.escape(str(exc))}")
                return

            if action == "reboot":
                try:
                    self._shutdown_pc(reboot=True)
                    self._audit_action(update, action="reboot", status="ok")
                    await self._send_message(context, chat_id, "✅ Команда на перезагрузку отправлена.")
                except Exception as exc:
                    self._audit_action(update, action="reboot", status="error", details=str(exc))
                    await self._send_message(context, chat_id, f"⚠️ Ошибка перезагрузки: {html.escape(str(exc))}")
                return

            if action == "logout":
                try:
                    self._logout_user()
                    self._audit_action(update, action="logout", status="ok")
                    await self._send_message(context, chat_id, "✅ Команда выхода из учетной записи отправлена.")
                except Exception as exc:
                    self._audit_action(update, action="logout", status="error", details=str(exc))
                    await self._send_message(context, chat_id, f"⚠️ Ошибка выхода: {html.escape(str(exc))}")
                return

        if mode == "proc_kill":
            if not text.isdigit():
                self._remember_pending(chat_id, "proc_kill")
                await self._send_message(context, chat_id, "⚠️ Нужен числовой PID.")
                return
            pid = int(text)
            try:
                proc_name = self._kill_process(pid)
                self._audit_action(update, action="proc_kill", status="ok", details=f"pid={pid};name={proc_name}")
                await self._send_message(context, chat_id, f"✅ Процесс завершен: <code>{html.escape(proc_name)}</code> ({pid})")
            except Exception as exc:
                self._audit_action(update, action="proc_kill", status="error", details=f"pid={pid};err={exc}")
                await self._send_message(context, chat_id, f"⚠️ Ошибка: {html.escape(str(exc))}")
            return

        if mode == "proc_start":
            try:
                self._start_process(text)
                self._audit_action(update, action="proc_start", status="ok", details=f"cmd={text[:200]}")
                await self._send_message(context, chat_id, "✅ Процесс запущен.")
            except Exception as exc:
                self._audit_action(update, action="proc_start", status="error", details=f"cmd={text[:200]};err={exc}")
                await self._send_message(context, chat_id, f"⚠️ Ошибка запуска: {html.escape(str(exc))}")
            return

        if mode == "service_start":
            try:
                out = self._service_control(text, start=True)
                self._audit_action(update, action="service_start", status="ok", details=f"name={text}")
                await self._send_message(context, chat_id, f"✅ Служба запущена.\n<code>{html.escape(out[:3000])}</code>")
            except Exception as exc:
                self._audit_action(update, action="service_start", status="error", details=f"name={text};err={exc}")
                await self._send_message(context, chat_id, f"⚠️ Ошибка: {html.escape(str(exc))}")
            return

        if mode == "service_stop":
            try:
                out = self._service_control(text, start=False)
                self._audit_action(update, action="service_stop", status="ok", details=f"name={text}")
                await self._send_message(context, chat_id, f"✅ Служба остановлена.\n<code>{html.escape(out[:3000])}</code>")
            except Exception as exc:
                self._audit_action(update, action="service_stop", status="error", details=f"name={text};err={exc}")
                await self._send_message(context, chat_id, f"⚠️ Ошибка: {html.escape(str(exc))}")
            return

        if mode == "startup_add":
            try:
                name, command = self._parse_pair(text)
                self._startup_add(name, command)
                self._audit_action(update, action="startup_add", status="ok", details=f"name={name};cmd={command[:120]}")
                await self._send_message(context, chat_id, "✅ Запись автозагрузки добавлена.")
            except Exception as exc:
                self._audit_action(update, action="startup_add", status="error", details=str(exc))
                await self._send_message(context, chat_id, f"⚠️ Ошибка: {html.escape(str(exc))}")
            return

        if mode == "startup_remove":
            try:
                self._startup_remove(text)
                self._audit_action(update, action="startup_remove", status="ok", details=f"name={text}")
                await self._send_message(context, chat_id, "✅ Запись автозагрузки удалена.")
            except Exception as exc:
                self._audit_action(update, action="startup_remove", status="error", details=str(exc))
                await self._send_message(context, chat_id, f"⚠️ Ошибка: {html.escape(str(exc))}")
            return

        if mode == "file_download":
            await self._handle_file_download(update, context, text)
            return

        if mode == "file_upload_dir":
            directory = self._normalize_path(text)
            if not directory.exists() or not directory.is_dir():
                self._remember_pending(chat_id, "file_upload_dir")
                await self._send_message(context, chat_id, "⚠️ Папка не существует. Введите корректный путь.")
                return
            self._remember_pending(chat_id, "file_upload_wait", str(directory))
            await self._send_message(
                context,
                chat_id,
                f"✅ Папка выбрана: <code>{html.escape(str(directory))}</code>\n"
                "Теперь отправьте файл как документ в этот чат.",
                reply_markup=self._danger_keyboard(),
            )
            return

        if mode == "file_move":
            try:
                src, dst = self._parse_pair(text)
                src_path = self._normalize_path(src)
                dst_path = self._normalize_path(dst)
                if not src_path.exists():
                    raise FileNotFoundError("Источник не найден")
                dst_path.parent.mkdir(parents=True, exist_ok=True)
                result = shutil.move(str(src_path), str(dst_path))
                self._audit_action(update, action="file_move", status="ok", details=f"src={src_path};dst={dst_path}")
                await self._send_message(context, chat_id, f"✅ Перемещено: <code>{html.escape(result)}</code>")
            except Exception as exc:
                self._audit_action(update, action="file_move", status="error", details=str(exc))
                await self._send_message(context, chat_id, f"⚠️ Ошибка перемещения: {html.escape(str(exc))}")
            return

        if mode == "file_delete":
            try:
                path = self._normalize_path(text)
                if not path.exists():
                    raise FileNotFoundError("Путь не найден")
                if path.is_dir():
                    shutil.rmtree(path)
                else:
                    path.unlink()
                self._audit_action(update, action="file_delete", status="ok", details=f"path={path}")
                await self._send_message(context, chat_id, "✅ Удалено.")
            except Exception as exc:
                self._audit_action(update, action="file_delete", status="error", details=str(exc))
                await self._send_message(context, chat_id, f"⚠️ Ошибка удаления: {html.escape(str(exc))}")
            return

        if mode == "volume_set":
            try:
                value = int(text)
                if value < 0 or value > 100:
                    raise ValueError("Громкость должна быть от 0 до 100")
                self._set_system_volume(value)
                self._audit_action(update, action="volume_set", status="ok", details=f"value={value}")
                await self._send_message(context, chat_id, f"✅ Громкость установлена: {value}%")
            except Exception as exc:
                self._audit_action(update, action="volume_set", status="error", details=str(exc))
                await self._send_message(context, chat_id, f"⚠️ Ошибка: {html.escape(str(exc))}")
            return

        if mode == "clipboard_set":
            try:
                self._set_clipboard_text(text)
                self._audit_action(update, action="clipboard_set", status="ok", details=f"len={len(text)}")
                await self._send_message(context, chat_id, "✅ Текст помещен в буфер обмена.")
            except Exception as exc:
                self._audit_action(update, action="clipboard_set", status="error", details=str(exc))
                await self._send_message(context, chat_id, f"⚠️ Ошибка буфера: {html.escape(str(exc))}")
            return

        if mode == "task_add":
            try:
                task = self._create_scheduled_task(text, update)
                self._scheduled_tasks[task.task_id] = task
                self._persist_scheduled_tasks()
                self._audit_action(
                    update,
                    action="task_add",
                    status="ok",
                    details=f"task={task.task_id};when={task.when_iso};cmd={task.command[:120]};why={task.reason[:80]}",
                )
                await self._send_message(context, chat_id, f"✅ Задача добавлена: <code>{task.task_id}</code>")
            except Exception as exc:
                self._audit_action(update, action="task_add", status="error", details=str(exc))
                self._remember_pending(chat_id, "task_add")
                await self._send_message(context, chat_id, f"⚠️ Ошибка: {html.escape(str(exc))}\nПроверьте формат и попробуйте снова.")
            return

        if mode == "task_remove":
            task_id = text.strip()
            task = self._scheduled_tasks.pop(task_id, None)
            if task is None:
                self._remember_pending(chat_id, "task_remove")
                await self._send_message(context, chat_id, "⚠️ Задача с таким ID не найдена.")
                return
            self._persist_scheduled_tasks()
            self._audit_action(update, action="task_remove", status="ok", details=f"task={task_id}")
            await self._send_message(context, chat_id, f"✅ Задача удалена: <code>{task_id}</code>")
            return

        if mode == "wol_send":
            try:
                result = self._wake_on_lan_from_text(text)
                self._audit_action(update, action="wake_on_lan", status="ok", details=result)
                await self._send_message(context, chat_id, f"✅ Magic packet отправлен.\n<code>{html.escape(result)}</code>")
            except Exception as exc:
                self._audit_action(update, action="wake_on_lan", status="error", details=str(exc))
                await self._send_message(context, chat_id, f"⚠️ Ошибка WoL: {html.escape(str(exc))}")
            return

        await self._send_message(
            context,
            chat_id,
            "ℹ️ Выберите действие через меню /start.",
            reply_markup=self._main_keyboard(),
        )

    async def _on_document(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._check_access(update, action="document"):
            return
        if not update.effective_message or not update.effective_chat:
            return

        chat_id = update.effective_chat.id
        state = self._pending_actions.get(chat_id, {})
        mode = state.get("mode", "")
        if mode != "file_upload_wait":
            await self._send_message(
                context,
                chat_id,
                "ℹ️ Сначала выберите действие <b>Загрузить на ПК</b> в меню <b>Файлы</b>.",
            )
            return

        directory = Path(state.get("payload", "")).expanduser()
        if not directory.exists() or not directory.is_dir():
            self._pending_actions.pop(chat_id, None)
            await self._send_message(context, chat_id, "⚠️ Папка назначения недоступна. Повторите операцию.")
            return

        doc = update.effective_message.document
        if doc is None:
            await self._send_message(context, chat_id, "⚠️ Документ не распознан.")
            return

        filename = doc.file_name or f"file_{doc.file_unique_id}.bin"
        target = directory / filename
        target = self._avoid_overwrite(target)

        try:
            tg_file = await context.bot.get_file(doc.file_id)
            await tg_file.download_to_drive(custom_path=str(target))
            self._pending_actions.pop(chat_id, None)
            self._audit_action(
                update,
                action="file_upload",
                status="ok",
                details=f"name={filename};path={target};size={doc.file_size or 0}",
            )
            await self._send_message(
                context,
                chat_id,
                f"✅ Файл сохранен:\n<code>{html.escape(str(target))}</code>",
                reply_markup=self._files_keyboard(),
            )
        except Exception as exc:
            self._audit_action(update, action="file_upload", status="error", details=str(exc))
            await self._send_message(context, chat_id, f"⚠️ Ошибка сохранения файла: {html.escape(str(exc))}")

    async def _send_screenshot(self, chat_id: int, context: ContextTypes.DEFAULT_TYPE, update: Update) -> None:
        try:
            with mss.mss() as sct:
                img = sct.grab(sct.monitors[0])
                png_bytes = mss.tools.to_png(img.rgb, img.size)
            await self._send_photo(
                context,
                chat_id=chat_id,
                photo=png_bytes,
                caption="📸 <b>Текущий экран</b>",
            )
            self._audit_action(update, action="screenshot", status="ok")
            self._log("[Команда] Скриншот отправлен.")
        except Exception as exc:
            self._audit_action(update, action="screenshot", status="error", details=str(exc))
            self._log(f"[Ошибка] Скриншот не получился: {exc}")
            await self._send_message(context, chat_id, f"⚠️ Не удалось сделать скриншот: {html.escape(str(exc))}")

    async def _handle_file_download(self, update: Update, context: ContextTypes.DEFAULT_TYPE, raw_path: str) -> None:
        if not update.effective_chat:
            return
        chat_id = update.effective_chat.id
        path = self._normalize_path(raw_path)
        try:
            if not path.exists() or not path.is_file():
                raise FileNotFoundError("Файл не найден")
            with path.open("rb") as handle:
                await context.bot.send_document(
                    chat_id=chat_id,
                    document=handle,
                    caption=f"⬇️ <b>{html.escape(path.name)}</b>",
                    parse_mode=ParseMode.HTML,
                )
            self._audit_action(update, action="file_download", status="ok", details=f"path={path};size={path.stat().st_size}")
            await self._send_message(context, chat_id, "✅ Файл отправлен.", reply_markup=self._files_keyboard())
        except Exception as exc:
            self._audit_action(update, action="file_download", status="error", details=f"path={path};err={exc}")
            await self._send_message(context, chat_id, f"⚠️ Ошибка отправки: {html.escape(str(exc))}")

    def _open_link(self, raw: str) -> tuple[bool, str]:
        url = raw.strip()
        if not url:
            return False, "Пустая ссылка."

        parsed = urllib.parse.urlparse(url)
        if not parsed.scheme:
            url = "https://" + url
            parsed = urllib.parse.urlparse(url)

        if parsed.scheme not in {"http", "https"}:
            return False, "Разрешены только http/https."
        if not parsed.netloc:
            return False, "Неверный формат URL."

        webbrowser.open(url, new=2)
        return True, url

    def _shutdown_pc(self, reboot: bool) -> None:
        system = platform.system().lower()
        if system == "windows":
            command = ["shutdown", "/r", "/t", "0"] if reboot else ["shutdown", "/s", "/t", "0"]
        elif system in {"linux", "darwin"}:
            command = ["shutdown", "-r", "now"] if reboot else ["shutdown", "-h", "now"]
        else:
            raise RuntimeError(f"ОС не поддерживается: {system}")

        subprocess.Popen(command)

    def _logout_user(self) -> None:
        system = platform.system().lower()
        if system == "windows":
            subprocess.Popen(["shutdown", "/l"])
            return
        raise RuntimeError("Выход из учетной записи поддерживается только на Windows.")

    def _lock_screen(self) -> None:
        system = platform.system().lower()
        if system == "windows":
            result = ctypes.windll.user32.LockWorkStation()  # type: ignore[attr-defined]
            if result == 0:
                raise RuntimeError("LockWorkStation вернул ошибку")
            return
        raise RuntimeError("Блокировка экрана поддерживается только на Windows.")

    def _show_message_async(self, message: str) -> None:
        threading.Thread(target=self._show_message_blocking, args=(message,), daemon=True).start()

    def _show_message_blocking(self, message: str) -> None:
        text = message.strip() or "Пустое сообщение"
        if platform.system().lower() == "windows":
            ctypes.windll.user32.MessageBoxW(0, text, "Сообщение от Telegram-бота", 0x40)
            return
        subprocess.Popen(["echo", text])

    def _format_stats(self) -> str:
        cpu = psutil.cpu_percent(interval=0.5)
        memory = psutil.virtual_memory()
        disk_root = "C:\\" if platform.system().lower() == "windows" else "/"
        disk = psutil.disk_usage(disk_root)
        boot_time = psutil.boot_time()
        uptime = int(time.time() - boot_time)
        net = psutil.net_io_counters()

        temps = ""
        try:
            all_temps = psutil.sensors_temperatures()
            if all_temps:
                values: list[str] = []
                for entries in all_temps.values():
                    for item in entries[:3]:
                        if item.current is not None:
                            values.append(f"{item.current:.1f}C")
                if values:
                    temps = f"\n🌡 Температуры: <code>{html.escape(', '.join(values[:6]))}</code>"
        except Exception:
            temps = ""

        return (
            "<b>📊 Статистика компьютера</b>\n"
            f"🖥 Хост: <code>{html.escape(socket.gethostname())}</code>\n"
            f"💻 Система: <code>{html.escape(platform.system())} {html.escape(platform.release())}</code>\n"
            f"⏱ Аптайм: <b>{html.escape(self._human_uptime(uptime))}</b>\n"
            f"🔥 CPU: <b>{cpu:.1f}%</b>\n"
            f"🧠 RAM: <b>{memory.percent:.1f}%</b> ({self._fmt_bytes(memory.used)} / {self._fmt_bytes(memory.total)})\n"
            f"💾 Диск {html.escape(disk_root)}: <b>{disk.percent:.1f}%</b> ({self._fmt_bytes(disk.used)} / {self._fmt_bytes(disk.total)})\n"
            f"🌐 Сеть: ⬆️ {self._fmt_bytes(net.bytes_sent)}, ⬇️ {self._fmt_bytes(net.bytes_recv)}"
            f"{temps}"
        )

    def _format_processes(self) -> str:
        rows: list[tuple[int, str, float, int]] = []
        for proc in psutil.process_iter(attrs=["pid", "name", "cpu_percent", "memory_info"]):
            try:
                info = proc.info
                pid = int(info.get("pid") or 0)
                name = str(info.get("name") or "unknown")
                cpu = float(info.get("cpu_percent") or 0.0)
                mem = int(getattr(info.get("memory_info"), "rss", 0) or 0)
                rows.append((pid, name, cpu, mem))
            except Exception:
                continue

        rows.sort(key=lambda item: item[3], reverse=True)
        lines = ["<b>🧠 Топ процессов по памяти</b>"]
        for pid, name, cpu, mem in rows[:15]:
            lines.append(f"• <code>{pid:>5}</code> {html.escape(name[:24])} | CPU {cpu:.0f}% | RAM {self._fmt_bytes(mem)}")
        return "\n".join(lines)

    def _kill_process(self, pid: int) -> str:
        proc = psutil.Process(pid)
        name = proc.name()
        proc.terminate()
        try:
            proc.wait(timeout=4)
        except psutil.TimeoutExpired:
            proc.kill()
        return name

    def _start_process(self, command: str) -> None:
        cmd = command.strip()
        if not cmd:
            raise ValueError("Пустая команда")
        subprocess.Popen(cmd, shell=True)

    def _ensure_windows(self) -> None:
        if platform.system().lower() != "windows":
            raise RuntimeError("Эта операция поддерживается только на Windows.")

    def _format_services(self) -> str:
        self._ensure_windows()
        items: list[tuple[str, str, str]] = []
        for service in psutil.win_service_iter():
            try:
                info = service.as_dict()
                items.append((str(info.get("name") or ""), str(info.get("status") or ""), str(info.get("display_name") or "")))
            except Exception:
                continue

        items.sort(key=lambda row: (row[1] != "running", row[0]))
        lines = ["<b>🛠 Службы Windows (первые 25)</b>"]
        for name, status, display_name in items[:25]:
            icon = "🟢" if status == "running" else "⚪"
            label = display_name or name
            lines.append(f"{icon} <code>{html.escape(name)}</code> - {html.escape(label)}")
        return "\n".join(lines)

    def _service_control(self, service_name: str, start: bool) -> str:
        self._ensure_windows()
        name = service_name.strip()
        if not name:
            raise ValueError("Не указано имя службы")

        action = "start" if start else "stop"
        result = subprocess.run(
            ["sc", action, name],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            timeout=20,
        )
        output = (result.stdout or "") + "\n" + (result.stderr or "")
        if result.returncode != 0:
            raise RuntimeError(output.strip() or f"Команда sc {action} завершилась ошибкой")
        return output.strip()

    def _format_startup_entries(self) -> str:
        self._ensure_windows()
        import winreg

        entries: list[tuple[str, str]] = []
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, autostart.RUN_REG_PATH, 0, winreg.KEY_READ) as key:
            index = 0
            while True:
                try:
                    name, value, _ = winreg.EnumValue(key, index)
                except OSError:
                    break
                entries.append((str(name), str(value)))
                index += 1

        lines = ["<b>🚀 HKCU автозагрузка</b>"]
        if not entries:
            lines.append("(пусто)")
        for name, value in entries[:20]:
            lines.append(f"• <code>{html.escape(name)}</code> -> <code>{html.escape(value[:120])}</code>")
        return "\n".join(lines)

    def _startup_add(self, name: str, command: str) -> None:
        self._ensure_windows()
        import winreg

        key_name = name.strip()
        key_value = command.strip()
        if not key_name or not key_value:
            raise ValueError("Название и команда обязательны")

        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, autostart.RUN_REG_PATH, 0, winreg.KEY_SET_VALUE) as key:
            winreg.SetValueEx(key, key_name, 0, winreg.REG_SZ, key_value)

    def _startup_remove(self, name: str) -> None:
        self._ensure_windows()
        import winreg

        key_name = name.strip()
        if not key_name:
            raise ValueError("Не указано название записи")

        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, autostart.RUN_REG_PATH, 0, winreg.KEY_SET_VALUE) as key:
            try:
                winreg.DeleteValue(key, key_name)
            except FileNotFoundError:
                raise FileNotFoundError("Запись автозагрузки не найдена")

    def _get_system_volume(self) -> int:
        self._ensure_windows()
        current = ctypes.c_uint()
        result = ctypes.windll.winmm.waveOutGetVolume(0, ctypes.byref(current))  # type: ignore[attr-defined]
        if result != 0:
            raise RuntimeError(f"waveOutGetVolume вернул код {result}")

        left = current.value & 0xFFFF
        right = (current.value >> 16) & 0xFFFF
        avg = int(round(((left + right) / 2) * 100 / 65535))
        return max(0, min(100, avg))

    def _set_system_volume(self, percent: int) -> None:
        self._ensure_windows()
        value = max(0, min(100, int(percent)))
        v16 = int(value * 65535 / 100)
        packed = v16 | (v16 << 16)
        result = ctypes.windll.winmm.waveOutSetVolume(0, packed)  # type: ignore[attr-defined]
        if result != 0:
            raise RuntimeError(f"waveOutSetVolume вернул код {result}")

    def _mute_system_volume(self) -> None:
        current = self._get_system_volume()
        if current > 0:
            self._volume_before_mute = current
        self._set_system_volume(0)

    def _unmute_system_volume(self) -> None:
        restore = self._volume_before_mute if self._volume_before_mute is not None else 40
        self._set_system_volume(restore)

    def _get_clipboard_text(self) -> str:
        self._ensure_windows()
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command", "Get-Clipboard -Raw"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            timeout=10,
        )
        if result.returncode != 0:
            raise RuntimeError((result.stderr or "Не удалось получить буфер обмена").strip())
        return result.stdout or ""

    def _set_clipboard_text(self, text: str) -> None:
        self._ensure_windows()
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command", "Set-Clipboard -Value ([Console]::In.ReadToEnd())"],
            input=text,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            timeout=10,
        )
        if result.returncode != 0:
            raise RuntimeError((result.stderr or "Не удалось установить буфер обмена").strip())

    def _wake_on_lan_from_text(self, text: str) -> str:
        parts = text.replace("|", " ").split()
        if not parts:
            raise ValueError("Укажите MAC-адрес")

        mac = parts[0]
        broadcast = parts[1] if len(parts) > 1 else "255.255.255.255"
        port = int(parts[2]) if len(parts) > 2 else 9
        self._wake_on_lan(mac, broadcast, port)
        return f"mac={mac};broadcast={broadcast};port={port}"

    def _wake_on_lan(self, mac: str, broadcast: str, port: int) -> None:
        clean_mac = mac.replace(":", "").replace("-", "").replace(".", "").strip()
        if len(clean_mac) != 12 or any(ch not in hexdigits for ch in clean_mac):
            raise ValueError("Неверный MAC-адрес")

        if port <= 0 or port > 65535:
            raise ValueError("Неверный порт")

        payload = bytes.fromhex("FF" * 6 + clean_mac * 16)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.sendto(payload, (broadcast, port))

    def _create_scheduled_task(self, payload: str, update: Update) -> ScheduledTask:
        parts = [chunk.strip() for chunk in payload.split("|")]
        if len(parts) < 2:
            raise ValueError("Формат: YYYY-MM-DD HH:MM | команда | зачем (опционально)")

        dt_raw = parts[0]
        command = parts[1]
        reason = parts[2] if len(parts) > 2 else ""
        if not command:
            raise ValueError("Команда не может быть пустой")

        local_tz = datetime.now().astimezone().tzinfo
        if local_tz is None:
            local_tz = timezone.utc

        run_local = datetime.strptime(dt_raw, "%Y-%m-%d %H:%M").replace(tzinfo=local_tz)
        run_utc = run_local.astimezone(timezone.utc)
        if run_utc <= datetime.now(timezone.utc):
            raise ValueError("Время задачи должно быть в будущем")

        user = update.effective_user
        created_by = str(user.id if user else "0")
        if user and user.username:
            created_by = f"{created_by}@{user.username}"

        task_id = secrets.token_hex(3)
        return ScheduledTask(
            task_id=task_id,
            when_iso=run_utc.isoformat(),
            command=command,
            created_by=created_by,
            reason=reason,
        )

    def _format_tasks(self) -> str:
        lines = ["<b>⏰ Планировщик задач</b>"]
        lines.append(f"Мониторинг: <b>{'ON' if self.config.monitor_enabled else 'OFF'}</b>")
        lines.append(
            f"Пороги: temp>{self.config.temperature_alert_c:.1f}C, disk<{self.config.disk_free_alert_gb:.1f}GB, "
            f"cooldown={self.config.alert_cooldown_sec}s"
        )

        if not self._scheduled_tasks:
            lines.append("\nЗадач пока нет.")
            return "\n".join(lines)

        lines.append("\n<b>Запланировано:</b>")
        tasks_sorted = sorted(
            self._scheduled_tasks.values(),
            key=lambda task: task.when_utc() or datetime.max.replace(tzinfo=timezone.utc),
        )
        for task in tasks_sorted[:15]:
            when_utc = task.when_utc()
            when_text = when_utc.astimezone().strftime("%Y-%m-%d %H:%M") if when_utc else task.when_iso
            reason = f" | why: {task.reason[:40]}" if task.reason else ""
            lines.append(
                f"• <code>{task.task_id}</code> at <b>{html.escape(when_text)}</b> -> "
                f"<code>{html.escape(task.command[:70])}</code>{html.escape(reason)}"
            )
        return "\n".join(lines)

    def _persist_scheduled_tasks(self) -> None:
        payload: list[dict[str, str]] = []
        for task in self._scheduled_tasks.values():
            payload.append(
                {
                    "id": task.task_id,
                    "when_iso": task.when_iso,
                    "command": task.command,
                    "created_by": task.created_by,
                    "reason": task.reason,
                }
            )
        payload.sort(key=lambda row: row.get("when_iso", ""))
        self.config.scheduled_tasks = payload
        self._save_config()

    async def _scheduler_loop(self, bot: Bot) -> None:
        while not self._stop_event.is_set():
            now = datetime.now(timezone.utc)
            due: list[ScheduledTask] = []
            for task in list(self._scheduled_tasks.values()):
                run_at = task.when_utc()
                if run_at is None:
                    continue
                if run_at <= now:
                    due.append(task)

            for task in due:
                details = f"task={task.task_id};cmd={task.command[:160]};why={task.reason[:120]}"
                try:
                    subprocess.Popen(task.command, shell=True)
                    self._audit.append(user_id=0, username="scheduler", action="scheduled_task_run", status="ok", details=details)
                    await self._send_owner_message(
                        bot,
                        "✅ Выполнена задача\n"
                        f"ID: <code>{task.task_id}</code>\n"
                        f"Команда: <code>{html.escape(task.command[:180])}</code>",
                    )
                except Exception as exc:
                    self._audit.append(
                        user_id=0,
                        username="scheduler",
                        action="scheduled_task_run",
                        status="error",
                        details=f"{details};err={exc}",
                    )
                    await self._send_owner_message(
                        bot,
                        "⚠️ Ошибка выполнения задачи\n"
                        f"ID: <code>{task.task_id}</code>\n"
                        f"Ошибка: <code>{html.escape(str(exc))}</code>",
                    )
                self._scheduled_tasks.pop(task.task_id, None)
                self._persist_scheduled_tasks()

            await asyncio.sleep(2.0)

    async def _monitor_loop(self, bot: Bot) -> None:
        while not self._stop_event.is_set():
            try:
                if self.config.monitor_enabled:
                    await self._check_monitor_alerts(bot, force_send=False)
            except Exception as exc:
                self._log(f"[Мониторинг] Ошибка цикла мониторинга: {exc}")
            await asyncio.sleep(60)

    async def _check_monitor_alerts(self, bot: Bot, force_send: bool) -> int:
        sent = 0

        internet_ok = self._check_internet_available(self.config.internet_check_host, self.config.internet_check_port)
        if await self._update_alert_state(
            bot,
            key="internet_down",
            active=not internet_ok,
            message=(
                "🌐🔴 Потеряно соединение с интернетом.\n"
                f"Проверка узла: <code>{html.escape(self.config.internet_check_host)}:{self.config.internet_check_port}</code>"
            ),
            force_send=force_send,
        ):
            sent += 1

        disk_root = "C:\\" if platform.system().lower() == "windows" else "/"
        disk = psutil.disk_usage(disk_root)
        disk_free_gb = disk.free / (1024**3)
        if await self._update_alert_state(
            bot,
            key="disk_low",
            active=disk_free_gb < float(self.config.disk_free_alert_gb),
            message=(
                "💾🟠 Мало места на диске.\n"
                f"Свободно: <b>{disk_free_gb:.2f} GB</b>, порог: <b>{self.config.disk_free_alert_gb:.2f} GB</b>"
            ),
            force_send=force_send,
        ):
            sent += 1

        max_temp = self._get_max_temperature_c()
        if max_temp is not None:
            if await self._update_alert_state(
                bot,
                key="temp_high",
                active=max_temp > float(self.config.temperature_alert_c),
                message=(
                    "🌡🔴 Высокая температура.\n"
                    f"Сейчас: <b>{max_temp:.1f}C</b>, порог: <b>{self.config.temperature_alert_c:.1f}C</b>"
                ),
                force_send=force_send,
            ):
                sent += 1
        else:
            self._alert_state["temp_high"] = False

        return sent

    async def _update_alert_state(self, bot: Bot, key: str, active: bool, message: str, force_send: bool) -> bool:
        prev_state = self._alert_state.get(key, False)
        self._alert_state[key] = active
        if not active:
            return False

        now = time.time()
        cooldown = max(60, int(self.config.alert_cooldown_sec))
        last = self._last_alert_sent_at.get(key, 0.0)
        need_send = force_send or (not prev_state) or (now - last >= cooldown)
        if not need_send:
            return False

        self._last_alert_sent_at[key] = now
        await self._send_owner_message(bot, message)
        self._audit.append(user_id=0, username="monitor", action=f"alert:{key}", status="ok", details=message)
        return True

    async def _send_owner_message(self, bot: Bot, text: str) -> None:
        if self.owner_user_id is None:
            return
        try:
            await self._send_message_with_bot(bot, self.owner_user_id, text)
        except Exception as exc:
            self._log(f"[Оповещения] Не удалось отправить владельцу: {exc}")

    def _check_internet_available(self, host: str, port: int) -> bool:
        try:
            with socket.create_connection((host, int(port)), timeout=4):
                return True
        except Exception:
            return False

    def _get_max_temperature_c(self) -> float | None:
        try:
            data = psutil.sensors_temperatures()
        except Exception:
            return None

        values: list[float] = []
        for entries in data.values():
            for item in entries:
                if item.current is not None:
                    values.append(float(item.current))
        if not values:
            return None
        return max(values)

    def _save_config(self) -> None:
        if self._persist_config:
            self._persist_config(self.config)
        else:
            save_config(self.config)

    def _mode_commands(self, mode_name: str) -> list[str]:
        mode = mode_name.strip().lower()
        if mode == "sleep":
            return [line.strip() for line in self.config.sleep_mode_commands if str(line).strip()]
        if mode == "work":
            return [line.strip() for line in self.config.work_mode_commands if str(line).strip()]
        return []

    def _sync_custom_scripts_from_config(self) -> None:
        try:
            latest = load_config()
        except Exception:
            latest = self.config
        self._custom_scripts = self._normalize_custom_scripts(getattr(latest, "custom_scripts", []))
        self.config.custom_scripts = list(self._custom_scripts)

    def _sync_scripts(self) -> None:
        self._sync_custom_scripts_from_config()
        ensure_scripts_dir(self._scripts_dir)
        file_scripts, warnings = load_scripts_from_directory(self._scripts_dir)
        legacy_scripts = convert_legacy_custom_scripts(self._custom_scripts)
        self._script_items = (file_scripts + legacy_scripts)[:64]
        self._script_warnings = warnings

    def _normalize_custom_scripts(self, value: object) -> list[dict[str, object]]:
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
            commands = [str(cmd).strip() for cmd in commands_source if str(cmd).strip()]
            if not script_id or not name or not commands:
                continue
            scripts.append(
                {
                    "id": script_id,
                    "name": name,
                    "description": description,
                    "commands": commands,
                }
            )
        return scripts

    def _format_scripts_overview(self) -> str:
        if not self._script_items:
            scripts_dir = html.escape(str(self._scripts_dir.resolve()))
            return (
                "<b>📜 Скрипты</b>\n"
                "Сценарии не найдены.\n"
                f"Добавьте JSON-файлы в <code>{scripts_dir}</code>."
            )

        lines = ["<b>📜 Скрипты</b>"]
        for item in self._script_items[:20]:
            script_id = html.escape(str(item.get("id", "")))
            name = html.escape(str(item.get("name", "Script")))
            description = html.escape(str(item.get("description", "")))
            buttons = item.get("buttons", [])
            count = len(buttons) if isinstance(buttons, list) else 0
            source = html.escape(str(item.get("source", "")))
            if description:
                lines.append(f"• <b>{name}</b> ({count} btn)\n<code>{script_id}</code>\n<i>{description}</i>")
            else:
                lines.append(f"• <b>{name}</b> ({count} btn)\n<code>{script_id}</code>")
            if source:
                lines.append(f"  ↳ <code>{source}</code>")
        if self._script_warnings:
            lines.append(f"\n⚠️ Пропущено файлов с ошибками: <b>{len(self._script_warnings)}</b>")
        return "\n".join(lines)

    def _find_script(self, script_id: str) -> dict[str, object] | None:
        target = script_id.strip()
        for item in self._script_items:
            if str(item.get("id", "")).strip() == target:
                return item
        return None

    def _find_script_button(self, script: dict[str, object], button_id: str) -> dict[str, object] | None:
        buttons = script.get("buttons", [])
        if not isinstance(buttons, list):
            return None
        target = button_id.strip()
        for item in buttons:
            if not isinstance(item, dict):
                continue
            if str(item.get("id", "")).strip() == target:
                return item
        return None

    def _format_script_details(self, script: dict[str, object]) -> str:
        name = html.escape(str(script.get("name", "Script")))
        description = html.escape(str(script.get("description", "")))
        source = html.escape(str(script.get("source", "")))
        buttons = script.get("buttons", [])
        count = len(buttons) if isinstance(buttons, list) else 0
        lines = [f"<b>🧩 {name}</b>", f"Кнопок: <b>{count}</b>"]
        if description:
            lines.append(f"<i>{description}</i>")
        if source:
            lines.append(f"Источник: <code>{source}</code>")
        return "\n".join(lines)

    async def _open_script(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        chat_id: int,
        script_id: str,
    ) -> None:
        script = self._find_script(script_id)
        if not script:
            self._audit_action(update, action="script_open", status="denied", details=f"id={script_id};not_found")
            await self._send_message(context, chat_id, "⚠️ Скрипт не найден.")
            return

        self._audit_action(update, action="script_open", status="ok", details=f"id={script_id}")
        await self._send_message(
            context,
            chat_id,
            self._format_script_details(script),
            reply_markup=self._script_detail_keyboard(script),
        )

    async def _run_script_button(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        chat_id: int,
        script_id: str,
        button_id: str,
    ) -> None:
        script = self._find_script(script_id)
        if not script:
            self._audit_action(update, action="script_button", status="denied", details=f"id={script_id};not_found")
            await self._send_message(context, chat_id, "⚠️ Скрипт не найден.")
            return

        button = self._find_script_button(script, button_id)
        if not button:
            self._audit_action(
                update,
                action="script_button",
                status="denied",
                details=f"id={script_id};button={button_id};not_found",
            )
            await self._send_message(context, chat_id, "⚠️ Кнопка скрипта не найдена.")
            return

        script_name = str(script.get("name", "Script")).strip()
        button_name = str(button.get("text", "Button")).strip()
        action_payload = button.get("action", {})
        action_type = ""
        if isinstance(action_payload, dict):
            action_type = str(action_payload.get("type", "")).strip()
        base_details = (
            f"script={script_id};button={button_id};script_name={script_name[:60]};"
            f"button_name={button_name[:60]};type={action_type}"
        )

        try:
            status, text, details = await self._execute_script_action(script, button)
        except Exception as exc:
            status = "error"
            details = f"{base_details};err={str(exc)[:200]}"
            text = f"⛔ Ошибка кнопки скрипта: <code>{html.escape(str(exc))}</code>"
        else:
            details = f"{base_details};{details}"

        self._audit_action(update, action="script_button", status=status, details=details)
        await self._send_message(context, chat_id, text, reply_markup=self._script_detail_keyboard(script))

    async def _execute_script_action(
        self,
        script: dict[str, object],
        button: dict[str, object],
    ) -> tuple[str, str, str]:
        action = button.get("action", {})
        if not isinstance(action, dict):
            raise ValueError("action is missing")
        action_type = str(action.get("type", "")).strip().lower()
        if not action_type:
            raise ValueError("action.type is empty")

        if action_type == "command":
            command = str(action.get("command", "")).strip()
            timeout_sec = int(action.get("timeout_sec", 90))
            return self._run_shell_commands_with_report(
                commands=[command],
                timeout_sec=timeout_sec,
                stop_on_error=True,
                header=str(button.get("text", "Command")),
            )

        if action_type == "commands":
            commands = action.get("commands", [])
            if isinstance(commands, str):
                commands = commands.splitlines()
            if not isinstance(commands, list):
                commands = []
            timeout_sec = int(action.get("timeout_sec", 90))
            stop_on_error = bool(action.get("stop_on_error", False))
            normalized = [str(item).strip() for item in commands if str(item).strip()]
            return self._run_shell_commands_with_report(
                commands=normalized,
                timeout_sec=timeout_sec,
                stop_on_error=stop_on_error,
                header=str(button.get("text", "Commands")),
            )

        if action_type == "open_url":
            ok, value = self._open_link(str(action.get("url", "")).strip())
            if not ok:
                raise ValueError(value)
            text = f"✅ Открыто: <code>{html.escape(value)}</code>"
            return "ok", text, f"type=open_url;url={value[:200]}"

        if action_type == "message":
            value = str(action.get("text", "")).strip()
            if not value:
                raise ValueError("message text is empty")
            text = f"💬 {html.escape(value)}"
            return "ok", text, f"type=message;len={len(value)}"

        if action_type == "mode":
            mode = str(action.get("mode", "")).strip().lower()
            commands = self._mode_commands(mode)
            if not commands:
                raise ValueError(f"mode '{mode}' has no commands")
            return self._run_shell_commands_with_report(
                commands=commands,
                timeout_sec=90,
                stop_on_error=False,
                header=f"Mode {mode}",
            )

        if action_type == "volume_set":
            percent = int(action.get("percent", 40))
            self._set_system_volume(percent)
            return "ok", f"✅ Громкость: <b>{percent}%</b>", f"type=volume_set;percent={percent}"

        if action_type == "volume_mute":
            self._mute_system_volume()
            return "ok", "✅ Звук выключен (mute).", "type=volume_mute"

        if action_type == "volume_unmute":
            self._unmute_system_volume()
            current = self._get_system_volume()
            return "ok", f"✅ Звук включен. Текущий уровень: <b>{current}%</b>", f"type=volume_unmute;current={current}"

        if action_type == "clipboard_set":
            value = str(action.get("text", ""))
            if not value.strip():
                raise ValueError("clipboard text is empty")
            self._set_clipboard_text(value)
            return "ok", "✅ Текст вставлен в буфер обмена.", f"type=clipboard_set;len={len(value)}"

        if action_type == "wake_on_lan":
            mac = str(action.get("mac", "")).strip()
            broadcast = str(action.get("broadcast", "255.255.255.255")).strip() or "255.255.255.255"
            port = int(action.get("port", 9))
            self._wake_on_lan(mac, broadcast, port)
            return "ok", f"✅ WoL пакет отправлен: <code>{html.escape(mac)}</code>", (
                f"type=wake_on_lan;mac={mac};broadcast={broadcast};port={port}"
            )

        if action_type == "lock_screen":
            self._lock_screen()
            return "ok", "✅ Экран заблокирован.", "type=lock_screen"

        if action_type == "logout":
            self._logout_user()
            return "ok", "✅ Выполнен выход из учетной записи.", "type=logout"

        if action_type == "shutdown":
            self._shutdown_pc(reboot=False)
            return "ok", "✅ Отправлена команда выключения.", "type=shutdown"

        if action_type == "reboot":
            self._shutdown_pc(reboot=True)
            return "ok", "✅ Отправлена команда перезагрузки.", "type=reboot"

        raise ValueError(f"unsupported action type: {action_type}")

    def _run_shell_commands_with_report(
        self,
        commands: list[str],
        timeout_sec: int,
        stop_on_error: bool,
        header: str,
    ) -> tuple[str, str, str]:
        filtered = [str(item).strip() for item in commands if str(item).strip()]
        if not filtered:
            raise ValueError("no commands to run")

        ok_count = 0
        failures: list[str] = []
        timeout = min(max(int(timeout_sec), 1), 600)
        for index, command in enumerate(filtered, start=1):
            try:
                result = subprocess.run(
                    command,
                    shell=True,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="ignore",
                    timeout=timeout,
                )
            except Exception as exc:
                failures.append(f"{index}:exception:{str(exc)[:120]}")
                if stop_on_error:
                    break
                continue

            if result.returncode == 0:
                ok_count += 1
                continue

            stderr = (result.stderr or result.stdout or "").strip().replace("\n", " ")
            failures.append(f"{index}:rc={result.returncode}:{stderr[:120]}")
            if stop_on_error:
                break

        total = len(filtered)
        details = f"type=shell;ok={ok_count};total={total};timeout={timeout}"
        if failures:
            details += ";errors=" + " | ".join(failures[:5])

        title = html.escape(header[:64] or "Script")
        if not failures:
            return "ok", f"✅ <b>{title}</b>: выполнено шагов <b>{ok_count}</b>.", details
        if ok_count > 0:
            text = (
                f"⚠️ <b>{title}</b>: частичное выполнение.\n"
                f"Успешно: <b>{ok_count}</b> из <b>{total}</b>.\n"
                f"<code>{html.escape(' | '.join(failures[:3]))}</code>"
            )
            return "partial", text, details

        text = (
            f"⛔ <b>{title}</b>: выполнение не удалось.\n"
            f"<code>{html.escape(' | '.join(failures[:3]))}</code>"
        )
        return "error", text, details

    def _parse_pair(self, text: str) -> tuple[str, str]:
        if "|" not in text:
            raise ValueError("Ожидается формат: поле1 | поле2")
        left, right = text.split("|", 1)
        a = left.strip()
        b = right.strip()
        if not a or not b:
            raise ValueError("Оба поля обязательны")
        return a, b

    def _normalize_path(self, raw: str) -> Path:
        value = raw.strip().strip('"').strip("'")
        if not value:
            raise ValueError("Путь пустой")
        return Path(value).expanduser().resolve()

    def _avoid_overwrite(self, path: Path) -> Path:
        if not path.exists():
            return path
        stem = path.stem
        suffix = path.suffix
        parent = path.parent
        for index in range(1, 1000):
            candidate = parent / f"{stem}_{index}{suffix}"
            if not candidate.exists():
                return candidate
        raise RuntimeError("Не удалось подобрать уникальное имя файла")

    @staticmethod
    def _extract_pin(text: str) -> str:
        value = text.strip()
        if value.lower().startswith("pin"):
            parts = value.split(maxsplit=1)
            if len(parts) == 2:
                return parts[1].strip()
        return value

    @staticmethod
    def _short_ts(value: str) -> str:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed.strftime("%H:%M:%S")
        except Exception:
            return value[-8:] if len(value) >= 8 else value

    @staticmethod
    def _human_uptime(seconds: int) -> str:
        days, rem = divmod(seconds, 86400)
        hours, rem = divmod(rem, 3600)
        minutes, _ = divmod(rem, 60)
        return f"{days}д {hours}ч {minutes}м"

    @staticmethod
    def _fmt_bytes(value: int) -> str:
        num = float(value)
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if num < 1024 or unit == "TB":
                return f"{num:.1f} {unit}"
            num /= 1024
        return f"{value} B"

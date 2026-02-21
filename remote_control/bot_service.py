from __future__ import annotations

import asyncio
import ctypes
import html
import platform
import socket
import subprocess
import threading
import time
import urllib.parse
import webbrowser
from datetime import datetime
from typing import Callable

import mss
import mss.tools
import psutil
from telegram import CopyTextButton, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import TelegramError
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

from remote_control.audit import AuditStore
from remote_control.config import AppConfig, save_config


LogFn = Callable[[str], None]
PersistConfigFn = Callable[[AppConfig], None]


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
            raise ValueError("Для первой привязки добавьте хотя бы один разрешенный username.")

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
        application.add_handler(CommandHandler("cancel", self._on_cancel))
        application.add_handler(CallbackQueryHandler(self._on_button))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self._on_text))

        await application.initialize()
        await application.start()
        if application.updater is None:
            raise RuntimeError("Updater не инициализирован.")
        await application.updater.start_polling(drop_pending_updates=True)
        self._log("Бот запущен и ожидает команды.")

        try:
            while not self._stop_event.is_set():
                await asyncio.sleep(0.3)
        finally:
            await application.updater.stop()
            await application.stop()
            await application.shutdown()

    def _main_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("🟢 📸 Скриншот", callback_data="screenshot"),
                    InlineKeyboardButton("🔵 📊 Статистика", callback_data="stats"),
                ],
                [
                    InlineKeyboardButton("🟡 📝 Сообщение", callback_data="prompt_message"),
                    InlineKeyboardButton("🟣 🌐 Открыть ссылку", callback_data="prompt_link"),
                ],
                [
                    InlineKeyboardButton("⚪ 📜 История", callback_data="history"),
                    InlineKeyboardButton("🧭 🆔 Мой ID", callback_data="show_id"),
                ],
                [
                    InlineKeyboardButton("🟠 🔄 Перезагрузка", callback_data="confirm_reboot"),
                    InlineKeyboardButton("🔴 ⛔ Выключение", callback_data="confirm_shutdown"),
                ],
            ]
        )

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
            await context.bot.send_message(**kwargs)
        except TelegramError:
            if "message_effect_id" in kwargs:
                kwargs.pop("message_effect_id")
                await context.bot.send_message(**kwargs)
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

        if self.allowed_usernames and username not in self.allowed_usernames:
            await self._send_message(
                context,
                chat_id,
                "⛔ Ваш username не находится в списке первичной привязки.\n"
                "Добавьте username в приложение и попробуйте снова.",
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
            "Выберите действие ниже.\n\n"
            "<i>Подсказка:</i> для отмены ожидания ввода используйте /cancel"
        )
        await self._send_message(context, update.effective_chat.id, text, reply_markup=self._main_keyboard())

    async def _on_history(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._check_access(update, action="history"):
            return
        if not update.effective_chat:
            return

        records = self._audit.tail(limit=12)
        if not records:
            await self._send_message(context, update.effective_chat.id, "📜 История пока пустая.")
            return

        lines = ["<b>📜 Последние действия</b>"]
        for item in records:
            stamp = self._short_ts(item.ts)
            user = f"@{html.escape(item.username)}" if item.username else str(item.user_id)
            detail = f" ({html.escape(item.details)})" if item.details else ""
            lines.append(f"• <code>{stamp}</code> {html.escape(item.action)}: <b>{html.escape(item.status)}</b> by {user}{detail}")

        await self._send_message(context, update.effective_chat.id, "\n".join(lines))

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
                "🌐 Введите ссылку (только <code>http/https</code>).",
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

        if action == "cancel_pending":
            self._pending_actions.pop(chat_id, None)
            await self._send_message(context, chat_id, "❎ Действие отменено.", reply_markup=self._main_keyboard())
            return

    async def _on_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not await self._check_access(update, action="text"):
            return
        if not update.effective_message or not update.effective_chat:
            return

        chat_id = update.effective_chat.id
        state = self._pop_pending(chat_id)
        text = update.effective_message.text.strip()
        mode = state.get("mode", "")

        if mode == "message":
            self._show_message_async(text)
            self._audit_action(update, action="show_message", status="ok")
            self._log("[Команда] Сообщение на экран отправлено.")
            await self._send_message(context, chat_id, "✅ Сообщение показано на экране.", reply_markup=self._main_keyboard())
            return

        if mode == "link":
            ok, result = self._open_link(text)
            if ok:
                self._audit_action(update, action="open_link", status="ok", details=result)
                self._log(f"[Команда] Открыта ссылка: {result}")
                await self._send_message(context, chat_id, "✅ Ссылка открыта в браузере.", reply_markup=self._main_keyboard())
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
                self._log("[Команда] Выключение ПК.")
                try:
                    self._shutdown_pc(reboot=False)
                    self._audit_action(update, action="shutdown", status="ok")
                    await self._send_message(context, chat_id, "✅ Команда на выключение отправлена.")
                except Exception as exc:
                    self._audit_action(update, action="shutdown", status="error", details=str(exc))
                    await self._send_message(context, chat_id, f"⚠️ Ошибка выключения: {html.escape(str(exc))}")
                return

            if action == "reboot":
                self._log("[Команда] Перезагрузка ПК.")
                try:
                    self._shutdown_pc(reboot=True)
                    self._audit_action(update, action="reboot", status="ok")
                    await self._send_message(context, chat_id, "✅ Команда на перезагрузку отправлена.")
                except Exception as exc:
                    self._audit_action(update, action="reboot", status="error", details=str(exc))
                    await self._send_message(context, chat_id, f"⚠️ Ошибка перезагрузки: {html.escape(str(exc))}")
                return

        await self._send_message(
            context,
            chat_id,
            "ℹ️ Выберите действие через меню /start.",
            reply_markup=self._main_keyboard(),
        )

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
            await self._send_message(
                context,
                chat_id,
                f"⚠️ Не удалось сделать скриншот: {html.escape(str(exc))}",
            )

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

    def _show_message_async(self, message: str) -> None:
        threading.Thread(target=self._show_message_blocking, args=(message,), daemon=True).start()

    def _show_message_blocking(self, message: str) -> None:
        text = message.strip() or "Пустое сообщение"
        if platform.system().lower() == "windows":
            ctypes.windll.user32.MessageBoxW(0, text, "Сообщение от Telegram-бота", 0x40)
            return
        subprocess.Popen(["echo", text])

    def _format_stats(self) -> str:
        cpu = psutil.cpu_percent(interval=0.6)
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
                    for item in entries[:2]:
                        if item.current is not None:
                            values.append(f"{item.current:.1f}C")
                if values:
                    temps = f"\n🌡️ Температуры: <code>{html.escape(', '.join(values[:4]))}</code>"
        except Exception:
            temps = ""

        return (
            "<b>📊 Статистика компьютера</b>\n"
            f"🖥️ Хост: <code>{html.escape(socket.gethostname())}</code>\n"
            f"💻 Система: <code>{html.escape(platform.system())} {html.escape(platform.release())}</code>\n"
            f"⏱️ Аптайм: <b>{html.escape(self._human_uptime(uptime))}</b>\n"
            f"🔥 CPU: <b>{cpu:.1f}%</b>\n"
            f"🧠 RAM: <b>{memory.percent:.1f}%</b> ({self._fmt_bytes(memory.used)} / {self._fmt_bytes(memory.total)})\n"
            f"💾 Диск {html.escape(disk_root)}: <b>{disk.percent:.1f}%</b> ({self._fmt_bytes(disk.used)} / {self._fmt_bytes(disk.total)})\n"
            f"🌐 Сеть: ⬆️ {self._fmt_bytes(net.bytes_sent)}, ⬇️ {self._fmt_bytes(net.bytes_recv)}"
            f"{temps}"
        )

    def _save_config(self) -> None:
        if self._persist_config:
            self._persist_config(self.config)
        else:
            save_config(self.config)

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


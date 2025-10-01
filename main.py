"""Entrypoint for the Confetti Telegram bot.

This module now contains a functional scenario for the Confetti studio bot.
The implementation mirrors the structure that is used in production while
remaining small enough for the accompanying exercises.

As in the original project we attempt to instantiate
``AIORateLimiter`` inside :func:`ConfettiTelegramBot._build_rate_limiter`.  If
the optional extra is missing the class is still importable but its
constructor raises a :class:`RuntimeError`.  We catch the exception and log a
warning which allows the bot to start without the rate limiter.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import mimetypes
import os
import random
import re
from datetime import datetime, timedelta
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Optional, Union
from xml.sax.saxutils import escape


TELEGRAM_IMPORT_ERROR: ModuleNotFoundError | None = None

_TELEGRAM_DEPENDENCY_INSTRUCTIONS = (
    "python-telegram-bot is required to run this project. "
    "Install it with 'pip install \"python-telegram-bot[rate-limiter]\"'."
)


class _MissingTelegramModule:
    """Placeholder that raises a helpful error when used without telegram."""

    def __getattr__(self, name: str) -> Any:  # pragma: no cover - guard clause
        if TELEGRAM_IMPORT_ERROR is not None:
            raise RuntimeError(_TELEGRAM_DEPENDENCY_INSTRUCTIONS) from TELEGRAM_IMPORT_ERROR
        raise RuntimeError(_TELEGRAM_DEPENDENCY_INSTRUCTIONS)


try:  # pragma: no cover - optional dependency
    import gspread
    from google.oauth2.service_account import Credentials
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    gspread = None  # type: ignore[assignment]
    Credentials = None  # type: ignore[assignment]


if TYPE_CHECKING:
    from telegram import (
        InlineKeyboardButton,
        InlineKeyboardMarkup,
        KeyboardButton,
        ReplyKeyboardMarkup,
        ReplyKeyboardRemove,
        Update,
    )
    from telegram.error import InvalidToken as TelegramInvalidToken
    from telegram.error import NetworkError as TelegramNetworkError
    from telegram.error import TimedOut as TelegramTimedOut
    from telegram.ext import (
        AIORateLimiter as _AIORateLimiter,
        Application,
        ApplicationBuilder,
        CallbackQueryHandler,
        CommandHandler,
        ContextTypes,
        ConversationHandler,
        MessageHandler,
        filters,
    )
else:  # pragma: no cover - import depends on environment
    try:
        from telegram import (
            InlineKeyboardButton,
            InlineKeyboardMarkup,
            KeyboardButton,
            ReplyKeyboardMarkup,
            ReplyKeyboardRemove,
            Update,
        )
        from telegram.error import InvalidToken as TelegramInvalidToken
        from telegram.error import NetworkError as TelegramNetworkError
        from telegram.error import TimedOut as TelegramTimedOut
        from telegram.ext import (
            Application,
            ApplicationBuilder,
            CallbackQueryHandler,
            CommandHandler,
            ContextTypes,
            ConversationHandler,
            MessageHandler,
            filters,
        )
    except ModuleNotFoundError as exc:  # pragma: no cover - environment specific
        TELEGRAM_IMPORT_ERROR = exc
        InlineKeyboardButton = InlineKeyboardMarkup = KeyboardButton = ReplyKeyboardMarkup = ReplyKeyboardRemove = Update = object  # type: ignore[assignment]
        Application = ApplicationBuilder = CommandHandler = ConversationHandler = MessageHandler = object  # type: ignore[assignment]
        ContextTypes = object  # type: ignore[assignment]
        filters = _MissingTelegramModule()  # type: ignore[assignment]
        TelegramInvalidToken = TelegramNetworkError = TelegramTimedOut = RuntimeError  # type: ignore[assignment]
        _AIORateLimiter = None
    else:
        try:
            from telegram.ext import AIORateLimiter as _AIORateLimiter
        except ImportError:  # pragma: no cover - optional dependency
            _AIORateLimiter = None

AIORateLimiter = _AIORateLimiter


LOGGER = logging.getLogger(__name__)


ChatIdInput = Union[int, str]
AdminChatIdsInput = Union[ChatIdInput, Iterable[ChatIdInput], None]


def _require_telegram() -> None:
    """Ensure python-telegram-bot is installed before continuing."""

    if TELEGRAM_IMPORT_ERROR is not None:
        raise RuntimeError(_TELEGRAM_DEPENDENCY_INSTRUCTIONS) from TELEGRAM_IMPORT_ERROR


@dataclass
class MediaAttachment:
    """Representation of a media payload that can be resent later."""

    kind: str
    file_id: str
    caption: Optional[str] = None


@dataclass
class ContentBlock:
    """Rich content containing text and optional media attachments."""

    text: str = ""
    media: list[MediaAttachment] = field(default_factory=list)

    def copy(self) -> "ContentBlock":
        return ContentBlock(
            text=self.text,
            media=[MediaAttachment(kind=item.kind, file_id=item.file_id, caption=item.caption) for item in self.media],
        )


@dataclass
class BotContent:
    """Mutable content blocks that administrators can edit at runtime."""

    schedule: ContentBlock
    about: ContentBlock
    teachers: ContentBlock
    payment: ContentBlock
    album: ContentBlock
    contacts: ContentBlock
    vocabulary: list[dict[str, str]]

    @classmethod
    def default(cls) -> "BotContent":
        return cls(
            schedule=ContentBlock(
                text=(
                    "Наше актуальное расписание:\n\n"
                    "☀️ Утро: 10:00 – 12:00\n"
                    "🌤 День: 14:00 – 16:00\n"
                    "🌙 Вечер: 18:00 – 20:00"
                )
            ),
            about=ContentBlock(
                text=(
                    "О студии\n"
                    "Наша студия существует уже 8 лет и стала местом, где дети узнают красоту французского языка и культуры.\n"
                    "С нами работают только профессионалы: преподаватели с высшим профильным образованием и красными дипломами, обладатели международного сертификата DALF, педагоги со стажем более 10 лет и носители языка, которые делятся аутентичной атмосферой Франции.\n"
                    "Каждый год мы участвуем во франкофонных фестивалях по всей России — от Москвы и Санкт-Петербурга до Екатеринбурга и Валдая. Мы выступаем на площадках города, организуем любимые французские праздники и завершаем сезон ярким событием, которого ждут все наши ученики.\n"
                    "Наша цель проста и очень важна: 👉 чтобы дети полюбили французский язык ❤️\n\n"
                    "🎭 У нас «Конфетти» = это всегда праздник!"
                )
            ),
            teachers=ContentBlock(
                text=("Наши преподаватели — увлечённые и опытные педагоги. Выберите имя ниже, чтобы узнать подробнее.")
            ),
            payment=ContentBlock(
                text=(
                    "Пожалуйста, отправьте сюда фото или чек об оплате.\n\n"
                    "📌 После проверки мы подтвердим вашу запись."
                )
            ),
            album=ContentBlock(
                text=(
                    "Посмотрите наши лучшие моменты 🎭\n\n"
                    "👉 [Ссылка на альбом]"
                )
            ),
            contacts=ContentBlock(
                text=(
                    "📞 Телефон: +7 (900) 000-00-00\n"
                    "📧 Email: confetti@example.com\n"
                    "🌐 Сайт: confetti.ru\n"
                    "📲 Telegram: @ConfettiAdmin"
                )
            ),
            vocabulary=[
                {
                    "word": "Soleil",
                    "emoji": "☀️",
                    "translation": "Солнце",
                    "example_fr": "Le soleil brille.",
                    "example_ru": "Солнце светит.",
                },
                {
                    "word": "Bonjour",
                    "emoji": "👋",
                    "translation": "Здравствуйте",
                    "example_fr": "Bonjour, comment ça va ?",
                    "example_ru": "Здравствуйте, как дела?",
                },
                {
                    "word": "Amitié",
                    "emoji": "🤝",
                    "translation": "Дружба",
                    "example_fr": "L'amitié rend la vie plus douce.",
                    "example_ru": "Дружба делает жизнь добрее.",
                },
                {
                    "word": "Étoile",
                    "emoji": "✨",
                    "translation": "Звезда",
                    "example_fr": "Chaque étoile brille à sa manière.",
                    "example_ru": "Каждая звезда сияет по-своему.",
                },
            ],
        )

    def copy(self) -> "BotContent":
        return BotContent(
            schedule=self.schedule.copy(),
            about=self.about.copy(),
            teachers=self.teachers.copy(),
            payment=self.payment.copy(),
            album=self.album.copy(),
            contacts=self.contacts.copy(),
            vocabulary=[entry.copy() for entry in self.vocabulary],
        )


class SheetsBackendError(RuntimeError):
    """Raised when the cloud spreadsheet backend cannot be used."""


@dataclass
class GoogleSheetsBackend:
    """Thin wrapper around Google Sheets for storing registrations."""

    sheet_id: str
    credentials: Any
    worksheet_title: str = "Заявки"
    _client: Any | None = field(init=False, default=None, repr=False)
    _spreadsheet: Any | None = field(init=False, default=None, repr=False)
    _worksheet: Any | None = field(init=False, default=None, repr=False)
    _worksheet_id: Optional[int] = field(init=False, default=None, repr=False)
    _lock: asyncio.Lock = field(init=False, repr=False)

    HEADERS: tuple[str, ...] = (
        "ID",
        "Дата заявки",
        "Программа",
        "Участник",
        "Класс / возраст",
        "Телефон",
        "Предпочтительное время",
        "Комментарий оплаты",
        "Статус оплаты",
        "Фото оплаты",
        "Отправитель",
        "Чат",
    )

    COLUMN_WIDTHS: tuple[int, ...] = (
        140,
        220,
        320,
        280,
        200,
        200,
        220,
        260,
        200,
        280,
        220,
        240,
    )

    IMAGE_COLUMN_INDEX: int = HEADERS.index("Фото оплаты") + 1

    def __post_init__(self) -> None:
        if gspread is None or Credentials is None:  # pragma: no cover - depends on optional deps
            raise SheetsBackendError(
                "Библиотека gspread не установлена. Установите 'gspread' и 'google-auth'."
            )
        self._lock = asyncio.Lock()

    async def ensure_ready(self) -> None:
        async with self._lock:
            if self._worksheet is not None:
                return
            await asyncio.to_thread(self._initialise)

    async def append_registration(
        self,
        record: dict[str, Any],
        *,
        payment_status: str,
        image_value: Optional[str] = None,
    ) -> int:
        await self.ensure_ready()
        return await asyncio.to_thread(
            self._append_row_sync,
            record,
            payment_status,
            image_value,
        )

    async def delete_registration(self, registration_id: str) -> None:
        await self.ensure_ready()
        await asyncio.to_thread(self._delete_row_sync, registration_id)

    async def update_registration_image(
        self,
        registration_id: str,
        image_value: Optional[str],
    ) -> None:
        await self.ensure_ready()
        await asyncio.to_thread(self._update_image_sync, registration_id, image_value)

    async def link(self) -> str:
        await self.ensure_ready()
        gid = self._worksheet_id or 0
        return f"https://docs.google.com/spreadsheets/d/{self.sheet_id}/edit#gid={gid}"

    @property
    def service_account_email(self) -> Optional[str]:
        return getattr(self.credentials, "service_account_email", None)

    # ------------------------------------------------------------------
    # Internal helpers (run in a thread pool)

    def _initialise(self) -> None:
        assert gspread is not None
        client = gspread.authorize(self.credentials)
        spreadsheet = client.open_by_key(self.sheet_id)
        try:
            worksheet = spreadsheet.worksheet(self.worksheet_title)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(
                title=self.worksheet_title,
                rows="200",
                cols=str(len(self.HEADERS)),
            )
        self._client = client
        self._spreadsheet = spreadsheet
        self._worksheet = worksheet
        self._worksheet_id = getattr(worksheet, "id", None)
        self._prepare_worksheet()

    def _prepare_worksheet(self) -> None:
        assert self._worksheet is not None
        existing = self._worksheet.row_values(1)
        if [item.strip() for item in existing] != list(self.HEADERS):
            self._worksheet.update("A1", [list(self.HEADERS)])

        if self._spreadsheet is not None and self._worksheet_id is not None:
            requests: list[dict[str, Any]] = [
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": self._worksheet_id,
                            "gridProperties": {"frozenRowCount": 1},
                        },
                        "fields": "gridProperties.frozenRowCount",
                    }
                }
            ]
            for index, width in enumerate(self.COLUMN_WIDTHS):
                requests.append(
                    {
                        "updateDimensionProperties": {
                            "range": {
                                "sheetId": self._worksheet_id,
                                "dimension": "COLUMNS",
                                "startIndex": index,
                                "endIndex": index + 1,
                            },
                            "properties": {"pixelSize": width},
                            "fields": "pixelSize",
                        }
                    }
                )
            self._spreadsheet.batch_update({"requests": requests})

    def _append_row_sync(
        self,
        record: dict[str, Any],
        payment_status: str,
        image_value: Optional[str],
    ) -> int:
        assert self._worksheet is not None
        values = [
            record.get("id") or "",
            record.get("created_at") or "",
            record.get("program") or "",
            record.get("child_name") or "",
            record.get("class") or "",
            record.get("phone") or "",
            record.get("time") or "",
            record.get("payment_note") or "",
            payment_status,
            "",
            record.get("submitted_by") or "",
            record.get("chat_title") or "",
        ]
        response = self._worksheet.append_row(values, value_input_option="USER_ENTERED")
        row_number = self._row_from_response(response, record.get("id"))
        if image_value is not None:
            self._worksheet.update_cell(row_number, self.IMAGE_COLUMN_INDEX, image_value)
        return row_number

    def _row_from_response(self, response: Any, registration_id: Any) -> int:
        assert self._worksheet is not None
        if isinstance(response, dict):
            updates = response.get("updates")
            if isinstance(updates, dict):
                updated_range = updates.get("updatedRange")
                if isinstance(updated_range, str):
                    match = re.search(r"[A-Z]+(\d+)", updated_range.split("!")[-1])
                    if match:
                        try:
                            return int(match.group(1))
                        except ValueError:
                            pass
        if registration_id:
            try:
                cell = self._worksheet.find(str(registration_id))
                if cell is not None:
                    return cell.row
            except Exception:  # pragma: no cover - depends on Sheets API
                pass
        return self._worksheet.row_count

    def _delete_row_sync(self, registration_id: str) -> None:
        assert self._worksheet is not None
        try:
            cell = self._worksheet.find(registration_id)
        except Exception:  # pragma: no cover - network dependent
            cell = None
        if cell is not None:
            try:
                self._worksheet.delete_rows(cell.row)
            except Exception:  # pragma: no cover - network dependent
                LOGGER.warning("Не удалось удалить строку %s из Google Sheets", cell.row)

    def _update_image_sync(self, registration_id: str, image_value: Optional[str]) -> None:
        assert self._worksheet is not None
        try:
            cell = self._worksheet.find(registration_id)
        except Exception:  # pragma: no cover - network dependent
            cell = None
        if cell is None:
            return
        if image_value is not None:
            self._worksheet.update_cell(cell.row, self.IMAGE_COLUMN_INDEX, image_value)
        else:
            self._worksheet.update_cell(cell.row, self.IMAGE_COLUMN_INDEX, "")


@dataclass
class ConfettiTelegramBot:
    """Light-weight wrapper around the PTB application builder."""

    token: str
    admin_chat_ids: AdminChatIdsInput = ()
    content_template: BotContent = field(default_factory=BotContent.default)
    storage_path: Optional[Path] = None

    REGISTRATION_PROGRAM = 1
    REGISTRATION_CHILD_NAME = 2
    REGISTRATION_CLASS = 3
    REGISTRATION_PHONE = 4
    REGISTRATION_TIME = 5
    REGISTRATION_PAYMENT = 6
    REGISTRATION_CONFIRM_DETAILS = 7
    REGISTRATION_TIME_DECISION = 8

    CANCELLATION_PROGRAM = 21
    CANCELLATION_REASON = 22

    MAIN_MENU_BUTTON = "⬅️ Главное меню"
    REGISTRATION_BUTTON = "📝 Запись"
    CANCELLATION_BUTTON = "❗️ Отменить занятие"
    REGISTRATION_SKIP_PAYMENT_BUTTON = "⏭ Пока без оплаты"
    REGISTRATION_CONFIRM_SAVED_BUTTON = "✅ Продолжить"
    REGISTRATION_EDIT_DETAILS_BUTTON = "✏️ Изменить данные"
    REGISTRATION_KEEP_TIME_BUTTON = "🔁 То же время"
    REGISTRATION_NEW_TIME_BUTTON = "⏰ Другое время"
    BACK_BUTTON = "◀️ Назад"
    REGISTRATION_LIST_BUTTON = "📋 Список записей"
    ADMIN_MENU_BUTTON = "🛠 Админ-панель"
    ADMIN_BACK_TO_USER_BUTTON = "⬅️ Пользовательское меню"
    ADMIN_BROADCAST_BUTTON = "📣 Рассылка"
    ADMIN_EXPORT_TABLE_BUTTON = "📊 Таблица заявок"
    ADMIN_ADD_ADMIN_BUTTON = "➕ Добавить администратора"
    ADMIN_EDIT_SCHEDULE_BUTTON = "🗓 Редактировать расписание"
    ADMIN_EDIT_ABOUT_BUTTON = "ℹ️ Редактировать информацию"
    ADMIN_EDIT_TEACHERS_BUTTON = "👩‍🏫 Редактировать преподавателей"
    ADMIN_EDIT_ALBUM_BUTTON = "📸 Редактировать фотоальбом"
    ADMIN_EDIT_CONTACTS_BUTTON = "📞 Редактировать контакты"
    ADMIN_EDIT_VOCABULARY_BUTTON = "📚 Редактировать словарь"
    ADMIN_CANCEL_KEYWORDS = ("отмена", "annuler", "cancel")
    ADMIN_CANCEL_PROMPT = f"\n\nЧтобы отменить, нажмите «{BACK_BUTTON}» или напишите «Отмена»."

    MAIN_MENU_LAYOUT = (
        (REGISTRATION_BUTTON, "📅 Расписание"),
        ("ℹ️ О студии", "👩‍🏫 Преподаватели"),
        (REGISTRATION_LIST_BUTTON, "📞 Контакты"),
        ("📚 Полезные слова", CANCELLATION_BUTTON),
    )

    TIME_OF_DAY_OPTIONS = (
        "☀️ Утро",
        "🌤 День",
        "🌙 Вечер",
    )

    PROGRAMS = (
        {
            "label": "📚 Французский каждый день",
            "audience": "С 3 по 11 класс",
            "teacher": "Преподаватель: Настыч Ксения Викторовна",
            "schedule": "Дни занятий: вторник или четверг вечером",
        },
        {
            "label": "🎭 Театр на французском (вечер)",
            "teacher": "Преподаватель: Настыч Ксения Викторовна",
            "schedule": "Дни занятий: вторник или четверг вечером",
        },
        {
            "label": "📚 Воскресный французский",
            "audience": "1–4 класс",
            "teacher": "Преподаватель: Банникова Анастасия Дмитриевна",
            "schedule": "Дни занятий: воскресенье",
        },
        {
            "label": "🎭 Театр на французском (воскресенье)",
            "teacher": "Преподаватель: Банникова Анастасия Дмитриевна",
            "schedule": "Дни занятий: воскресенье",
        },
        {
            "label": "🇫🇷 Французский по-взрослому",
            "audience": "Группа для взрослых (продолжающие)",
            "teacher": "Преподаватель: Красноборова Людмила Анатольевна",
            "schedule": "Дни занятий: понедельник / четверг / пятница",
        },
        {
            "label": "👩🏼‍🏫 Индивидуальные занятия",
        },
        {
            "label": "🍂 Осенний интенсив",
        },
    )

    TEACHERS = (
        {
            "key": "nastytsch",
            "name": "Ксения Настыч",
            "description": (
                "Преподаватель французского языка с опытом более 20 лет. "
                "Окончила Пермский государственный университет по специальности "
                "«Филология» и имеет международный сертификат DALF. "
                "Регулярно стажировалась во Франции и организовывала «русские сезоны» в Посольстве России."
            ),
            "photo_file_id": None,  # TODO: добавьте file_id фотографии Ксении Настыч
        },
        {
            "key": "bannikova",
            "name": "Анастасия Банникова",
            "description": (
                "Ведёт воскресные программы и театральные занятия. "
                "Создаёт дружелюбную атмосферу и помогает детям полюбить французский язык через игру и творчество."
            ),
            "photo_file_id": None,  # TODO: добавьте file_id фотографии Анастасии Банниковой
        },
        {
            "key": "marinot",
            "name": "Ален Марино",
            "description": (
                "Носитель французского языка с академическим парижским акцентом. "
                "Актёр и душа студии, который общается с учениками только по-французски и погружает в живую культуру."
            ),
            "photo_file_id": None,  # TODO: добавьте file_id фотографии Алена Марино
        },
        {
            "key": "krasnoborova",
            "name": "Людмила Красноборова",
            "description": (
                "Кандидат филологических наук, доцент ПГНИУ и экзаменатор DALF. "
                "Готовит подростков и взрослых к экзаменам и олимпиадам, сочетая академизм и практику."
            ),
            "photo_file_id": None,  # TODO: добавьте file_id фотографии Людмилы Красноборовой
        },
    )

    VOCABULARY = (
        {
            "word": "Soleil",
            "emoji": "☀️",
            "translation": "Солнце",
            "example_fr": "Le soleil brille.",
            "example_ru": "Солнце светит.",
        },
        {
            "word": "Bonjour",
            "emoji": "👋",
            "translation": "Здравствуйте",
            "example_fr": "Bonjour, comment ça va ?",
            "example_ru": "Здравствуйте, как дела?",
        },
        {
            "word": "Amitié",
            "emoji": "🤝",
            "translation": "Дружба",
            "example_fr": "L'amitié rend la vie plus douce.",
            "example_ru": "Дружба делает жизнь добрее.",
        },
        {
            "word": "Étoile",
            "emoji": "✨",
            "translation": "Звезда",
            "example_fr": "Chaque étoile brille à sa manière.",
            "example_ru": "Каждая звезда сияет по-своему.",
        },
    )

    CONTENT_LABELS = {
        "schedule": "Расписание",
        "about": "О студии",
        "teachers": "Преподаватели",
        "payment": "Оплата",
        "album": "Фотоальбом",
        "contacts": "Контакты",
    }

    def build_application(self) -> Application:
        """Construct the PTB application."""

        _require_telegram()

        builder = ApplicationBuilder().token(self.token)

        limiter = self._build_rate_limiter()
        if limiter is not None:
            builder = builder.rate_limiter(limiter)

        application = builder.build()
        self._register_handlers(application)
        return application

    def __post_init__(self) -> None:
        normalised = _normalise_admin_chat_ids(self.admin_chat_ids)
        self.admin_chat_ids = normalised
        self._runtime_admin_ids: set[int] = set(normalised)
        self._admin_cancel_tokens: set[str] = {token.lower() for token in self.ADMIN_CANCEL_KEYWORDS}
        storage_path = self.storage_path or Path(os.environ.get("CONFETTI_STORAGE_PATH", "data/confetti_state.json"))
        self.storage_path = storage_path.expanduser()
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        self._known_registration_ids: set[str] = set()
        self._persistent_store: dict[str, Any] = self._load_persistent_state()
        self._ensure_registration_ids()
        dynamic_admins = self._persistent_store.get("dynamic_admins")
        if isinstance(dynamic_admins, set):
            self._runtime_admin_ids.update(dynamic_admins)
        self._storage_dirty = False
        self._bot_username: Optional[str] = None
        self._sheets_backend: Optional[GoogleSheetsBackend] = self._create_sheets_backend()

    # ------------------------------------------------------------------
    # Persistence helpers

    def _ensure_registration_ids(self) -> None:
        registrations = self._persistent_store.get("registrations")
        if not isinstance(registrations, list):
            self._persistent_store["registrations"] = []
            return

        dirty = False
        for entry in registrations:
            if not isinstance(entry, dict):
                continue
            record_id = entry.get("id")
            if record_id:
                record_id_str = str(record_id)
                entry["id"] = record_id_str
                self._known_registration_ids.add(record_id_str)
            else:
                entry["id"] = self._generate_registration_id()
                dirty = True
        if dirty:
            self._save_persistent_state()

    def _create_sheets_backend(self) -> Optional[GoogleSheetsBackend]:
        sheet_id = os.environ.get("CONFETTI_GOOGLE_SHEET_ID")
        if not sheet_id:
            return None
        if gspread is None or Credentials is None:
            LOGGER.warning(
                "Google Sheets не настроен: отсутствуют зависимости gspread/google-auth."
            )
            return None

        credentials_payload = self._load_service_account_credentials()
        if credentials_payload is None:
            LOGGER.warning(
                "Google Sheets не настроен: не найден файл или JSON сервисного аккаунта."
            )
            return None

        try:
            credentials = Credentials.from_service_account_info(
                credentials_payload,
                scopes=[
                    "https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive",
                ],
            )
        except Exception as exc:  # pragma: no cover - depends on config
            LOGGER.warning("Не удалось загрузить креды Google: %s", exc)
            return None

        try:
            return GoogleSheetsBackend(sheet_id=sheet_id, credentials=credentials)
        except SheetsBackendError as exc:  # pragma: no cover - optional deps
            LOGGER.warning("Google Sheets недоступен: %s", exc)
        except Exception as exc:  # pragma: no cover - optional deps
            LOGGER.warning("Не удалось инициализировать Google Sheets: %s", exc)
        return None

    def _load_service_account_credentials(self) -> Optional[dict[str, Any]]:
        candidates = [
            os.environ.get("CONFETTI_GOOGLE_SERVICE_ACCOUNT_JSON"),
            os.environ.get("CONFETTI_GOOGLE_SERVICE_ACCOUNT_FILE"),
        ]
        for value in candidates:
            if not value:
                continue
            payload = self._parse_credentials_value(value)
            if payload is not None:
                return payload
        return None

    def _parse_credentials_value(self, value: str) -> Optional[dict[str, Any]]:
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            path = Path(value).expanduser()
            if not path.exists():
                return None
            try:
                parsed = json.loads(path.read_text(encoding="utf-8"))
            except Exception:  # pragma: no cover - filesystem dependant
                LOGGER.warning("Не удалось прочитать файл с сервисным аккаунтом: %s", path)
                return None
        if isinstance(parsed, dict):
            return parsed
        LOGGER.warning("Неверный формат сервисного аккаунта: ожидался JSON-объект.")
        return None

    def _generate_registration_id(self) -> str:
        while True:
            candidate = datetime.utcnow().strftime("%Y%m%d%H%M%S") + f"-{random.randint(1000, 9999)}"
            if candidate not in self._known_registration_ids:
                self._known_registration_ids.add(candidate)
                return candidate

    def _load_persistent_state(self) -> dict[str, Any]:
        """Load bot state from disk and normalise structures."""

        data: dict[str, Any] = {}

        if self.storage_path.exists():
            try:
                raw = json.loads(self.storage_path.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    data.update(raw)
            except Exception as exc:  # pragma: no cover - filesystem dependant
                LOGGER.warning("Не удалось загрузить сохранённое состояние: %s", exc)

        content_payload = data.get("content")
        if isinstance(content_payload, dict):
            content = self._deserialize_content(content_payload)
        else:
            content = self.content_template.copy()
        data["content"] = content

        registrations = data.get("registrations")
        if isinstance(registrations, list):
            filtered: list[dict[str, Any]] = []
            for item in registrations:
                if isinstance(item, dict):
                    filtered.append(item)
            data["registrations"] = filtered
        else:
            data["registrations"] = []

        cancellations = data.get("cancellations")
        if not isinstance(cancellations, list):
            data["cancellations"] = []

        known_raw = data.get("known_chats")
        known: set[int] = set()
        if isinstance(known_raw, (list, set, tuple)):
            for item in known_raw:
                try:
                    known.add(_coerce_chat_id(item))
                except ValueError:
                    continue
        data["known_chats"] = known

        admins_raw = data.get("dynamic_admins")
        admins: set[int] = set()
        if isinstance(admins_raw, (list, set, tuple)):
            for item in admins_raw:
                try:
                    admins.add(_coerce_chat_id(item))
                except ValueError:
                    continue
        data["dynamic_admins"] = admins

        profiles = data.get("user_profiles")
        if not isinstance(profiles, dict):
            profiles = {}
        else:
            normalised_profiles: dict[str, dict[str, Any]] = {}
            for key, value in profiles.items():
                if not isinstance(key, str) or not isinstance(value, dict):
                    continue

                def _text(field: str) -> str:
                    payload = value.get(field)
                    return str(payload) if payload is not None else ""

                def _item_text(source: dict[str, Any], field: str) -> str:
                    payload = source.get(field)
                    return str(payload) if payload is not None else ""

                entry: dict[str, Any] = {
                    "child_name": _text("child_name"),
                    "class": _text("class"),
                    "phone": _text("phone"),
                    "last_program": _text("last_program"),
                    "last_time": _text("last_time"),
                }

                registrations_payload = value.get("registrations")
                registrations: list[dict[str, str]] = []
                if isinstance(registrations_payload, list):
                    seen_ids: set[str] = set()
                    for item in registrations_payload:
                        if not isinstance(item, dict):
                            continue
                        reg_id_raw = item.get("id")
                        reg_id = str(reg_id_raw).strip() if reg_id_raw is not None else ""
                        if not reg_id or reg_id in seen_ids:
                            continue
                        seen_ids.add(reg_id)
                        registrations.append(
                            {
                                "id": reg_id,
                                "program": _item_text(item, "program"),
                                "time": _item_text(item, "time"),
                                "child_name": _item_text(item, "child_name"),
                                "class": _item_text(item, "class"),
                                "created_at": _item_text(item, "created_at"),
                            }
                        )
                entry["registrations"] = registrations
                normalised_profiles[key] = entry
            profiles = normalised_profiles
        data["user_profiles"] = profiles

        exports = data.get("exports")
        if not isinstance(exports, dict):
            data["exports"] = {}

        return data

    def _serialize_persistent_store(self) -> dict[str, Any]:
        """Prepare the in-memory state for JSON serialisation."""

        payload: dict[str, Any] = {}

        for key, value in self._persistent_store.items():
            if key == "content" and isinstance(value, BotContent):
                payload[key] = self._serialize_content(value)
            elif key in {"known_chats", "dynamic_admins"} and isinstance(value, set):
                payload[key] = sorted(value)
            elif key == "user_profiles" and isinstance(value, dict):
                payload[key] = value
            else:
                payload[key] = value

        return payload

    def _save_persistent_state(self) -> None:
        """Persist the current state to disk."""

        try:
            serializable = self._serialize_persistent_store()
            tmp_path = self.storage_path.with_suffix(self.storage_path.suffix + ".tmp")
            with tmp_path.open("w", encoding="utf-8") as handle:
                json.dump(serializable, handle, ensure_ascii=False, indent=2)
            tmp_path.replace(self.storage_path)
            self._storage_dirty = False
        except Exception as exc:  # pragma: no cover - filesystem dependant
            LOGGER.warning("Не удалось сохранить состояние бота: %s", exc)

    def _serialize_content(self, content: BotContent) -> dict[str, Any]:
        return {
            "schedule": self._serialize_content_block(content.schedule),
            "about": self._serialize_content_block(content.about),
            "teachers": self._serialize_content_block(content.teachers),
            "payment": self._serialize_content_block(content.payment),
            "album": self._serialize_content_block(content.album),
            "contacts": self._serialize_content_block(content.contacts),
            "vocabulary": [dict(entry) for entry in content.vocabulary],
        }

    def _serialize_content_block(self, block: ContentBlock) -> dict[str, Any]:
        return {
            "text": block.text,
            "media": [
                {
                    "kind": item.kind,
                    "file_id": item.file_id,
                    "caption": item.caption,
                }
                for item in block.media
            ],
        }

    def _deserialize_content(self, payload: dict[str, Any]) -> BotContent:
        content = self.content_template.copy()
        for field_name in self.CONTENT_LABELS:
            block_payload = payload.get(field_name)
            if isinstance(block_payload, dict):
                setattr(content, field_name, self._deserialize_content_block(block_payload))
        vocabulary = payload.get("vocabulary")
        if isinstance(vocabulary, list):
            content.vocabulary = [entry for entry in vocabulary if isinstance(entry, dict)]
        return content

    def _deserialize_content_block(self, payload: dict[str, Any]) -> ContentBlock:
        text = str(payload.get("text", ""))
        media_payload = payload.get("media")
        media: list[MediaAttachment] = []
        if isinstance(media_payload, list):
            for item in media_payload:
                if not isinstance(item, dict):
                    continue
                kind = item.get("kind")
                file_id = item.get("file_id")
                if not kind or not file_id:
                    continue
                media.append(
                    MediaAttachment(
                        kind=str(kind),
                        file_id=str(file_id),
                        caption=item.get("caption"),
                    )
                )
        return ContentBlock(text=text, media=media)

    def _user_key(self, identity: Any | None) -> Optional[str]:
        if identity is None:
            return None
        candidate = getattr(identity, "id", identity)
        try:
            coerced = _coerce_chat_id(candidate)  # type: ignore[arg-type]
        except ValueError:
            return None
        return str(coerced)

    def _get_user_defaults(self, user: Any | None) -> dict[str, str]:
        if user is None:
            return {}
        user_key = self._user_key(user)
        if user_key is None:
            return {}
        profiles = self._persistent_store.setdefault("user_profiles", {})
        if not isinstance(profiles, dict):
            profiles = {}
            self._persistent_store["user_profiles"] = profiles
        entry = profiles.get(user_key)
        if isinstance(entry, dict):
            return {
                "child_name": entry.get("child_name", ""),
                "class": entry.get("class", ""),
                "phone": entry.get("phone", ""),
                "program": entry.get("last_program", ""),
                "time": entry.get("last_time", ""),
            }
        return {}

    def _update_user_defaults(self, user: Any | None, data: dict[str, Any]) -> bool:
        if user is None:
            return False
        user_key = self._user_key(user)
        if user_key is None:
            return False
        profiles = self._persistent_store.setdefault("user_profiles", {})
        if not isinstance(profiles, dict):
            profiles = {}
            self._persistent_store["user_profiles"] = profiles
        entry = profiles.get(user_key)
        if not isinstance(entry, dict):
            entry = {}
            profiles[user_key] = entry

        changed = False
        for source_key, target_key in (
            ("child_name", "child_name"),
            ("class", "class"),
            ("phone", "phone"),
        ):
            value = str(data.get(source_key, ""))
            if entry.get(target_key) != value:
                entry[target_key] = value
                changed = True

        for source_key, target_key in (("program", "last_program"), ("time", "last_time")):
            value = data.get(source_key)
            if value is None:
                continue
            value_str = str(value)
            if entry.get(target_key) != value_str:
                entry[target_key] = value_str
                changed = True

        registrations = entry.get("registrations")
        if not isinstance(registrations, list):
            entry["registrations"] = []
            changed = True

        return changed

    def _identity_keys(self, *identities: Any | None) -> list[str]:
        keys: list[str] = []
        for identity in identities:
            key = self._user_key(identity)
            if key is not None and key not in keys:
                keys.append(key)
        return keys

    def _user_profile_entry_by_key(self, user_key: str) -> dict[str, Any]:
        profiles = self._persistent_store.setdefault("user_profiles", {})
        if not isinstance(profiles, dict):
            profiles = {}
            self._persistent_store["user_profiles"] = profiles
        entry = profiles.get(user_key)
        if not isinstance(entry, dict):
            entry = {}
            profiles[user_key] = entry
        registrations = entry.get("registrations")
        if not isinstance(registrations, list):
            entry["registrations"] = []
        return entry

    def _append_user_registration_snapshot(
        self,
        record: dict[str, Any],
        *identities: Any | None,
    ) -> bool:
        record_id = record.get("id")
        if record_id is None:
            return False
        record_id_str = str(record_id)
        snapshot = {
            "id": record_id_str,
            "program": str(record.get("program", "")),
            "time": str(record.get("time", "")),
            "child_name": str(record.get("child_name", "")),
            "class": str(record.get("class", "")),
            "created_at": str(record.get("created_at", "")),
        }
        changed = False
        for key in self._identity_keys(*identities):
            entry = self._user_profile_entry_by_key(key)
            registrations = entry.setdefault("registrations", [])
            if not isinstance(registrations, list):
                registrations = []
                entry["registrations"] = registrations
            replaced = False
            for index, existing in enumerate(registrations):
                if isinstance(existing, dict) and existing.get("id") == record_id_str:
                    if existing != snapshot:
                        registrations[index] = snapshot
                        changed = True
                    replaced = True
                    break
            if not replaced:
                registrations.append(snapshot)
                changed = True
            if snapshot["program"] and entry.get("last_program") != snapshot["program"]:
                entry["last_program"] = snapshot["program"]
                changed = True
            if snapshot["time"] and entry.get("last_time") != snapshot["time"]:
                entry["last_time"] = snapshot["time"]
                changed = True
        return changed

    def _remove_user_registration_snapshot(self, record: dict[str, Any]) -> bool:
        record_id = record.get("id")
        if record_id is None:
            return False
        record_id_str = str(record_id)
        identities = self._identity_keys(record.get("submitted_by_id"), record.get("chat_id"))
        if not identities:
            return False
        changed = False
        for key in identities:
            entry = self._user_profile_entry_by_key(key)
            registrations = entry.get("registrations")
            if not isinstance(registrations, list):
                entry["registrations"] = []
                continue
            original_len = len(registrations)
            registrations[:] = [
                item for item in registrations if not (isinstance(item, dict) and item.get("id") == record_id_str)
            ]
            if len(registrations) != original_len:
                changed = True
                if registrations:
                    latest = registrations[-1]
                    entry["last_program"] = str(latest.get("program", ""))
                    entry["last_time"] = str(latest.get("time", ""))
                else:
                    entry["last_program"] = ""
                    entry["last_time"] = ""
        return changed

    def _collect_user_registrations(
        self,
        user: Any | None,
        chat: Any | None,
    ) -> list[dict[str, Any]]:
        records: dict[str, dict[str, Any]] = {}
        profiles = self._persistent_store.setdefault("user_profiles", {})
        if not isinstance(profiles, dict):
            profiles = {}
            self._persistent_store["user_profiles"] = profiles
        for key in self._identity_keys(user, chat):
            entry = profiles.get(key)
            if not isinstance(entry, dict):
                continue
            registrations = entry.get("registrations")
            if not isinstance(registrations, list):
                continue
            for item in registrations:
                if not isinstance(item, dict):
                    continue
                record_id = item.get("id")
                if not record_id:
                    continue
                record_id_str = str(record_id)
                if record_id_str not in records:
                    records[record_id_str] = item
        return list(records.values())

    def build_profile(self, chat: Any, user: Any | None = None) -> "UserProfile":
        """Return the appropriate profile for ``chat`` and optional ``user``."""

        chat_id = _coerce_chat_id_from_object(chat)
        if self._is_admin_identity(chat=chat, user=user):
            return AdminProfile(chat_id=chat_id)
        return UserProfile(chat_id=chat_id)

    def is_admin_chat(self, chat: Any) -> bool:
        """Return ``True`` when ``chat`` belongs to an administrator."""

        return self._is_admin_identity(chat=chat)

    def is_admin_user(self, user: Any) -> bool:
        """Return ``True`` when ``user`` is recognised as an administrator."""

        return self._is_admin_identity(user=user)

    def _build_rate_limiter(self) -> Optional[AIORateLimiter]:  # type: ignore[name-defined]
        """Return an ``AIORateLimiter`` instance when possible."""

        if AIORateLimiter is None:
            LOGGER.warning(
                "python-telegram-bot was installed without the optional rate limiter extras. "
                "The bot will run without a rate limiter."
            )
            return None

        try:
            return AIORateLimiter()
        except RuntimeError as exc:  # pragma: no cover - depends on installation
            LOGGER.warning(
                "Failed to initialise the AIORateLimiter: %s. Running without a rate limiter.",
                exc,
            )
            return None

    # ------------------------------------------------------------------
    # Handler registration helpers

    def _register_handlers(self, application: Application) -> None:
        """Attach all command and message handlers to ``application``."""

        conversation = ConversationHandler(
            entry_points=[
                MessageHandler(
                    filters.Regex(self._exact_match_regex(self.REGISTRATION_BUTTON)),
                    self._start_registration,
                )
            ],
            states={
                self.REGISTRATION_PROGRAM: [
                    CallbackQueryHandler(
                        self._registration_collect_program,
                        pattern=r"^reg_program:\\d+$",
                    ),
                    CallbackQueryHandler(
                        self._registration_cancel_from_program,
                        pattern=r"^reg_back:menu$",
                    ),
                    MessageHandler(
                        filters.Regex(self._exact_match_regex(self.MAIN_MENU_BUTTON)),
                        self._registration_cancel,
                    ),
                    MessageHandler(
                        filters.TEXT & ~filters.COMMAND,
                        self._registration_prompt_program_buttons,
                    ),
                ],
                self.REGISTRATION_CHILD_NAME: [
                    MessageHandler(
                        filters.Regex(self._exact_match_regex(self.MAIN_MENU_BUTTON)),
                        self._registration_cancel,
                    ),
                    MessageHandler(
                        filters.Regex(self._exact_match_regex(self.BACK_BUTTON)),
                        self._registration_back_to_program,
                    ),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self._registration_collect_child_name),
                ],
                self.REGISTRATION_CLASS: [
                    MessageHandler(
                        filters.Regex(self._exact_match_regex(self.MAIN_MENU_BUTTON)),
                        self._registration_cancel,
                    ),
                    MessageHandler(
                        filters.Regex(self._exact_match_regex(self.BACK_BUTTON)),
                        self._registration_back_to_child_name,
                    ),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self._registration_collect_class),
                ],
                self.REGISTRATION_PHONE: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self._registration_collect_phone_text),
                ],
                self.REGISTRATION_CONFIRM_DETAILS: [
                    MessageHandler(
                        filters.Regex(self._exact_match_regex(self.REGISTRATION_CONFIRM_SAVED_BUTTON)),
                        self._registration_accept_saved_details,
                    ),
                    MessageHandler(
                        filters.Regex(self._exact_match_regex(self.REGISTRATION_EDIT_DETAILS_BUTTON)),
                        self._registration_request_details_update,
                    ),
                    MessageHandler(
                        filters.Regex(self._exact_match_regex(self.BACK_BUTTON)),
                        self._registration_back_from_confirm,
                    ),
                    MessageHandler(
                        filters.Regex(self._exact_match_regex(self.MAIN_MENU_BUTTON)),
                        self._registration_cancel,
                    ),
                ],
                self.REGISTRATION_TIME_DECISION: [
                    MessageHandler(
                        filters.Regex(self._exact_match_regex(self.REGISTRATION_KEEP_TIME_BUTTON)),
                        self._registration_use_saved_time,
                    ),
                    MessageHandler(
                        filters.Regex(self._exact_match_regex(self.REGISTRATION_NEW_TIME_BUTTON)),
                        self._registration_request_new_time,
                    ),
                    MessageHandler(
                        filters.Regex(self._exact_match_regex(self.BACK_BUTTON)),
                        self._registration_back_from_time_decision,
                    ),
                    MessageHandler(
                        filters.Regex(self._exact_match_regex(self.MAIN_MENU_BUTTON)),
                        self._registration_cancel,
                    ),
                ],
                self.REGISTRATION_TIME: [
                    MessageHandler(
                        filters.Regex(self._exact_match_regex(self.BACK_BUTTON)),
                        self._registration_back_from_time,
                    ),
                    MessageHandler(
                        filters.Regex(self._time_regex()),
                        self._registration_collect_time,
                    ),
                    MessageHandler(
                        filters.Regex(self._exact_match_regex(self.MAIN_MENU_BUTTON)),
                        self._registration_cancel,
                    ),
                ],
                self.REGISTRATION_PAYMENT: [
                    MessageHandler(~filters.COMMAND, self._registration_collect_payment),
                ],
            },
            fallbacks=[
                CommandHandler("cancel", self._registration_cancel),
                MessageHandler(
                    filters.Regex(self._exact_match_regex(self.MAIN_MENU_BUTTON)),
                    self._registration_cancel,
                ),
            ],
            allow_reentry=True,
        )

        cancellation = ConversationHandler(
            entry_points=[
                MessageHandler(
                    filters.Regex(self._exact_match_regex(self.CANCELLATION_BUTTON)),
                    self._start_cancellation,
                )
            ],
            states={
                self.CANCELLATION_PROGRAM: [
                    MessageHandler(
                        filters.TEXT & ~filters.COMMAND,
                        self._cancellation_collect_program,
                    ),
                    MessageHandler(
                        filters.Regex(self._exact_match_regex(self.MAIN_MENU_BUTTON)),
                        self._cancellation_cancel,
                    ),
                ],
                self.CANCELLATION_REASON: [
                    MessageHandler(~filters.COMMAND, self._cancellation_collect_reason),
                ],
            },
            fallbacks=[
                CommandHandler("cancel", self._cancellation_cancel),
                MessageHandler(
                    filters.Regex(self._exact_match_regex(self.MAIN_MENU_BUTTON)),
                    self._cancellation_cancel,
                ),
            ],
            allow_reentry=True,
        )

        application.add_handler(CommandHandler("start", self._start))
        application.add_handler(CommandHandler("menu", self._show_main_menu))
        application.add_handler(CommandHandler("admin", self._show_admin_menu))
        application.add_handler(conversation)
        application.add_handler(cancellation)
        application.add_handler(CallbackQueryHandler(self._teacher_show_profile, pattern=r"^teacher:"))
        application.add_handler(MessageHandler(~filters.COMMAND, self._handle_message))

    def _exact_match_regex(self, text: str) -> str:
        return rf"^{re.escape(text)}$"

    def _time_regex(self) -> str:
        parts = [re.escape(option) for option in self.TIME_OF_DAY_OPTIONS]
        return rf"^({'|'.join(parts)})$"

    # ------------------------------------------------------------------
    # Shared messaging helpers

    def _main_menu_markup(self, *, include_admin: bool = False) -> ReplyKeyboardMarkup:
        keyboard = [list(row) for row in self.MAIN_MENU_LAYOUT]
        if include_admin:
            keyboard.append([self.ADMIN_MENU_BUTTON])
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

    def _main_menu_markup_for(
        self, update: Update, context: Optional[ContextTypes.DEFAULT_TYPE] = None
    ) -> ReplyKeyboardMarkup:
        return self._main_menu_markup(include_admin=self._is_admin_update(update, context))

    def _admin_menu_markup(self) -> ReplyKeyboardMarkup:
        keyboard = [
            [self.ADMIN_BACK_TO_USER_BUTTON],
            [self.ADMIN_BROADCAST_BUTTON, self.ADMIN_EXPORT_TABLE_BUTTON],
            [self.ADMIN_ADD_ADMIN_BUTTON],
            [self.ADMIN_EDIT_SCHEDULE_BUTTON],
            [self.ADMIN_EDIT_ABOUT_BUTTON],
            [self.ADMIN_EDIT_TEACHERS_BUTTON],
            [self.ADMIN_EDIT_ALBUM_BUTTON],
            [self.ADMIN_EDIT_CONTACTS_BUTTON],
            [self.ADMIN_EDIT_VOCABULARY_BUTTON],
        ]
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

    def _is_admin_identity(self, *, chat: Any | None = None, user: Any | None = None) -> bool:
        """Check whether either ``chat`` or ``user`` matches an admin id."""

        for candidate in (chat, user):
            if candidate is None:
                continue
            try:
                candidate_id = _coerce_chat_id_from_object(candidate)
            except ValueError:
                continue
            if candidate_id in self._runtime_admin_ids:
                return True
        return False

    def _is_admin_update(
        self, update: Update, context: Optional[ContextTypes.DEFAULT_TYPE] = None
    ) -> bool:
        if context is not None:
            self._refresh_admin_cache(context)
        return self._is_admin_identity(chat=update.effective_chat, user=update.effective_user)

    def _application_data(self, context: ContextTypes.DEFAULT_TYPE) -> dict[str, Any]:
        """Return application-level storage across PTB versions."""

        storage = self._persistent_store

        # Expose the shared storage on context objects for compatibility, ignoring failures.
        for attribute in ("application_data", "bot_data"):
            if hasattr(context, attribute):
                try:
                    setattr(context, attribute, storage)
                except Exception:  # pragma: no cover - attribute may be read-only
                    pass

        setattr(context, "_fallback_application_data", storage)
        return storage

    def _refresh_admin_cache(self, context: ContextTypes.DEFAULT_TYPE) -> set[int]:
        """Load dynamic administrators from storage into the runtime cache."""

        storage = self._application_data(context)
        candidates = storage.get("dynamic_admins")
        ids: set[int] = set()
        if isinstance(candidates, (set, list, tuple)):
            for candidate in candidates:
                try:
                    ids.add(_coerce_chat_id(candidate))
                except ValueError:
                    continue
        storage["dynamic_admins"] = ids
        self._runtime_admin_ids.update(ids)
        return ids

    def _store_dynamic_admin(
        self, context: ContextTypes.DEFAULT_TYPE, admin_id: int
    ) -> set[int]:
        storage = self._application_data(context)
        existing = storage.get("dynamic_admins")
        if not isinstance(existing, set):
            existing = self._refresh_admin_cache(context)
        if admin_id in existing:
            return existing
        existing.add(admin_id)
        storage["dynamic_admins"] = existing
        self._runtime_admin_ids.add(admin_id)
        self._save_persistent_state()
        return existing

    def _remember_chat(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        self._refresh_admin_cache(context)
        chat = update.effective_chat
        if not chat:
            return
        known = self._get_known_chats(context)
        chat_id = _coerce_chat_id_from_object(chat)
        if chat_id not in known:
            known.add(chat_id)
            self._save_persistent_state()

    def _get_known_chats(self, context: ContextTypes.DEFAULT_TYPE) -> set[int]:
        store = self._application_data(context).setdefault("known_chats", set())
        if isinstance(store, set):
            return store
        if isinstance(store, list):
            converted: set[int] = set()
            for chat_id in store:
                try:
                    converted.add(_coerce_chat_id(chat_id))
                except ValueError:
                    continue
            self._application_data(context)["known_chats"] = converted
            self._save_persistent_state()
            return converted
        converted: set[int] = set()
        self._application_data(context)["known_chats"] = converted
        self._save_persistent_state()
        return converted

    def _get_content(self, context: ContextTypes.DEFAULT_TYPE) -> BotContent:
        content = self._application_data(context).get("content")
        if isinstance(content, BotContent):
            for field_name in self.CONTENT_LABELS:
                block = getattr(content, field_name, None)
                if isinstance(block, str):
                    setattr(content, field_name, ContentBlock(text=block))
            return content
        if isinstance(content, dict):
            # Backward compatibility if someone serialised a dict previously.
            restored = self.content_template.copy()
            self._application_data(context)["content"] = restored
            self._save_persistent_state()
            return restored
        fresh = self.content_template.copy()
        self._application_data(context)["content"] = fresh
        self._save_persistent_state()
        return fresh

    def _store_registration(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        data: dict[str, Any],
        attachments: Optional[list[MediaAttachment]] = None,
    ) -> dict[str, Any]:
        chat = update.effective_chat
        user = update.effective_user
        record_id = data.get("id") or self._generate_registration_id()
        record = {
            "id": record_id,
            "program": data.get("program", ""),
            "child_name": data.get("child_name", ""),
            "class": data.get("class", ""),
            "phone": data.get("phone", ""),
            "time": data.get("time", ""),
            "chat_id": _coerce_chat_id_from_object(chat) if chat else None,
            "chat_title": getattr(chat, "title", None) if chat else None,
            "submitted_by": getattr(user, "full_name", None) if user else None,
            "submitted_by_id": getattr(user, "id", None) if user else None,
            "created_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
            "payment_note": data.get("payment_note", ""),
            "payment_media": self._attachments_to_dicts(attachments or [])
            if attachments
            else data.get("payment_media", []),
        }
        registrations = self._application_data(context).setdefault("registrations", [])
        needs_save = False
        if isinstance(registrations, list):
            registrations.append(record)
            needs_save = True
        else:
            self._application_data(context)["registrations"] = [record]
            needs_save = True

        if self._append_user_registration_snapshot(record, user, chat):
            needs_save = True

        if self._update_user_defaults(user, data):
            needs_save = True

        if needs_save:
            self._save_persistent_state()

        return record

    async def _sync_registration_sheet(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        record: dict[str, Any],
        attachments: Optional[list[MediaAttachment]],
    ) -> None:
        backend = self._sheets_backend
        if backend is None:
            return
        if record.get("sheet_row"):
            # Remove existing row before appending fresh data.
            try:
                await backend.delete_registration(str(record.get("id")))
            except Exception as exc:  # pragma: no cover - network dependent
                LOGGER.warning("Не удалось обновить строку Google Sheets: %s", exc)

        payment_media = attachments or []
        payment_status = "Получено" if payment_media else "Ожидается"
        if payment_media:
            payment_status += f" ({len(payment_media)} влож.)"

        image_formula: Optional[str] = None
        for attachment in payment_media:
            image_formula = await self._build_image_formula(context, attachment)
            if image_formula:
                break

        if payment_media:
            image_value: Optional[str] = image_formula or "\n".join(
                self._describe_attachment(item) for item in payment_media
            )
        else:
            image_value = "—"

        try:
            row_number = await backend.append_registration(
                record,
                payment_status=payment_status,
                image_value=image_value,
            )
        except Exception as exc:  # pragma: no cover - network dependent
            LOGGER.warning("Не удалось записать данные в Google Sheets: %s", exc)
            return

        record["sheet_row"] = row_number
        record["sheet_synced_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
        self._save_persistent_state()

    async def _build_image_formula(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        attachment: MediaAttachment,
    ) -> Optional[str]:
        data_url = await self._download_photo_data_url(context, attachment)
        if not data_url:
            return None
        return f'=IMAGE("{data_url}")'

    def _parse_record_timestamp(self, value: Any) -> Optional[datetime]:
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue
        return None

    async def _purge_expired_registrations(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        registrations = self._application_data(context).get("registrations")
        if not isinstance(registrations, list) or not registrations:
            return

        threshold = datetime.utcnow() - timedelta(days=7)
        removed: list[dict[str, Any]] = []
        for index in range(len(registrations) - 1, -1, -1):
            record = registrations[index]
            if not isinstance(record, dict):
                continue
            created_at = self._parse_record_timestamp(record.get("created_at"))
            if created_at is None:
                continue
            if created_at < threshold:
                removed.append(registrations.pop(index))

        if not removed:
            return

        profiles_changed = False
        for record in removed:
            if self._remove_user_registration_snapshot(record):
                profiles_changed = True

        if profiles_changed or removed:
            self._save_persistent_state()

        backend = self._sheets_backend
        if backend is None:
            return

        for record in removed:
            registration_id = record.get("id")
            if not registration_id:
                continue
            try:
                await backend.delete_registration(str(registration_id))
            except Exception as exc:  # pragma: no cover - network dependent
                LOGGER.warning("Не удалось удалить просроченную запись из Google Sheets: %s", exc)

    async def _download_photo_data_url(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        attachment: MediaAttachment,
    ) -> Optional[str]:
        if attachment.kind != "photo":
            return None
        bot = getattr(context, "bot", None)
        if bot is None:
            return None
        try:
            telegram_file = await bot.get_file(attachment.file_id)
        except Exception as exc:  # pragma: no cover - network dependent
            LOGGER.warning("Не удалось получить файл оплаты: %s", exc)
            return None
        try:
            payload = await telegram_file.download_as_bytearray()
        except Exception as exc:  # pragma: no cover - network dependent
            LOGGER.warning("Не удалось скачать файл оплаты: %s", exc)
            return None
        data = bytes(payload)
        mime_type = getattr(telegram_file, "mime_type", None)
        if not mime_type:
            file_path = getattr(telegram_file, "file_path", "")
            mime_type = mimetypes.guess_type(file_path)[0] or "image/jpeg"
        encoded = base64.b64encode(data).decode("ascii")
        return f"data:{mime_type};base64,{encoded}"

    async def _remove_registration_for_cancellation(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        cancellation: dict[str, Any],
    ) -> Optional[dict[str, Any]]:
        registrations = self._application_data(context).get("registrations")
        if not isinstance(registrations, list):
            return None

        target_id = cancellation.get("registration_id")
        target_id_str = str(target_id) if target_id is not None else None

        match_index: Optional[int] = None
        if target_id_str:
            for index in range(len(registrations) - 1, -1, -1):
                candidate = registrations[index]
                if not isinstance(candidate, dict):
                    continue
                if str(candidate.get("id")) == target_id_str:
                    match_index = index
                    break

        if match_index is None:
            chat_id = cancellation.get("chat_id")
            user_id = cancellation.get("submitted_by_id")
            program = cancellation.get("program")
            time_value = cancellation.get("time")
            for index in range(len(registrations) - 1, -1, -1):
                candidate = registrations[index]
                if not isinstance(candidate, dict):
                    continue
                if chat_id is not None and candidate.get("chat_id") != chat_id:
                    continue
                if user_id is not None and candidate.get("submitted_by_id") != user_id:
                    continue
                if program and candidate.get("program") != program:
                    continue
                if time_value and candidate.get("time") != time_value:
                    continue
                match_index = index
                break

        if match_index is None:
            return None

        removed = registrations.pop(match_index)
        self._remove_user_registration_snapshot(removed)

        backend = self._sheets_backend
        registration_id = removed.get("id")
        if backend is not None and registration_id:
            try:
                await backend.delete_registration(str(registration_id))
            except Exception as exc:  # pragma: no cover - network dependent
                LOGGER.warning("Не удалось удалить запись из Google Sheets: %s", exc)

        return removed

    def _describe_attachment(self, attachment: MediaAttachment) -> str:
        labels = {
            "photo": "Фото",
            "video": "Видео",
            "animation": "GIF",
            "document": "Файл",
            "video_note": "Видео-заметка",
            "audio": "Аудио",
            "voice": "Голос",
        }
        title = labels.get(attachment.kind, attachment.kind or "Вложение")
        return f"{title}: {attachment.file_id}"

    async def _store_cancellation(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        data: dict[str, Any],
        attachments: Optional[list[MediaAttachment]] = None,
    ) -> None:
        chat = update.effective_chat
        user = update.effective_user
        record = {
            "program": data.get("program", ""),
            "time": data.get("time", ""),
            "child_name": data.get("child_name", ""),
            "registration_id": data.get("registration_id"),
            "details": data.get("details", ""),
            "chat_id": _coerce_chat_id_from_object(chat) if chat else None,
            "submitted_by": getattr(user, "full_name", None) if user else None,
            "submitted_by_id": getattr(user, "id", None) if user else None,
            "created_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
            "attachments": self._attachments_to_dicts(attachments or [])
            if attachments
            else data.get("evidence", []),
        }
        storage = self._application_data(context).setdefault("cancellations", [])
        if isinstance(storage, list):
            storage.append(record)
        else:
            self._application_data(context)["cancellations"] = [record]

        removed = await self._remove_registration_for_cancellation(context, record)
        if removed:
            record["removed_registration_id"] = removed.get("id")
            record["removed_child"] = removed.get("child_name")
            record["removed_program"] = removed.get("program")
            record["removed_time"] = removed.get("time")

        self._save_persistent_state()

        admin_message = (
            "🚫 Отмена занятия\n"
            f"📚 Программа: {record.get('program', '—')}\n"
            f"🕒 Время: {record.get('time', '—')}\n"
            f"👦 Участник: {record.get('child_name', '—')}\n"
            f"📝 Комментарий: {record.get('details', '—')}\n"
            f"👤 Отправил: {record.get('submitted_by', '—')}"
        )
        if removed:
            admin_message += (
                "\n🗂 Заявка удалена из таблицы: "
                f"{removed.get('child_name', '—')} ({removed.get('program', '—')}, {removed.get('time', '—')})"
            )
        else:
            admin_message += "\n⚠️ В таблице не нашлось записи, соответствующей этой отмене."
        await self._notify_admins(
            context,
            admin_message,
            media=self._dicts_to_attachments(record.get("attachments")),
        )
        context.user_data.pop("cancellation", None)

    async def _start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Send the greeting and display the main menu."""

        self._remember_chat(update, context)

        args = context.args if context.args is not None else []
        if args:
            payload = args[0]
            if payload == "registrations_excel":
                if not self._is_admin_update(update, context):
                    await self._reply(
                        update,
                        "Этот раздел доступен только администраторам.",
                        reply_markup=self._main_menu_markup_for(update, context),
                    )
                    return
                sent = await self._send_registrations_excel(update, context)
                if sent:
                    await self._reply(
                        update,
                        "Экспорт завершён. Таблица отправлена сообщением выше.",
                        reply_markup=self._admin_menu_markup(),
                    )
                return

        await self._send_greeting(update, context)

    async def _show_main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Show the menu without repeating the full greeting."""

        self._remember_chat(update, context)
        message = (
            "👉 Veuillez choisir une rubrique dans le menu ci-dessous.\n"
            "👉 Пожалуйста, выберите раздел в меню ниже."
        )
        if self._is_admin_update(update, context):
            message += "\n\n🛠 Для управления ботом откройте «Админ-панель» в меню."
        await self._reply(update, message, reply_markup=self._main_menu_markup_for(update, context))

    async def _show_admin_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._is_admin_update(update, context):
            await self._reply(
                update,
                "Эта панель доступна только администраторам.",
                reply_markup=self._main_menu_markup_for(update, context),
            )
            return
        self._remember_chat(update, context)
        message = "Админ-панель открыта. Выберите действие ниже."
        await self._reply(update, message, reply_markup=self._admin_menu_markup())

    async def _send_greeting(self, update: Update, context: Optional[ContextTypes.DEFAULT_TYPE] = None) -> None:
        greeting = (
            "🎉 🇫🇷 Bonjour et bienvenue dans la compagnie «Confetti» !\n"
            "🎉 🇷🇺 Здравствуйте и добро пожаловать в студию «Конфетти»!\n\n"
            "Nous adorons la France et le français — et nous sommes prêts à partager cet amour à chacun.\n\n"
            "Мы обожаем Францию и французский — и готовы делиться этой любовью с каждым.\n\n"
            "👉 Veuillez choisir une rubrique dans le menu ci-dessous.\n"
            "👉 Пожалуйста, выберите раздел в меню ниже."
        )
        if self._is_admin_update(update, context):
            greeting += "\n\n🛠 У вас есть доступ к админ-панели — нажмите кнопку ниже, чтобы управлять контентом."
        await self._reply(update, greeting, reply_markup=self._main_menu_markup_for(update, context))

    async def _reply(
        self,
        update: Update,
        text: Optional[str] = None,
        *,
        reply_markup: Optional[
            ReplyKeyboardMarkup | ReplyKeyboardRemove | InlineKeyboardMarkup
        ] = None,
        media: Optional[list[MediaAttachment]] = None,
        prefer_edit: bool = False,
    ) -> None:
        message = update.message
        callback = update.callback_query
        target = message or (callback.message if callback else None)

        if callback:
            try:
                await callback.answer()
            except Exception as exc:  # pragma: no cover - network/runtime specific
                LOGGER.debug("Unable to answer callback query: %s", exc)

        markup_used = False
        inline_markup = reply_markup if reply_markup and hasattr(reply_markup, "inline_keyboard") else None

        if (
            prefer_edit
            and callback
            and callback.message is not None
            and (inline_markup is not None or reply_markup is None)
        ):
            try:
                if text is not None:
                    await callback.message.edit_text(text, reply_markup=inline_markup)
                    markup_used = inline_markup is not None
                    text = None
                elif inline_markup is not None:
                    await callback.message.edit_reply_markup(inline_markup)
                    markup_used = True
            except Exception as exc:  # pragma: no cover - Telegram runtime dependent
                LOGGER.debug("Failed to edit callback message: %s", exc)
            else:
                target = callback.message

        if text:
            if target is not None:
                await target.reply_text(text, reply_markup=reply_markup)
                markup_used = True
        if media and target is not None:
            for index, attachment in enumerate(media):
                extra: dict[str, Any] = {}
                if not markup_used and reply_markup is not None and index == 0:
                    extra["reply_markup"] = reply_markup
                    markup_used = True
                if attachment.caption:
                    extra["caption"] = attachment.caption
                try:
                    if attachment.kind == "photo":
                        await target.reply_photo(attachment.file_id, **extra)
                    elif attachment.kind == "video":
                        await target.reply_video(attachment.file_id, **extra)
                    elif attachment.kind == "animation":
                        await target.reply_animation(attachment.file_id, **extra)
                    elif attachment.kind == "document":
                        await target.reply_document(attachment.file_id, **extra)
                    elif attachment.kind == "video_note":
                        await target.reply_video_note(attachment.file_id)
                    else:
                        LOGGER.debug("Unsupported media type %s", attachment.kind)
                except Exception as exc:  # pragma: no cover - network dependent
                    LOGGER.warning("Failed to reply with media %s: %s", attachment.kind, exc)
        elif reply_markup is not None and not markup_used and target is not None:
            await target.reply_text("", reply_markup=reply_markup)

    def _extract_message_payload(self, message: Any | None) -> tuple[str, list[MediaAttachment]]:
        """Return the plain text and media attachments contained in ``message``."""

        if message is None:
            return "", []

        text = (getattr(message, "text", None) or "").strip()
        caption = (getattr(message, "caption", None) or "").strip()
        remaining_caption = caption or None
        attachments: list[MediaAttachment] = []

        def push(kind: str, file_id: str) -> None:
            nonlocal remaining_caption
            attachments.append(
                MediaAttachment(kind=kind, file_id=file_id, caption=remaining_caption)
            )
            remaining_caption = None

        photos = getattr(message, "photo", None)
        if photos:
            try:
                best_photo = max(photos, key=lambda item: getattr(item, "file_size", 0) or 0)
            except ValueError:
                best_photo = photos[-1]
            push("photo", best_photo.file_id)

        video = getattr(message, "video", None)
        if video:
            push("video", video.file_id)

        animation = getattr(message, "animation", None)
        if animation:
            push("animation", animation.file_id)

        document = getattr(message, "document", None)
        if document:
            push("document", document.file_id)

        video_note = getattr(message, "video_note", None)
        if video_note:
            attachments.append(MediaAttachment(kind="video_note", file_id=video_note.file_id))

        return text, attachments

    async def _send_payload_to_chat(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        chat_id: int,
        *,
        text: Optional[str] = None,
        media: Optional[list[MediaAttachment]] = None,
        reply_markup: Optional[ReplyKeyboardMarkup | ReplyKeyboardRemove] = None,
    ) -> None:
        if text:
            await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)
            reply_markup = None
        if not media:
            return
        for index, attachment in enumerate(media):
            extra: dict[str, Any] = {}
            if attachment.caption:
                extra["caption"] = attachment.caption
            if reply_markup is not None and index == 0:
                extra["reply_markup"] = reply_markup
            try:
                if attachment.kind == "photo":
                    await context.bot.send_photo(chat_id=chat_id, photo=attachment.file_id, **extra)
                elif attachment.kind == "video":
                    await context.bot.send_video(chat_id=chat_id, video=attachment.file_id, **extra)
                elif attachment.kind == "animation":
                    await context.bot.send_animation(chat_id=chat_id, animation=attachment.file_id, **extra)
                elif attachment.kind == "document":
                    await context.bot.send_document(chat_id=chat_id, document=attachment.file_id, **extra)
                elif attachment.kind == "video_note":
                    await context.bot.send_video_note(chat_id=chat_id, video_note=attachment.file_id)
                else:
                    LOGGER.debug("Unsupported media type %s for broadcast", attachment.kind)
            except Exception as exc:  # pragma: no cover - network dependent
                LOGGER.warning("Failed to deliver media %s to %s: %s", attachment.kind, chat_id, exc)

    async def _notify_admins(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        text: str,
        *,
        media: Optional[list[MediaAttachment]] = None,
    ) -> None:
        recipients = set(self._runtime_admin_ids)
        recipients.update(self._refresh_admin_cache(context))
        for admin_id in sorted(recipients):
            try:
                await self._send_payload_to_chat(context, admin_id, text=text, media=media)
            except Exception as exc:  # pragma: no cover - network dependent
                LOGGER.warning("Failed to notify admin %s: %s", admin_id, exc)

    def _attachments_to_dicts(self, attachments: list[MediaAttachment]) -> list[dict[str, str]]:
        serialised: list[dict[str, str]] = []
        for attachment in attachments:
            serialised.append(
                {
                    "kind": attachment.kind,
                    "file_id": attachment.file_id,
                    "caption": attachment.caption or "",
                }
            )
        return serialised

    def _dicts_to_attachments(self, payload: Any) -> list[MediaAttachment]:
        attachments: list[MediaAttachment] = []
        if not isinstance(payload, list):
            return attachments
        for entry in payload:
            if not isinstance(entry, dict):
                continue
            kind = entry.get("kind")
            file_id = entry.get("file_id")
            if not kind or not file_id:
                continue
            caption = entry.get("caption") or None
            attachments.append(MediaAttachment(kind=kind, file_id=file_id, caption=caption))
        return attachments

    # ------------------------------------------------------------------
    # Registration conversation

    def _registration_program_prompt(self) -> str:
        return (
            "На какую программу вы хотите записать ребёнка или себя?\n"
            "Нажмите на кнопку ниже, чтобы выбрать вариант и посмотреть подробности."
        )

    async def _start_registration(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        self._remember_chat(update, context)
        await self._purge_expired_registrations(context)
        context.user_data["registration"] = {}
        await self._reply(
            update,
            self._registration_program_prompt(),
            reply_markup=self._program_inline_keyboard(),
            prefer_edit=update.callback_query is not None,
        )
        return self.REGISTRATION_PROGRAM

    def _program_inline_keyboard(self) -> "InlineKeyboardMarkup":
        buttons = [
            [InlineKeyboardButton(program["label"], callback_data=f"reg_program:{index}")]
            for index, program in enumerate(self.PROGRAMS)
        ]
        buttons.append([InlineKeyboardButton(self.BACK_BUTTON, callback_data="reg_back:menu")])
        return InlineKeyboardMarkup(buttons)

    def _teacher_inline_keyboard(self) -> "InlineKeyboardMarkup":
        buttons = [
            [InlineKeyboardButton(teacher["name"], callback_data=f"teacher:{teacher['key']}")]
            for teacher in self.TEACHERS
        ]
        return InlineKeyboardMarkup(buttons)

    def _format_program_details(self, program: Dict[str, str]) -> str:
        parts = [program["label"]]
        for key in ("audience", "teacher", "schedule"):
            if value := program.get(key):
                parts.append(value)
        return "\n".join(parts)

    async def _registration_prompt_program_buttons(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        await self._reply(
            update,
            self._registration_program_prompt(),
            reply_markup=self._program_inline_keyboard(),
            prefer_edit=update.callback_query is not None,
        )
        return self.REGISTRATION_PROGRAM

    async def _registration_collect_program(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        query = update.callback_query
        message = update.message

        program_label = ""
        if query is not None:
            data = query.data or ""
            try:
                index = int(data.split(":", 1)[1])
            except (IndexError, ValueError):
                await query.answer("Не удалось определить программу.", show_alert=True)
                return self.REGISTRATION_PROGRAM
            if not 0 <= index < len(self.PROGRAMS):
                await query.answer("Программа недоступна.", show_alert=True)
                return self.REGISTRATION_PROGRAM
            program = self.PROGRAMS[index]
            await query.answer()
            program_label = program["label"]
            details = self._format_program_details(program)
            if query.message is not None:
                try:  # pragma: no cover - depends on telegram runtime
                    await query.edit_message_text(f"Вы выбрали программу:\n{details}")
                except Exception:
                    try:
                        await query.edit_message_reply_markup(None)
                    except Exception:
                        pass
                    await self._reply(update, f"Вы выбрали программу:\n{details}")
            else:
                await self._reply(update, f"Вы выбрали программу:\n{details}")
        else:
            program_label = (message.text if message else "").strip()
            program = next((item for item in self.PROGRAMS if item["label"] == program_label), None)
            if not program:
                await self._registration_prompt_program_buttons(update, context)
                return self.REGISTRATION_PROGRAM

        registration = context.user_data.setdefault("registration", {})
        registration["program"] = program_label

        defaults = self._get_user_defaults(update.effective_user)
        if defaults:
            for key in ("child_name", "class", "phone"):
                value = defaults.get(key)
                if value:
                    registration[key] = value

        saved_time = ""
        user_records = self._collect_user_registrations(update.effective_user, update.effective_chat)
        for record in reversed(user_records):
            if record.get("program") == program_label and record.get("time"):
                saved_time = str(record.get("time"))
                break
        if not saved_time and defaults:
            saved_time = str(defaults.get("time", "") or "")
        if saved_time:
            registration["saved_time"] = saved_time
            registration["saved_time_original"] = saved_time
        else:
            registration.pop("saved_time_original", None)
            registration["saved_time"] = saved_time

        if not registration.get("child_name"):
            return await self._registration_prompt_child_name(update, context)

        if not registration.get("class"):
            return await self._registration_prompt_class(update, context, remind=True)

        if not registration.get("phone"):
            return await self._registration_prompt_phone(update, context, remind=True)

        return await self._registration_show_saved_details_prompt(update, context)

    async def _registration_prompt_child_name(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, *, remind: bool = False
    ) -> int:
        registration = context.user_data.setdefault("registration", {})
        if remind and registration.get("child_name"):
            message = (
                f"Сейчас указано имя: {registration.get('child_name', '—')}.\n"
                "Введите новое имя и фамилию ребёнка."
            )
        else:
            message = "Отлично! Напишите, пожалуйста, имя и фамилию ребёнка."
        await self._reply(update, message, reply_markup=self._back_keyboard())
        return self.REGISTRATION_CHILD_NAME

    async def _registration_prompt_class(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, *, remind: bool = False
    ) -> int:
        registration = context.user_data.setdefault("registration", {})
        child_name = registration.get("child_name", "—")
        if remind and registration.get("class"):
            message = (
                f"Имя участника: {child_name}.\n"
                f"Текущий класс: {registration.get('class', '—')}.\n"
                "Укажите актуальный класс."
            )
        else:
            message = f"Мы сохранили имя: {child_name}.\nУкажите, пожалуйста, класс."
        await self._reply(update, message, reply_markup=self._back_keyboard())
        return self.REGISTRATION_CLASS

    async def _registration_prompt_phone(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, *, remind: bool = False
    ) -> int:
        registration = context.user_data.setdefault("registration", {})
        child_name = registration.get("child_name", "—")
        child_class = registration.get("class", "—")
        if remind and registration.get("phone"):
            message = (
                f"Имя и класс: {child_name} ({child_class}).\n"
                f"Сейчас указан номер: {registration.get('phone', '—')}.\n"
                "Введите номер телефона вручную."
            )
        else:
            message = (
                f"Мы сохранили имя и класс: {child_name} ({child_class}).\n"
                "Введите номер телефона вручную."
            )
        await self._reply(update, message, reply_markup=self._phone_keyboard())
        return self.REGISTRATION_PHONE

    async def _registration_show_saved_details_prompt(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        registration = context.user_data.setdefault("registration", {})
        message = (
            "Мы заполнили данные из вашей предыдущей заявки:\n"
            f"👦 Имя: {registration.get('child_name', '—')} ({registration.get('class', '—')})\n"
            f"📱 Телефон: {registration.get('phone', '—')}\n\n"
            "Нажмите «Продолжить», если всё верно, или «Изменить данные», чтобы указать новые значения."
        )
        await self._reply(update, message, reply_markup=self._saved_details_keyboard())
        return self.REGISTRATION_CONFIRM_DETAILS

    async def _registration_cancel_from_program(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        query = update.callback_query
        if query is not None:
            await query.answer()
            if query.message is not None:
                try:  # pragma: no cover - depends on telegram runtime
                    await query.edit_message_text("Регистрация прервана.")
                except Exception:
                    pass
        context.user_data.pop("registration", None)
        await self._show_main_menu(update, context)
        return ConversationHandler.END

    async def _registration_back_to_program(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        registration = context.user_data.setdefault("registration", {})
        for key in ("program", "time", "saved_time", "saved_time_original", "proposed_time"):
            registration.pop(key, None)
        await self._reply(
            update,
            self._registration_program_prompt(),
            reply_markup=self._program_inline_keyboard(),
        )
        return self.REGISTRATION_PROGRAM

    async def _registration_back_to_child_name(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        return await self._registration_prompt_child_name(update, context, remind=True)

    async def _registration_back_from_confirm(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        return await self._registration_prompt_phone(update, context, remind=True)

    async def _registration_back_from_time_decision(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        registration = context.user_data.setdefault("registration", {})
        registration.pop("proposed_time", None)
        return await self._registration_show_saved_details_prompt(update, context)

    async def _registration_back_from_time(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        registration = context.user_data.setdefault("registration", {})
        registration.pop("time", None)
        if registration.get("saved_time_original"):
            registration["saved_time"] = registration["saved_time_original"]
            return await self._prompt_time_of_day(update, context)
        return await self._prompt_time_selection(update)

    async def _registration_back_to_time(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        registration = context.user_data.setdefault("registration", {})
        registration.pop("payment_media", None)
        registration.pop("payment_note", None)
        return await self._registration_back_from_time(update, context)

    async def _registration_collect_child_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        text = (update.message.text or "").strip()
        if text == self.MAIN_MENU_BUTTON:
            return await self._registration_cancel(update, context)
        if text == self.BACK_BUTTON:
            return await self._registration_back_to_program(update, context)
        context.user_data.setdefault("registration", {})["child_name"] = text
        return await self._registration_prompt_class(update, context)

    async def _registration_collect_class(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        text = (update.message.text or "").strip()
        if text == self.MAIN_MENU_BUTTON:
            return await self._registration_cancel(update, context)
        if text == self.BACK_BUTTON:
            return await self._registration_prompt_child_name(update, context, remind=True)
        context.user_data.setdefault("registration", {})["class"] = text
        return await self._registration_prompt_phone(update, context)

    def _back_keyboard(self, *, include_menu: bool = True) -> ReplyKeyboardMarkup:
        row = [KeyboardButton(self.BACK_BUTTON)]
        if include_menu:
            row.append(KeyboardButton(self.MAIN_MENU_BUTTON))
        return ReplyKeyboardMarkup([row], resize_keyboard=True, one_time_keyboard=True)

    def _phone_keyboard(self) -> ReplyKeyboardMarkup:
        return self._back_keyboard()

    def _admin_action_keyboard(self) -> ReplyKeyboardMarkup:
        keyboard = [
            [KeyboardButton(self.BACK_BUTTON), KeyboardButton(self.ADMIN_MENU_BUTTON)],
            [KeyboardButton(self.MAIN_MENU_BUTTON)],
        ]
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)

    def _saved_details_keyboard(self) -> ReplyKeyboardMarkup:
        keyboard = [
            [KeyboardButton(self.REGISTRATION_CONFIRM_SAVED_BUTTON)],
            [KeyboardButton(self.REGISTRATION_EDIT_DETAILS_BUTTON)],
            [KeyboardButton(self.BACK_BUTTON), KeyboardButton(self.MAIN_MENU_BUTTON)],
        ]
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)

    def _payment_keyboard(self) -> ReplyKeyboardMarkup:
        keyboard = [
            [KeyboardButton(self.REGISTRATION_SKIP_PAYMENT_BUTTON)],
            [KeyboardButton(self.BACK_BUTTON), KeyboardButton(self.MAIN_MENU_BUTTON)],
        ]
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)

    def _saved_time_keyboard(self) -> ReplyKeyboardMarkup:
        keyboard = [
            [KeyboardButton(self.REGISTRATION_KEEP_TIME_BUTTON)],
            [KeyboardButton(self.REGISTRATION_NEW_TIME_BUTTON)],
            [KeyboardButton(self.BACK_BUTTON), KeyboardButton(self.MAIN_MENU_BUTTON)],
        ]
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)

    def _cancellation_keyboard(self, labels: list[str]) -> ReplyKeyboardMarkup:
        keyboard = [[label] for label in labels]
        keyboard.append([self.BACK_BUTTON, self.MAIN_MENU_BUTTON])
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)

    async def _registration_collect_phone_text(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        text = update.message.text.strip()
        if text == self.MAIN_MENU_BUTTON:
            return await self._registration_cancel(update, context)
        if text == self.BACK_BUTTON:
            return await self._registration_prompt_class(update, context, remind=True)
        context.user_data.setdefault("registration", {})["phone"] = text
        return await self._prompt_time_of_day(update, context)

    async def _registration_accept_saved_details(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        return await self._prompt_time_of_day(update, context)

    async def _registration_use_saved_time(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        registration = context.user_data.setdefault("registration", {})
        saved_time = str(
            registration.get("proposed_time")
            or registration.get("saved_time")
            or registration.get("time", "")
        ).strip()
        if not saved_time:
            return await self._registration_request_new_time(update, context)
        registration["time"] = saved_time
        registration.setdefault("saved_time_original", saved_time)
        registration.pop("saved_time", None)
        registration.pop("proposed_time", None)
        return await self._prompt_payment_request(update, context)

    async def _registration_request_new_time(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        registration = context.user_data.setdefault("registration", {})
        original = registration.get("saved_time") or registration.get("saved_time_original")
        if original:
            registration["saved_time_original"] = original
        registration.pop("proposed_time", None)
        registration.pop("saved_time", None)
        return await self._prompt_time_selection(update)

    async def _registration_request_details_update(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        registration = context.user_data.setdefault("registration", {})
        for key in ("child_name", "class", "phone"):
            registration.pop(key, None)
        return await self._registration_prompt_child_name(update, context, remind=True)

    async def _prompt_time_of_day(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        registration = context.user_data.setdefault("registration", {})
        saved_time = str(registration.get("saved_time", "")).strip()
        if saved_time:
            registration.setdefault("saved_time_original", saved_time)
            registration["proposed_time"] = saved_time
            message = (
                "⏱️ Ранее вы выбирали время: "
                f"{saved_time}.\n"
                "🔁 Нажмите «То же время», чтобы оставить его, или «Другое время», чтобы выбрать новый слот."
            )
            await self._reply(
                update,
                message,
                reply_markup=self._saved_time_keyboard(),
            )
            return self.REGISTRATION_TIME_DECISION
        return await self._prompt_time_selection(update)

    async def _prompt_time_selection(self, update: Update) -> int:
        await self._reply(
            update,
            "Выберите удобное время занятий.",
            reply_markup=self._time_keyboard(),
        )
        return self.REGISTRATION_TIME

    def _time_keyboard(self) -> ReplyKeyboardMarkup:
        keyboard = [[option] for option in self.TIME_OF_DAY_OPTIONS]
        keyboard.append([self.BACK_BUTTON, self.MAIN_MENU_BUTTON])
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)

    async def _registration_collect_time(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        text = (update.message.text or "").strip()
        if text == self.MAIN_MENU_BUTTON:
            return await self._registration_cancel(update, context)
        if text == self.BACK_BUTTON:
            return await self._registration_back_from_time(update, context)
        registration = context.user_data.setdefault("registration", {})
        registration["time"] = text
        if not registration.get("saved_time_original"):
            registration["saved_time_original"] = text
        registration.pop("saved_time", None)
        registration.pop("proposed_time", None)
        return await self._prompt_payment_request(update, context)

    async def _prompt_payment_request(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        instructions = self._get_content(context).payment
        message = (
            "💳 Отправьте подтверждение оплаты (фото, видео или файл).\n\n"
            "➡️ Если оплаты ещё нет, нажмите «⏭ Пока без оплаты» и мы свяжемся с вами позже."
        )
        if instructions.text:
            message += "\n\n" + instructions.text
        await self._reply(
            update,
            message,
            reply_markup=self._payment_keyboard(),
            media=instructions.media or None,
        )
        return self.REGISTRATION_PAYMENT

    async def _registration_collect_payment(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        data = context.user_data.setdefault("registration", {})
        text, attachments = self._extract_message_payload(update.message)

        if text == self.MAIN_MENU_BUTTON:
            return await self._registration_cancel(update, context)

        if text == self.BACK_BUTTON:
            return await self._registration_back_to_time(update, context)

        if text == self.REGISTRATION_SKIP_PAYMENT_BUTTON:
            data["payment_note"] = "Платёж будет подтверждён позже"
            data.pop("payment_media", None)
            await self._send_registration_summary(update, context, media=None)
            await self._show_main_menu(update, context)
            return ConversationHandler.END

        if attachments:
            data["payment_media"] = self._attachments_to_dicts(attachments)
        if text:
            data["payment_note"] = text

        await self._send_registration_summary(update, context, media=attachments or None)
        await self._show_main_menu(update, context)
        return ConversationHandler.END

    async def _registration_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        context.user_data.pop("registration", None)
        await self._reply(
            update,
            "❌ Регистрация отменена.",
            reply_markup=self._main_menu_markup_for(update, context),
        )
        return ConversationHandler.END

    # ------------------------------------------------------------------
    # Cancellation conversation

    async def _start_cancellation(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        self._remember_chat(update, context)
        await self._purge_expired_registrations(context)
        records = self._collect_user_registrations(update.effective_user, update.effective_chat)
        if not records:
            await self._reply(
                update,
                "ℹ️ Активных записей не найдено.",
                reply_markup=self._main_menu_markup_for(update, context),
            )
            await self._show_main_menu(update, context)
            return ConversationHandler.END

        sorted_records = sorted(
            records,
            key=lambda item: self._parse_record_timestamp(item.get("created_at")) or datetime.min,
            reverse=True,
        )
        options: dict[str, dict[str, Any]] = {}
        counts: dict[str, int] = {}
        for record in sorted_records:
            base_label = self._format_cancellation_option(record)
            index = counts.get(base_label, 0)
            counts[base_label] = index + 1
            label = base_label if index == 0 else f"{base_label} ({index + 1})"
            options[label] = record

        context.user_data["cancellation"] = {"options": options}
        message = (
            "❗️ Выберите занятие, которое хотите отменить.\n\n"
            "⚠️ Оплата не возвращается — средства остаются на балансе студии."
        )
        await self._reply(
            update,
            message,
            reply_markup=self._cancellation_keyboard(list(options.keys())),
        )
        return self.CANCELLATION_PROGRAM

    def _format_cancellation_option(self, record: dict[str, Any]) -> str:
        program = str(record.get("program", "")) or "Без программы"
        time = str(record.get("time", ""))
        child = str(record.get("child_name", ""))
        record_id = str(record.get("id", ""))
        suffix = f"#{record_id[-4:]}" if record_id else ""
        components = [program]
        if time:
            components.append(time)
        if child:
            components.append(child)
        if suffix:
            components.append(suffix)
        return " • ".join(components)

    async def _cancellation_collect_program(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        payload = update.message.text.strip()
        if payload == self.MAIN_MENU_BUTTON:
            return await self._cancellation_cancel(update, context)
        if payload == self.BACK_BUTTON:
            return await self._cancellation_cancel(update, context)

        data = context.user_data.setdefault("cancellation", {})
        options: dict[str, dict[str, Any]] = data.get("options", {})  # type: ignore[assignment]
        record = options.get(payload)
        if record is None:
            await self._reply(
                update,
                "Пожалуйста, выберите запись из списка.",
                reply_markup=self._cancellation_keyboard(list(options.keys())),
            )
            return self.CANCELLATION_PROGRAM

        data["selected_registration"] = record
        data["program"] = record.get("program", "")
        data["time"] = record.get("time", "")
        data["child_name"] = record.get("child_name", "")
        data["registration_id"] = record.get("id")
        await self._reply(
            update,
            "📅 Напишите дату и время пропуска, а также короткий комментарий.",
            reply_markup=self._back_keyboard(),
        )
        return self.CANCELLATION_REASON

    async def _cancellation_restart_program(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        options: dict[str, dict[str, Any]],
    ) -> int:
        data = context.user_data.setdefault("cancellation", {})
        data.pop("details", None)
        data.pop("evidence", None)
        message = (
            "❗️ Выберите занятие, которое хотите отменить.\n\n"
            "⚠️ Оплата не возвращается — средства остаются на балансе студии."
        )
        await self._reply(
            update,
            message,
            reply_markup=self._cancellation_keyboard(list(options.keys())),
        )
        return self.CANCELLATION_PROGRAM

    async def _cancellation_collect_reason(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        data = context.user_data.setdefault("cancellation", {})
        text, attachments = self._extract_message_payload(update.message)

        if text == self.MAIN_MENU_BUTTON:
            return await self._cancellation_cancel(update, context)
        if text == self.BACK_BUTTON:
            options: dict[str, dict[str, Any]] = data.get("options", {})  # type: ignore[assignment]
            return await self._cancellation_restart_program(update, context, options)

        if attachments:
            data["evidence"] = self._attachments_to_dicts(attachments)
        data["details"] = text or ""

        await self._store_cancellation(update, context, data, attachments or None)
        context.user_data.pop("cancellation", None)

        confirmation = (
            "✅ Отмена зафиксирована.\n"
            "ℹ️ Средства за пропущенное занятие не возвращаются, но мы учли ваш комментарий."
        )
        await self._reply(
            update,
            confirmation,
            reply_markup=self._main_menu_markup_for(update, context),
        )
        await self._show_main_menu(update, context)
        return ConversationHandler.END

    async def _cancellation_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        context.user_data.pop("cancellation", None)
        await self._reply(
            update,
            "Отмена занятия не отправлена.",
            reply_markup=self._main_menu_markup_for(update, context),
        )
        return ConversationHandler.END

    async def _send_registration_summary(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        *,
        media: Optional[list[MediaAttachment]] = None,
    ) -> None:
        data = context.user_data.get("registration", {})
        attachments = media or self._dicts_to_attachments(data.get("payment_media"))
        payment_note = data.get("payment_note")
        payment_status = "✅ Оплата подтверждена" if attachments else "⏳ Оплата ожидается"

        summary = (
            "Ваша заявка принята!\n\n"
            f"👦 Участник: {data.get('child_name', '—')} ({data.get('class', '—')})\n"
            f"📱 Телефон: {data.get('phone', '—')}\n"
            f"🕒 Время: {data.get('time', '—')}\n"
            f"📚 Программа: {data.get('program', '—')}\n"
            f"💳 {payment_status}\n"
        )
        if payment_note:
            summary += f"📝 Комментарий: {payment_note}\n"
        summary += "\nМы свяжемся с вами в ближайшее время."

        await self._reply(update, summary, reply_markup=self._main_menu_markup_for(update, context))
        record = self._store_registration(update, context, data, attachments)
        await self._sync_registration_sheet(context, record, attachments or None)

        admin_message = (
            "🆕 Новая заявка\n"
            f"📚 Программа: {data.get('program', '—')}\n"
            f"👦 Участник: {data.get('child_name', '—')} ({data.get('class', '—')})\n"
            f"📱 Телефон: {data.get('phone', '—')}\n"
            f"🕒 Время: {data.get('time', '—')}\n"
            f"💳 Статус оплаты: {'получено' if attachments else 'ожидается'}"
        )
        if payment_note:
            admin_message += f"\n📝 Комментарий: {payment_note}"

        await self._notify_admins(context, admin_message, media=attachments or None)
        context.user_data.pop("registration", None)

    # ------------------------------------------------------------------
    # Menu handlers

    async def _handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.message
        if message is None:
            return

        self._remember_chat(update, context)

        text, attachments = self._extract_message_payload(message)

        if text == self.MAIN_MENU_BUTTON:
            context.chat_data.pop("pending_admin_action", None)
            await self._show_main_menu(update, context)
            return

        profile = self.build_profile(update.effective_chat, update.effective_user)
        pending = context.chat_data.get("pending_admin_action")

        if pending and profile.is_admin:
            trimmed = text.strip() if text else ""
            lowered = trimmed.lower()

            if trimmed == self.BACK_BUTTON or lowered in self._admin_cancel_tokens:
                context.chat_data.pop("pending_admin_action", None)
                await self._reply(
                    update,
                    "Действие отменено.\n",
                    reply_markup=self._admin_menu_markup(),
                )
                return

            if trimmed == self.ADMIN_MENU_BUTTON:
                context.chat_data.pop("pending_admin_action", None)
                await self._show_admin_menu(update, context)
                return

            if trimmed == self.ADMIN_BACK_TO_USER_BUTTON:
                context.chat_data.pop("pending_admin_action", None)
                await self._show_main_menu(update, context)
                return

            context.chat_data.pop("pending_admin_action", None)
            await self._dispatch_admin_action(
                update,
                context,
                pending,
                text=text,
                attachments=attachments,
            )
            return

        if profile.is_admin and text:
            command_text = text.strip()
            if command_text == self.ADMIN_MENU_BUTTON:
                await self._show_admin_menu(update, context)
                return
            if command_text == self.ADMIN_BACK_TO_USER_BUTTON:
                await self._show_main_menu(update, context)
                return
            if command_text == self.ADMIN_BROADCAST_BUTTON:
                context.chat_data["pending_admin_action"] = {"type": "broadcast"}
                await self._reply(
                    update,
                    "Отправьте сообщение или медиа для рассылки."
                    + self.ADMIN_CANCEL_PROMPT,
                    reply_markup=self._admin_action_keyboard(),
                )
                return
            if command_text == self.ADMIN_EXPORT_TABLE_BUTTON:
                await self._admin_share_registrations_table(update, context)
                return
            if command_text == self.ADMIN_ADD_ADMIN_BUTTON:
                context.chat_data["pending_admin_action"] = {"type": "add_admin"}
                await self._reply(
                    update,
                    "Введите chat_id нового администратора.\n"
                    + self.ADMIN_CANCEL_PROMPT,
                    reply_markup=self._admin_action_keyboard(),
                )
                return
            if command_text == self.ADMIN_EDIT_SCHEDULE_BUTTON:
                await self._prompt_admin_content_edit(
                    update,
                    context,
                    field="schedule",
                    instruction="Отправьте текст и вложения нового расписания."
                    + self.ADMIN_CANCEL_PROMPT,
                )
                return
            if command_text == self.ADMIN_EDIT_ABOUT_BUTTON:
                await self._prompt_admin_content_edit(
                    update,
                    context,
                    field="about",
                    instruction="Отправьте обновлённый блок «О студии» (текст, фото, видео)."
                    + self.ADMIN_CANCEL_PROMPT,
                )
                return
            if command_text == self.ADMIN_EDIT_TEACHERS_BUTTON:
                await self._prompt_admin_content_edit(
                    update,
                    context,
                    field="teachers",
                    instruction="Поделитесь новым описанием преподавателей и медиа."
                    + self.ADMIN_CANCEL_PROMPT,
                )
                return
            if command_text == self.ADMIN_EDIT_ALBUM_BUTTON:
                await self._prompt_admin_content_edit(
                    update,
                    context,
                    field="album",
                    instruction="Отправьте ссылку или материалы для фотоальбома."
                    + self.ADMIN_CANCEL_PROMPT,
                )
                return
            if command_text == self.ADMIN_EDIT_CONTACTS_BUTTON:
                await self._prompt_admin_content_edit(
                    update,
                    context,
                    field="contacts",
                    instruction="Введите обновлённые контакты (при необходимости с медиа)."
                    + self.ADMIN_CANCEL_PROMPT,
                )
                return
            if command_text == self.ADMIN_EDIT_VOCABULARY_BUTTON:
                await self._prompt_admin_vocabulary_edit(update, context)
                return

        if text:
            await self._handle_menu_selection(update, context)
            return

        if attachments:
            await self._reply(
                update,
                "📌 Пожалуйста, используйте кнопки меню или отправьте текстовое сообщение.",
                reply_markup=self._main_menu_markup_for(update, context),
            )

    async def _dispatch_admin_action(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        pending: Dict[str, Any],
        *,
        text: str,
        attachments: list[MediaAttachment],
    ) -> None:
        action_type = pending.get("type")
        if action_type == "broadcast":
            await self._admin_send_broadcast(update, context, text, attachments)
            return
        if action_type == "edit_content":
            field = pending.get("field")
            if isinstance(field, str):
                await self._admin_apply_content_update(
                    update,
                    context,
                    field,
                    text=text,
                    attachments=attachments,
                )
            else:
                await self._reply(
                    update,
                    "Не удалось определить редактируемый блок.",
                    reply_markup=self._admin_menu_markup(),
                )
            return
        if action_type == "add_admin":
            await self._admin_add_new_admin(update, context, text)
            return
        if action_type == "edit_vocabulary":
            success = await self._admin_apply_vocabulary_update(update, context, text)
            if not success:
                context.chat_data["pending_admin_action"] = pending
            return
        await self._reply(
            update,
            "Неизвестное действие администратора.",
            reply_markup=self._admin_menu_markup(),
        )

    async def _admin_add_new_admin(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, payload: str
    ) -> None:
        try:
            admin_id = _coerce_chat_id(payload)
        except ValueError:
            await self._reply(
                update,
                "Пожалуйста, отправьте числовой chat_id администратора."
                + self.ADMIN_CANCEL_PROMPT,
                reply_markup=self._admin_action_keyboard(),
            )
            context.chat_data["pending_admin_action"] = {"type": "add_admin"}
            return

        if admin_id in self._runtime_admin_ids:
            await self._reply(
                update,
                "Этот chat_id уже обладает правами администратора.",
                reply_markup=self._admin_menu_markup(),
            )
            return

        self._store_dynamic_admin(context, admin_id)
        message = f"✅ Администратор {admin_id} добавлен."
        await self._reply(update, message, reply_markup=self._admin_menu_markup())

        await self._notify_admins(
            context,
            f"👑 Обновление прав: {admin_id} теперь администратор.",
        )

    async def _prompt_admin_content_edit(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        *,
        field: str,
        instruction: str,
    ) -> None:
        content = self._get_content(context)
        if not hasattr(content, field):
            await self._reply(
                update,
                "Этот раздел нельзя редактировать.",
                reply_markup=self._admin_menu_markup(),
            )
            return
        context.chat_data["pending_admin_action"] = {"type": "edit_content", "field": field}
        current_block = getattr(content, field)
        if isinstance(current_block, ContentBlock):
            text_preview = current_block.text or "(текста нет)"
            media_note = (
                f"📎 Текущих вложений: {len(current_block.media)}"
                if current_block.media
                else "📎 Вложения отсутствуют."
            )
        else:
            text_preview = str(current_block)
            media_note = "📎 Вложения отсутствуют."
        message = (
            f"{instruction}\n\n"
            "Текущий текст:"
            f"\n{text_preview}\n{media_note}"
        )
        await self._reply(update, message, reply_markup=self._admin_action_keyboard())

    async def _prompt_admin_vocabulary_edit(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        content = self._get_content(context)
        context.chat_data["pending_admin_action"] = {"type": "edit_vocabulary"}
        serialized_entries = []
        for entry in content.vocabulary:
            serialized_entries.append(
                "|".join(
                    [
                        entry.get("word", ""),
                        entry.get("emoji", ""),
                        entry.get("translation", ""),
                        entry.get("example_fr", ""),
                        entry.get("example_ru", ""),
                    ]
                )
            )
        sample = "\n".join(serialized_entries) if serialized_entries else "(пока нет записей)"
        message = (
            "Отправьте новые слова в формате: слово|эмодзи|перевод|пример FR|пример RU."
            "\nКаждое слово — на отдельной строке."
            f"\n\nТекущий список:\n{sample}"
        )
        await self._reply(
            update,
            message + self.ADMIN_CANCEL_PROMPT,
            reply_markup=self._admin_action_keyboard(),
        )

    async def _admin_send_broadcast(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        message: str,
        attachments: list[MediaAttachment],
    ) -> None:
        known_chats = self._get_known_chats(context)
        if not known_chats:
            await self._reply(
                update,
                "Пока нет чатов для рассылки.",
                reply_markup=self._admin_menu_markup(),
            )
            return

        successes = 0
        failures: list[str] = []
        for chat_id in sorted(known_chats):
            try:
                await self._send_payload_to_chat(
                    context,
                    chat_id,
                    text=message if message else None,
                    media=attachments or None,
                )
                successes += 1
            except Exception as exc:  # pragma: no cover - network dependent
                LOGGER.warning("Failed to send broadcast to %s: %s", chat_id, exc)
                failures.append(str(chat_id))

        result = f"Рассылка завершена: {successes} из {len(known_chats)} чатов."
        if failures:
            result += "\nНе удалось доставить сообщения в чаты: " + ", ".join(failures)
        await self._reply(update, result, reply_markup=self._admin_menu_markup())

    async def _admin_share_registrations_table(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        await self._purge_expired_registrations(context)
        registrations = self._application_data(context).get("registrations", [])
        if not isinstance(registrations, list) or not registrations:
            await self._reply(
                update,
                "Заявок пока нет.",
                reply_markup=self._admin_menu_markup(),
            )
            return

        backend = self._sheets_backend
        if backend is not None:
            try:
                sheet_link = await backend.link()
            except Exception as exc:  # pragma: no cover - network dependent
                LOGGER.warning("Не удалось получить ссылку на Google Sheets: %s", exc)
            else:
                preview_lines = self._format_registrations_preview(registrations)
                message_parts = [
                    "📊 Живая таблица заявок доступна в Google Sheets!",
                    f"🗂 Всего записей: {len(registrations)}",
                    "",
                    f"🔗 Откройте таблицу: {sheet_link}",
                    "Все изменения, внесённые в таблицу, видны администраторам и сохраняются в облаке.",
                ]
                if preview_lines:
                    message_parts.append("")
                    message_parts.extend(preview_lines)
                service_email = getattr(backend, "service_account_email", None)
                if service_email:
                    message_parts.append("")
                    message_parts.append(
                        "ℹ️ Убедитесь, что сервисному аккаунту предоставлен доступ на редактирование:"
                    )
                    message_parts.append(service_email)

                await self._reply(
                    update,
                    "\n".join(message_parts),
                    reply_markup=self._admin_menu_markup(),
                )
                return

        export_path, generated_at = self._export_registrations_excel(context, registrations)
        preview_lines = self._format_registrations_preview(registrations)
        deeplink = await self._build_registrations_deeplink(context)

        message_parts = [
            "📊 Экспорт заявок готов!\n",
            f"🗂 Всего записей: {len(registrations)}",
            f"🕒 Обновлено: {generated_at}",
        ]
        if preview_lines:
            message_parts.append("")
            message_parts.extend(preview_lines)
        if deeplink:
            message_parts.append("")
            message_parts.append(f"🔗 Таблица: {deeplink}")
            message_parts.append(
                "Нажмите ссылку, чтобы в любой момент получить свежую версию."
            )
        else:
            message_parts.append("")
            message_parts.append(
                "🔽 Файл с таблицей отправлен ниже. Сохраните его в облаке Telegram для быстрого доступа."
            )

        await self._reply(
            update,
            "\n".join(message_parts),
            reply_markup=self._admin_menu_markup(),
        )
        await self._send_registrations_excel(
            update,
            context,
            path=export_path,
            generated_at=generated_at,
        )

    def _export_registrations_excel(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        registrations: list[dict[str, Any]],
    ) -> tuple[Path, str]:
        builder = _SimpleXlsxBuilder(sheet_name="Заявки")
        builder.add_row(
            (
                "Дата заявки",
                "Программа",
                "Участник",
                "Класс / возраст",
                "Телефон",
                "Предпочтительное время",
                "Оплата",
                "Вложения оплаты",
                "Комментарий",
                "Отправитель",
                "Чат",
            )
        )

        for record in registrations:
            payment_media = record.get("payment_media") or []
            payment_status = "Получено" if payment_media else "Ожидается"
            if payment_media:
                payment_status += f" ({len(payment_media)} влож.)"
            payment_files = []
            for item in payment_media:
                kind = item.get("kind", "") if isinstance(item, dict) else ""
                file_id = item.get("file_id", "") if isinstance(item, dict) else ""
                if kind and file_id:
                    payment_files.append(f"{kind}: {file_id}")
            builder.add_row(
                (
                    record.get("created_at") or "",
                    record.get("program") or "",
                    record.get("child_name") or "",
                    record.get("class") or "",
                    record.get("phone") or "",
                    record.get("time") or "",
                    payment_status,
                    "\n".join(payment_files) if payment_files else "",
                    record.get("payment_note") or "",
                    record.get("submitted_by") or "",
                    record.get("chat_title") or "",
                )
            )

        generated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        export_path = Path("data") / "exports" / "confetti_registrations.xlsx"
        builder.to_file(export_path)

        storage = self._application_data(context)
        exports_meta = storage.setdefault("exports", {})
        if isinstance(exports_meta, dict):
            exports_meta["registrations"] = {
                "generated_at": generated_at,
                "path": str(export_path),
            }
        else:
            storage["exports"] = {
                "registrations": {
                    "generated_at": generated_at,
                    "path": str(export_path),
                }
            }

        self._save_persistent_state()

        return export_path, generated_at

    def _format_registrations_preview(
        self, registrations: list[dict[str, Any]]
    ) -> list[str]:
        if not registrations:
            return []

        preview = ["🆕 Последние заявки:"]
        latest = registrations[-3:]
        for record in reversed(latest):
            child = record.get("child_name") or "—"
            program = record.get("program") or "—"
            created = record.get("created_at") or "—"
            preview.append(f"• {child} | {program} | {created}")
        remaining = len(registrations) - len(latest)
        if remaining > 0:
            preview.append(f"…и ещё {remaining} записей в таблице")
        return preview

    async def _build_registrations_deeplink(
        self, context: ContextTypes.DEFAULT_TYPE
    ) -> Optional[str]:
        if self._bot_username:
            return f"https://t.me/{self._bot_username}?start=registrations_excel"

        try:
            me = await context.bot.get_me()
        except Exception as exc:  # pragma: no cover - network dependent
            LOGGER.debug("Failed to resolve bot username: %s", exc)
            return None

        username = getattr(me, "username", None)
        if not username:
            return None

        self._bot_username = username
        return f"https://t.me/{username}?start=registrations_excel"

    async def _send_registrations_excel(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        *,
        path: Optional[Path] = None,
        generated_at: Optional[str] = None,
    ) -> bool:
        chat = update.effective_chat
        if chat is None:
            return False

        await self._purge_expired_registrations(context)
        registrations = self._application_data(context).get("registrations", [])
        if path is None or generated_at is None:
            if not isinstance(registrations, list) or not registrations:
                await self._reply(
                    update,
                    "Заявок пока нет.",
                    reply_markup=self._admin_menu_markup(),
                )
                return False
            path, generated_at = self._export_registrations_excel(context, registrations)

        try:
            chat_id = _coerce_chat_id_from_object(chat)
        except ValueError:
            return False

        caption = (
            "📊 Таблица заявок студии «Конфетти»\n"
            f"Обновлено: {generated_at}\n"
            "Документ включает все заявки и обновляется при каждом экспорте."
        )

        try:
            with path.open("rb") as handle:
                await context.bot.send_document(
                    chat_id=chat_id,
                    document=handle,
                    filename=path.name,
                    caption=caption,
                )
        except Exception as exc:  # pragma: no cover - network dependent
            LOGGER.warning("Не удалось отправить таблицу заявок: %s", exc)
            return False

        return True

    async def _admin_apply_content_update(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        field: str,
        *,
        text: str,
        attachments: list[MediaAttachment],
    ) -> None:
        content = self._get_content(context)
        if not hasattr(content, field):
            await self._reply(
                update,
                "Этот раздел нельзя редактировать.",
                reply_markup=self._admin_menu_markup(),
            )
            return
        block = getattr(content, field)
        new_block = ContentBlock(
            text=text.strip(),
            media=[MediaAttachment(kind=item.kind, file_id=item.file_id, caption=item.caption) for item in attachments],
        )
        if isinstance(block, ContentBlock):
            block.text = new_block.text
            block.media = new_block.media
        else:
            setattr(content, field, new_block)
        label = self.CONTENT_LABELS.get(field, field)
        await self._reply(
            update,
            "Раздел обновлён!",
            reply_markup=self._admin_menu_markup(),
        )
        await self._notify_admins(
            context,
            f"🛠 Раздел «{label}» был обновлён администратором.",
            media=attachments or None,
        )
        self._save_persistent_state()

    async def _admin_apply_vocabulary_update(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, payload: str
    ) -> bool:
        lines = [line.strip() for line in payload.splitlines() if line.strip()]
        if not lines:
            await self._reply(
                update,
                "Отправьте хотя бы одну строку с данными."
                + self.ADMIN_CANCEL_PROMPT,
                reply_markup=self._admin_action_keyboard(),
            )
            return False

        entries: list[dict[str, str]] = []
        for line in lines:
            parts = [part.strip() for part in line.split("|")]
            if len(parts) != 5:
                await self._reply(
                    update,
                    "Неверный формат. Используйте 5 частей через вертикальную черту."
                    + self.ADMIN_CANCEL_PROMPT,
                    reply_markup=self._admin_action_keyboard(),
                )
                return False
            entries.append(
                {
                    "word": parts[0],
                    "emoji": parts[1],
                    "translation": parts[2],
                    "example_fr": parts[3],
                    "example_ru": parts[4],
                }
            )

        content = self._get_content(context)
        content.vocabulary = entries
        await self._reply(
            update,
            f"Обновлено слов: {len(entries)}.",
            reply_markup=self._admin_menu_markup(),
        )
        self._save_persistent_state()
        return True


    async def _handle_menu_selection(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        text = (update.message.text or "").strip()
        handlers = {
            "📅 Расписание": self._send_schedule,
            "ℹ️ О студии": self._send_about,
            "👩‍🏫 Преподаватели": self._send_teachers,
            self.REGISTRATION_LIST_BUTTON: self._send_registration_list,
            "📞 Контакты": self._send_contacts,
            "📚 Полезные слова": self._send_vocabulary,
        }

        handler = handlers.get(text)
        if handler is None:
            await self._reply(
                update,
                "Пожалуйста, воспользуйтесь меню внизу экрана.",
                reply_markup=self._main_menu_markup_for(update, context),
            )
            return
        await handler(update, context)

    async def _send_content_block(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, block: ContentBlock
    ) -> None:
        text = block.text.strip() if block.text else ""
        media = block.media or None
        reply_markup = self._main_menu_markup_for(update, context)
        if text:
            await self._reply(update, text, reply_markup=reply_markup, media=media)
            return
        if media:
            await self._reply(
                update,
                "📎 Материал доступен во вложениях.",
                reply_markup=reply_markup,
                media=media,
            )
            return
        await self._reply(
            update,
            "Раздел пока пуст.",
            reply_markup=reply_markup,
        )

    async def _send_schedule(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        content = self._get_content(context)
        await self._send_content_block(update, context, content.schedule)

    async def _send_about(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        content = self._get_content(context)
        await self._send_content_block(update, context, content.about)

    async def _send_registration_list(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        await self._purge_expired_registrations(context)
        records = self._collect_user_registrations(update.effective_user, update.effective_chat)
        reply_markup = self._main_menu_markup_for(update, context)
        if not records:
            await self._reply(
                update,
                "📋 У вас пока нет активных заявок.",
                reply_markup=reply_markup,
            )
            return

        sorted_records = sorted(
            records,
            key=lambda item: self._parse_record_timestamp(item.get("created_at")) or datetime.min,
            reverse=True,
        )

        lines: list[str] = []
        for index, record in enumerate(sorted_records, start=1):
            program = str(record.get("program", "")) or "Без программы"
            child = str(record.get("child_name", ""))
            grade = str(record.get("class", ""))
            time_slot = str(record.get("time", ""))
            created_at = str(record.get("created_at", ""))
            payment_note = str(record.get("payment_note", ""))
            payment_media = record.get("payment_media") or []

            entry_lines = [f"{index}. {program}"]
            details: list[str] = []
            if child:
                details.append(child)
            if grade:
                details.append(f"класс: {grade}")
            if time_slot:
                details.append(f"время: {time_slot}")
            if details:
                entry_lines.append(" • ".join(details))
            if created_at:
                entry_lines.append(f"📅 Заявка от: {created_at}")
            if payment_media:
                entry_lines.append("💳 Оплата: подтверждение во вложении")
            elif payment_note:
                entry_lines.append(f"💳 Оплата: {payment_note}")
            else:
                entry_lines.append("💳 Оплата: ожидается")
            lines.append("\n".join(entry_lines))

        text = "📋 Ваши заявки:\n\n" + "\n\n".join(lines)
        await self._reply(update, text, reply_markup=reply_markup)

    async def _send_teachers(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        content = self._get_content(context)
        intro = content.teachers.text.strip() if content.teachers.text else "Наши преподаватели — увлечённые и опытные педагоги."
        await self._reply(
            update,
            intro,
            reply_markup=self._teacher_inline_keyboard(),
        )

    async def _teacher_show_profile(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        query = update.callback_query
        if query is None:
            return
        data = (query.data or "").split(":", 1)
        if len(data) != 2:
            await query.answer("Не удалось открыть профиль.", show_alert=True)
            return
        key = data[1]
        teacher = next((item for item in self.TEACHERS if item["key"] == key), None)
        if teacher is None:
            await query.answer("Педагог не найден.", show_alert=True)
            return

        caption = f"{teacher['name']}\n\n{teacher['description']}"
        photo_id = teacher.get("photo_file_id")

        if photo_id:
            try:
                await query.answer()
                await query.message.reply_photo(
                    photo_id,
                    caption=caption,
                    reply_markup=self._teacher_inline_keyboard(),
                )
            except Exception as exc:  # pragma: no cover - depends on Telegram runtime
                LOGGER.warning("Failed to send teacher photo: %s", exc)
                await self._reply(
                    update,
                    caption + "\n\n📸 Добавьте корректный file_id в TEACHERS, чтобы показывать фото.",
                    reply_markup=self._teacher_inline_keyboard(),
                    prefer_edit=True,
                )
        else:
            await self._reply(
                update,
                caption + "\n\n📸 Добавьте file_id в TEACHERS, чтобы показать фотографию.",
                reply_markup=self._teacher_inline_keyboard(),
                prefer_edit=True,
            )

    async def _send_album(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        content = self._get_content(context)
        await self._send_content_block(update, context, content.album)

    async def _send_contacts(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        content = self._get_content(context)
        await self._send_content_block(update, context, content.contacts)

    async def _send_vocabulary(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        content = self._get_content(context)
        if not content.vocabulary:
            await self._reply(
                update,
                "Список слов пока пуст. Добавьте варианты через админ-панель.\n"
                "La liste de vocabulaire est vide pour le moment.",
                reply_markup=self._main_menu_markup_for(update, context),
            )
            return
        entry = random.choice(content.vocabulary)
        text = (
            "🎁 Mot du jour / Слово дня :\n\n"
            f"🇫🇷 {entry.get('word', '—')} {entry.get('emoji', '')}\n"
            f"🇷🇺 {entry.get('translation', '—')}\n\n"
            f"💬 Exemple : {entry.get('example_fr', '—')} — {entry.get('example_ru', '—')}"
        )
        await self._reply(update, text, reply_markup=self._main_menu_markup_for(update, context))


@dataclass(frozen=True)
class UserProfile:
    """Representation of a standard chat profile."""

    chat_id: int
    role: str = field(init=False, default="user")

    @property
    def is_admin(self) -> bool:
        return False


@dataclass(frozen=True)
class AdminProfile(UserProfile):
    """Profile granted elevated permissions."""

    role: str = field(init=False, default="admin")

    @property
    def is_admin(self) -> bool:
        return True


class _SimpleXlsxBuilder:
    """Minimal XLSX writer for structured admin exports."""

    def __init__(self, sheet_name: str = "Sheet1") -> None:
        self.sheet_name = self._sanitise_sheet_name(sheet_name)
        self.rows: list[list[str]] = []

    def add_row(self, values: Iterable[Any]) -> None:
        row: list[str] = []
        for value in values:
            if value is None:
                row.append("")
            else:
                row.append(str(value))
        self.rows.append(row)

    def to_file(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with ZipFile(path, "w", ZIP_DEFLATED) as archive:
            archive.writestr("[Content_Types].xml", self._content_types())
            archive.writestr("_rels/.rels", self._rels_root())
            archive.writestr("xl/workbook.xml", self._workbook())
            archive.writestr("xl/_rels/workbook.xml.rels", self._workbook_rels())
            archive.writestr("xl/styles.xml", self._styles())
            archive.writestr("xl/worksheets/sheet1.xml", self._sheet())

    def _sheet(self) -> str:
        rows_xml: list[str] = []
        for row_index, row in enumerate(self.rows, start=1):
            cells: list[str] = []
            for column_index, value in enumerate(row):
                cell_reference = f"{self._column_letter(column_index)}{row_index}"
                style = ' s="1"' if row_index == 1 else ""
                text = escape(value, {"\n": "&#10;"})
                cells.append(
                    f'<c r="{cell_reference}" t="inlineStr"{style}><is><t>{text}</t></is></c>'
                )
            rows_xml.append(f'<row r="{row_index}">{"".join(cells)}</row>')

        sheet_data = "".join(rows_xml)
        return (
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
            "<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" "
            "xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">"
            f"<sheetData>{sheet_data}</sheetData>"
            "</worksheet>"
        )

    def _workbook(self) -> str:
        return (
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
            "<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" "
            "xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">"
            "<sheets>"
            f"<sheet name=\"{escape(self.sheet_name)}\" sheetId=\"1\" r:id=\"rId1\"/>"
            "</sheets>"
            "</workbook>"
        )

    @staticmethod
    def _content_types() -> str:
        return (
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
            "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\">"
            "<Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/>"
            "<Default Extension=\"xml\" ContentType=\"application/xml\"/>"
            "<Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/>"
            "<Override PartName=\"/xl/worksheets/sheet1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>"
            "<Override PartName=\"/xl/styles.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml\"/>"
            "</Types>"
        )

    @staticmethod
    def _rels_root() -> str:
        return (
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
            "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">"
            "<Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument\" Target=\"xl/workbook.xml\"/>"
            "</Relationships>"
        )

    @staticmethod
    def _workbook_rels() -> str:
        return (
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
            "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">"
            "<Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" Target=\"worksheets/sheet1.xml\"/>"
            "<Relationship Id=\"rId2\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles\" Target=\"styles.xml\"/>"
            "</Relationships>"
        )

    @staticmethod
    def _styles() -> str:
        return (
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
            "<styleSheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\">"
            "<fonts count=\"2\">"
            "<font><sz val=\"11\"/><color theme=\"1\"/><name val=\"Calibri\"/><family val=\"2\"/></font>"
            "<font><b/><sz val=\"11\"/><color theme=\"1\"/><name val=\"Calibri\"/><family val=\"2\"/></font>"
            "</fonts>"
            "<fills count=\"1\"><fill><patternFill patternType=\"none\"/></fill></fills>"
            "<borders count=\"1\"><border><left/><right/><top/><bottom/><diagonal/></border></borders>"
            "<cellStyleXfs count=\"1\"><xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\"/></cellStyleXfs>"
            "<cellXfs count=\"2\">"
            "<xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\" xfId=\"0\"/>"
            "<xf numFmtId=\"0\" fontId=\"1\" fillId=\"0\" borderId=\"0\" xfId=\"0\" applyFont=\"1\"/>"
            "</cellXfs>"
            "<cellStyles count=\"1\"><cellStyle name=\"Normal\" xfId=\"0\" builtinId=\"0\"/></cellStyles>"
            "</styleSheet>"
        )

    @staticmethod
    def _column_letter(index: int) -> str:
        result = ""
        while index >= 0:
            index, remainder = divmod(index, 26)
            result = chr(65 + remainder) + result
            index -= 1
        return result

    @staticmethod
    def _sanitise_sheet_name(name: str) -> str:
        sanitized = re.sub(r"[\\/*?:\[\]]", "", name).strip()
        if not sanitized:
            sanitized = "Sheet1"
        return sanitized[:31]


def _normalise_admin_chat_ids(chat_ids: AdminChatIdsInput) -> frozenset[int]:
    """Return a normalised, deduplicated set of admin chat identifiers."""

    result: set[int] = set()
    for candidate in _iter_chat_id_candidates(chat_ids):
        for part in _split_candidate(candidate):
            result.add(_coerce_chat_id(part))
    return frozenset(result)


def _iter_chat_id_candidates(value: AdminChatIdsInput) -> Iterable[ChatIdInput]:
    if value is None:
        return ()
    if isinstance(value, (str, bytes)):
        return (value,)
    if isinstance(value, Iterable):
        return tuple(value)
    return (value,)


def _split_candidate(candidate: ChatIdInput) -> Iterable[ChatIdInput]:
    if isinstance(candidate, str):
        parts = [part.strip() for part in candidate.split(",")]
        return tuple(part for part in parts if part)
    return (candidate,)


def _coerce_chat_id(value: ChatIdInput) -> int:
    if isinstance(value, bool):
        raise ValueError("Boolean values cannot represent a chat id")
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            raise ValueError("Chat id strings cannot be empty")
        value = stripped
    try:
        return int(value)
    except (TypeError, ValueError) as exc:  # pragma: no cover - guard clause
        raise ValueError(f"Invalid chat id: {value!r}") from exc


def _coerce_chat_id_from_object(chat: Any) -> int:
    if hasattr(chat, "id"):
        chat = getattr(chat, "id")
    return _coerce_chat_id(chat)  # type: ignore[arg-type]


def main() -> None:  # pragma: no cover - thin wrapper
    """Entry point used by the console script in the original project."""

    logging.basicConfig(level=logging.INFO)

    _load_environment_files()

    token = _resolve_bot_token()
    if token is None:
        LOGGER.error(
            "Bot token is not configured. Set one of %s or provide a file path via %s",
            ", ".join(TOKEN_ENVIRONMENT_KEYS),
            ", ".join(TOKEN_FILE_ENVIRONMENT_KEYS),
        )
        raise SystemExit(1)

    admin_chat_ids = os.environ.get("CONFETTI_ADMIN_CHAT_IDS", "")

    try:
        _require_telegram()
    except RuntimeError as exc:
        LOGGER.error("%s", exc)
        raise SystemExit(1) from exc

    bot = ConfettiTelegramBot(token=token, admin_chat_ids=admin_chat_ids)
    application = bot.build_application()
    # The original project keeps polling outside of the kata scope.  We expose
    # the configured application so that callers can decide how to run it.
    try:
        application.run_polling()
    except TelegramInvalidToken as exc:  # pragma: no cover - network dependent
        LOGGER.error(
            "Telegram отклонил переданный токен. Проверьте значение переменных: %s.",
            ", ".join(TOKEN_ENVIRONMENT_KEYS),
        )
        raise SystemExit(1) from exc
    except TelegramTimedOut as exc:  # pragma: no cover - network dependent
        LOGGER.error(
            "Не удалось подключиться к Telegram: истекло время ожидания (%s).",
            exc,
        )
        LOGGER.error(
            "Проверьте интернет-соединение, настройки прокси или доступ к api.telegram.org."
        )
        raise SystemExit(1) from exc
    except TelegramNetworkError as exc:  # pragma: no cover - network dependent
        LOGGER.error("Сетевой сбой при обращении к Telegram: %s", exc)
        LOGGER.error(
            "Убедитесь, что есть доступ к сети и что запросы к Telegram не блокируются."
        )
        raise SystemExit(1) from exc


TOKEN_ENVIRONMENT_KEYS: tuple[str, ...] = (
    "CONFETTI_BOT_TOKEN",
    "TELEGRAM_BOT_TOKEN",
    "BOT_TOKEN",
    "TELEGRAM_TOKEN",
    "CONFETTI_TOKEN",
)

TOKEN_FILE_ENVIRONMENT_KEYS: tuple[str, ...] = (
    "CONFETTI_BOT_TOKEN_FILE",
    "TELEGRAM_BOT_TOKEN_FILE",
    "BOT_TOKEN_FILE",
    "TELEGRAM_TOKEN_FILE",
    "CONFETTI_BOT_TOKEN_PATH",
)


def _resolve_bot_token() -> Optional[str]:
    """Read the bot token from the environment and validate it."""

    for key in TOKEN_ENVIRONMENT_KEYS:
        token = os.environ.get(key)
        if token:
            token = token.strip()
            if token and token != "TOKEN_PLACEHOLDER":
                return token

    for key in TOKEN_FILE_ENVIRONMENT_KEYS:
        token_path = os.environ.get(key)
        if token_path:
            token = _read_token_file(Path(token_path))
            if token:
                return token
    return None


def _load_environment_files() -> None:
    """Populate ``os.environ`` with values from common dotenv files."""

    base_dir = Path(__file__).resolve().parent
    for filename in (".env", ".env.local"):
        _apply_env_file(base_dir / filename)


def _apply_env_file(path: Path) -> None:
    if not path.exists() or not path.is_file():
        return

    try:
        content = path.read_text(encoding="utf-8")
    except OSError as exc:  # pragma: no cover - filesystem dependent
        LOGGER.warning("Failed to read environment file %s: %s", path, exc)
        return

    for line in content.splitlines():
        parsed = _parse_env_assignment(line)
        if not parsed:
            continue
        key, value = parsed
        if key in os.environ:
            continue
        os.environ[key] = value


def _parse_env_assignment(line: str) -> Optional[tuple[str, str]]:
    """Parse a dotenv-style assignment returning ``(key, value)`` when valid."""

    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None

    if stripped.startswith("export "):
        stripped = stripped[len("export ") :].lstrip()

    if "=" not in stripped:
        LOGGER.debug("Ignoring malformed environment line: %s", line)
        return None

    key, value = stripped.split("=", 1)
    key = key.strip()
    value = value.strip()

    if not key:
        LOGGER.debug("Ignoring environment line with empty key: %s", line)
        return None

    if value and value[0] == value[-1] and value[0] in {'"', "'"}:
        value = value[1:-1]

    return key, value


def _read_token_file(path: Path) -> Optional[str]:
    try:
        token = path.read_text(encoding="utf-8").strip()
    except OSError as exc:  # pragma: no cover - filesystem dependent
        LOGGER.warning("Unable to read token file %s: %s", path, exc)
        return None

    if token and token != "TOKEN_PLACEHOLDER":
        return token
    return None


if __name__ == "__main__":  # pragma: no cover - module executable guard
    main()

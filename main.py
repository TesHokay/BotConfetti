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
import json
import logging
import warnings
import os
import random
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, Optional, Sequence, Union
from xml.sax.saxutils import escape

try:  # pragma: no cover - optional dependency
    from google.auth.transport.requests import Request as GoogleAuthRequest
    from google.oauth2.service_account import (
        Credentials as GoogleServiceAccountCredentials,
    )
    from googleapiclient.discovery import build as google_build
    from googleapiclient.errors import HttpError as GoogleHttpError
except ModuleNotFoundError:  # pragma: no cover - handled at runtime
    GoogleAuthRequest = None  # type: ignore[assignment]
    GoogleServiceAccountCredentials = None  # type: ignore[assignment]
    google_build = None  # type: ignore[assignment]
    GoogleHttpError = Exception  # type: ignore[assignment]

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


@dataclass
class Chat:
    id: int
    type: str = "private"
    title: Optional[str] = None


@dataclass
class User:
    id: int
    full_name: str = ""


@dataclass
class Update:
    effective_chat: Optional[Chat] = None
    effective_user: Optional[User] = None


if TYPE_CHECKING:
    from telegram import (
        InlineKeyboardButton,
        InlineKeyboardMarkup,
        InputMediaAnimation,
        InputMediaDocument,
        InputMediaPhoto,
        InputMediaVideo,
        KeyboardButton,
        ReplyKeyboardMarkup,
        ReplyKeyboardRemove,
        Update,
    )
    from telegram.error import InvalidToken as TelegramInvalidToken
    from telegram.error import NetworkError as TelegramNetworkError
    from telegram.error import TimedOut as TelegramTimedOut
    from telegram.warnings import PTBUserWarning
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
            InputMediaAnimation,
            InputMediaDocument,
            InputMediaPhoto,
            InputMediaVideo,
            KeyboardButton,
            ReplyKeyboardMarkup,
            ReplyKeyboardRemove,
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
        InlineKeyboardButton = InlineKeyboardMarkup = KeyboardButton = ReplyKeyboardMarkup = ReplyKeyboardRemove = object  # type: ignore[assignment]
        InputMediaAnimation = InputMediaDocument = InputMediaPhoto = InputMediaVideo = object  # type: ignore[assignment]
        Application = ApplicationBuilder = CommandHandler = ConversationHandler = MessageHandler = object  # type: ignore[assignment]
        ContextTypes = object  # type: ignore[assignment]
        filters = _MissingTelegramModule()  # type: ignore[assignment]
        TelegramInvalidToken = TelegramNetworkError = TelegramTimedOut = RuntimeError  # type: ignore[assignment]
        _AIORateLimiter = None
        PTBUserWarning = Warning  # type: ignore[assignment]
    else:
        try:
            from telegram.ext import AIORateLimiter as _AIORateLimiter
        except ImportError:  # pragma: no cover - optional dependency
            _AIORateLimiter = None
        try:
            from telegram.warnings import PTBUserWarning
        except ImportError:  # pragma: no cover - warning class depends on version
            PTBUserWarning = Warning  # type: ignore[assignment]

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
    preview_base64: Optional[str] = None
    preview_mime: Optional[str] = None


@dataclass
class ContentBlock:
    """Rich content containing text and optional media attachments."""

    text: str = ""
    media: list[MediaAttachment] = field(default_factory=list)

    def copy(self) -> "ContentBlock":
        return ContentBlock(
            text=self.text,
            media=[
                MediaAttachment(
                    kind=item.kind,
                    file_id=item.file_id,
                    caption=item.caption,
                    preview_base64=item.preview_base64,
                    preview_mime=item.preview_mime,
                )
                for item in self.media
            ],
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
                    "📞 Телефон: +7-912-986-46-31\n"
                    "📧 Email: k.nastytch@gmail.com\n"
                    "🌐 Сайт: https://vk.com/theatreconfetti\n"
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

@dataclass
class ConfettiTelegramBot:
    """Light-weight wrapper around the PTB application builder."""

    token: str
    admin_chat_ids: AdminChatIdsInput = ()
    content_template: BotContent = field(default_factory=BotContent.default)
    storage_path: Optional[Path] = None

    CAPTION_LIMIT = 1024
    MESSAGE_LIMIT = 4096

    REGISTRATION_PROGRAM = 1
    REGISTRATION_CHILD_NAME = 2
    REGISTRATION_SCHOOL = 3
    REGISTRATION_CLASS = 4
    REGISTRATION_CONTACT_NAME = 5
    REGISTRATION_PHONE = 6
    REGISTRATION_COMMENT = 7

    CANCELLATION_PROGRAM = 21
    CANCELLATION_CONTACT = 22
    CANCELLATION_CHILD = 23
    CANCELLATION_PHONE = 24
    CANCELLATION_REASON = 25

    PAYMENT_REPORT_PROGRAM = 41
    PAYMENT_REPORT_NAME = 42
    PAYMENT_REPORT_MEDIA = 43

    MAIN_MENU_BUTTON = "⬅️ Главное меню"
    REGISTRATION_BUTTON = "📝 Запись"
    CANCELLATION_BUTTON = "❗️ Сообщить об отсутствии"
    BACK_BUTTON = "◀️ Назад"
    PAYMENT_REPORT_BUTTON = "💳 Сообщить об оплате"
    ADMIN_MENU_BUTTON = "🛠 Админ-панель"
    ADMIN_BACK_TO_USER_BUTTON = "⬅️ Пользовательское меню"
    ADMIN_BROADCAST_BUTTON = "📣 Рассылка"
    ADMIN_EXPORT_TABLE_BUTTON = "📊 Таблица заявок"
    ADMIN_MANAGE_ADMINS_BUTTON = "👤 Редактировать администраторов"
    ADMIN_EDIT_SCHEDULE_BUTTON = "🗓 Редактировать расписание"
    ADMIN_EDIT_ABOUT_BUTTON = "ℹ️ Редактировать информацию"
    ADMIN_EDIT_TEACHERS_BUTTON = "👩‍🏫 Редактировать преподавателей"
    ADMIN_EDIT_CONTACTS_BUTTON = "📞 Редактировать контакты"
    ADMIN_EDIT_VOCABULARY_BUTTON = "📚 Редактировать словарь"
    ADMIN_CANCEL_KEYWORDS = ("отмена", "annuler", "cancel")
    ADMIN_CANCEL_PROMPT = f"\n\nЧтобы отменить, нажмите «{BACK_BUTTON}» или напишите «Отмена»."

    REGISTRATION_EXPORT_COLUMN_WIDTHS = (
        20,
        36,
        30,
        30,
        18,
        30,
        18,
        32,
    )

    PAYMENT_EXPORT_COLUMN_WIDTHS = (
        20,
        36,
        30,
        36,
        24,
    )

    PAYMENTS_SPREADSHEET_ENV = "CONFETTI_PAYMENTS_SHEETS_ID"
    DEFAULT_PAYMENTS_SPREADSHEET_ID = "1dPD-mvtncpl0Fn2VYBE2VPSHZESk9NGJxfGNUljHOr0"

    MAIN_MENU_LAYOUT = (
        (REGISTRATION_BUTTON, "📅 Расписание"),
        ("ℹ️ О студии", "👩‍🏫 Преподаватели"),
        (PAYMENT_REPORT_BUTTON, "📞 Контакты"),
        ("📚 Слово дня", CANCELLATION_BUTTON),
    )

    PAYMENT_PROGRAM_OPTIONS: tuple[str, ...] = (
        "Весёлый французский, 3 класс (Alain Marinot)",
        "Весёлый французский и театр (Ксения Настыч)",
        "Весёлый французский, 1 класс (Вшивкова Ксения)",
        "Корейский для подростков (Вшивкова Ксения)",
        "Весёлый французский и театр (Анастасия Банникова)",
        "Разговорный клуб",
        "Французский по-взрослому",
        "Индивидуальные занятия",
        "Интенсивы в каникулы",
    )

    DEFAULT_PROGRAMS: tuple[dict[str, str], ...] = (
        {
            "id": "prog-french",
            "title": "📚 Веселый французский",
            "body": (
                "Интенсивная языковая практика. Ученики погружаются в язык через"
                " общение, игры и проекты, закрепляя школьную программу и"
                " расширяя словарный запас.\n\n"
                "С 3 по 11 класс."
            ),
            "photo_url": "https://storage.yandexcloud.net/bigbob/kazd.png",
            "code": "french",
        },
        {
            "id": "prog-theatre",
            "title": "🎭 Театр на французском",
            "body": (
                "Театральная студия для тех, кто любит сцену и французский язык."
                " Готовим постановки, работаем над произношением и учимся"
                " импровизировать на французском."
            ),
            "photo_url": "https://storage.yandexcloud.net/bigbob/photo_2025-09-29_16-01-53(1).jpg",
        },
        {
            "id": "prog-adults",
            "title": "🇫🇷 Французский по-взрослому",
            "body": (
                "Курс для тех, кто уже влюблён во французский. Углубляем"
                " грамматику, отрабатываем разговорные ситуации и готовимся к"
                " международным экзаменам.\n\n"
                "Дни занятий: понедельник / четверг / пятница."
            ),
            "photo_url": "https://storage.yandexcloud.net/bigbob/vzros.png",
        },
        {
            "id": "prog-individual",
            "title": "👩🏼‍🏫 Индивидуальные занятия",
            "body": (
                "Персональные уроки под ваши цели: подготовка к экзаменам,"
                " разговорная практика или помощь по школе.\n\n"
                "Французский, английский и корейский языки."
            ),
            "photo_url": "https://storage.yandexcloud.net/bigbob/indidvid.png",
        },
        {
            "id": "prog-camps",
            "title": "🍂 Интенсивы в каникулы",
            "body": (
                "Погружение в язык на время каникул — отличная возможность"
                " повторить важные темы и сделать большой шаг в изучении"
                " французского. Занятия каждый день по 60 минут офлайн и онлайн"
                " в мини и стандартных группах. Программа построена в"
                " соответствии со школьной."
            ),
            "photo_url": "https://storage.yandexcloud.net/bigbob/osen.png",
        },
        {
            "id": "prog-korean-teens",
            "title": "🇰🇷 Корейский для подростков",
            "body": (
                "Погружаемся в язык и культуру K-pop, сериалов и современных"
                " трендов. Учимся говорить, писать и понимать живой корейский в"
                " дружеской атмосфере."
            ),
            "photo_url": "https://storage.yandexcloud.net/bigbob/20251013_1759_%D0%92%D0%B5%D1%81%D1%91%D0%BB%D0%B0%D1%8F%20%D1%8F%D0%B7%D1%8B%D0%BA%D0%BE%D0%B2%D0%B0%D1%8F%20%D1%88%D0%BA%D0%BE%D0%BB%D0%B0_simple_compose_01k7etc95tfker7vx0btdsps6m.png",
        },
        {
            "id": "prog-club",
            "title": "🗣️ Языковой клуб",
            "body": (
                "Живое общение с носителем на актуальные темы. Практикуем"
                " разговорную речь, учимся выражать мнение и обсуждать всё, что"
                " действительно интересно."
            ),
            "photo_url": "https://storage.yandexcloud.net/bigbob/20251002_1805_%D0%A3%D1%80%D0%BE%D0%BA%D0%B8%20%D1%84%D1%80%D0%B0%D0%BD%D1%86%D1%83%D0%B7%D1%81%D0%BA%D0%BE%D0%B3%D0%BE%20%D1%8F%D0%B7%D1%8B%D0%BA%D0%B0_simple_compose_01k6jgb8aqet48evvgkw40f90a.png",
        },
    )

    FRENCH_PROGRAM_LABEL = "📚 Веселый французский"
    FRENCH_PROGRAM_VARIANTS: tuple[dict[str, str], ...] = (
        {
            "button": "Для 1 класса",
            "stored": "Для 1 класса",
        },
        {
            "button": "Для 2 класса",
            "stored": "Для 2 класса",
        },
        {
            "button": "Для 3 класса",
            "stored": "Для 3 класса",
        },
        {
            "button": "Для 4 класса",
            "stored": "Для 4 класса",
        },
        {
            "button": "Для 5-8 классов",
            "stored": "Для 5-8 классов",
        },
    )

    DEFAULT_TEACHERS: tuple[dict[str, str], ...] = (
        {
            "id": "teacher-nastytsch",
            "name": "Ксения Настыч",
            "bio": (
                "Преподаватель французского языка с опытом более 20 лет."
                " Окончила Пермский государственный университет по"
                " специальности «Филология» и имеет международный сертификат"
                " DALF. Регулярно стажировалась во Франции и организовывала"
                " «русские сезоны» в Посольстве России."
            ),
            "photo_url": "https://storage.yandexcloud.net/bigbob/nastych.jpg",
        },
        {
            "id": "teacher-bannikova",
            "name": "Анастасия Банникова",
            "bio": (
                "Ведёт воскресные программы и театральные занятия. Выпускница"
                " Пермского государственного университета, стажировалась во"
                " Франции (Университет Гренобль-Альпы), имеет международный"
                " диплом DALF C1. Её стиль — дисциплина и порядок: дети"
                " учатся работать системно и добиваются стабильных результатов"
                " уже в первый год."
            ),
            "photo_url": "https://storage.yandexcloud.net/bigbob/bannikova.jpg",
        },
        {
            "id": "teacher-marinot",
            "name": "Ален Марино",
            "bio": (
                "Носитель французского языка с академическим парижским"
                " акцентом. Актёр и душа студии, который общается с учениками"
                " только по-французски и погружает в живую культуру."
            ),
            "photo_url": "https://storage.yandexcloud.net/bigbob/marinot.jpg",
        },
        {
            "id": "teacher-krasnoborova",
            "name": "Людмила Красноборова",
            "bio": (
                "Кандидат филологических наук, доцент ПГНИУ и экзаменатор DALF."
                " Готовит подростков и взрослых к экзаменам и олимпиадам,"
                " сочетая академизм и практику."
            ),
            "photo_url": "https://storage.yandexcloud.net/bigbob/lydmila.jpg",
        },
        {
            "id": "teacher-vshivkova",
            "name": "Ксения Вшивкова",
            "bio": (
                "Владеет французским, английским и корейским языками."
                " Студентка ПГНИУ (2021–2026), факультет современных иностранных"
                " языков и литератур по направлению «Перевод и"
                " переводоведение». Работает с детьми более четырёх лет."
                " Ведёт групповые занятия по французскому и корейскому, а"
                " также индивидуальные уроки по французскому, английскому и"
                " корейскому языкам."
            ),
            "photo_url": "https://storage.yandexcloud.net/bigbob/vshyk.jpg",
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

    MEDIA_DIRECTIVE_PATTERN = re.compile(
        r"^(?P<kind>photo|video|animation|document)\s*:\s*(?P<url>https?://\S+)(?:\s*\|\s*(?P<caption>.+))?$",
        re.IGNORECASE,
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
        if not isinstance(self.token, str):
            potential_admins = self.token
            if not self.admin_chat_ids:
                self.admin_chat_ids = potential_admins  # type: ignore[assignment]
            self.token = ""
        normalised = _normalise_admin_chat_ids(self.admin_chat_ids)
        self.admin_chat_ids = normalised
        self._runtime_admin_ids: set[int] = set(normalised)
        self._admin_cancel_tokens: set[str] = {token.lower() for token in self.ADMIN_CANCEL_KEYWORDS}
        storage_path = self.storage_path or Path(os.environ.get("CONFETTI_STORAGE_PATH", "data/confetti_state.json"))
        self.storage_path = storage_path.expanduser()
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        self._known_registration_ids: set[str] = set()
        self._known_payment_ids: set[str] = set()
        self._persistent_store: dict[str, Any] = self._load_persistent_state()
        self._ensure_registration_ids()
        self._ensure_payment_ids()
        self._ensure_program_catalog()
        self._ensure_teacher_directory()
        dynamic_admins = self._persistent_store.get("dynamic_admins")
        if isinstance(dynamic_admins, set):
            self._runtime_admin_ids.update(dynamic_admins)
        self._storage_dirty = False
        self._bot_username: Optional[str] = None
        self._google_sheets_exporters: dict[str, Optional[_GoogleSheetsExporter]] = {}
        self._last_google_sheet_urls: dict[str, Optional[str]] = {}

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

    def _ensure_payment_ids(self) -> None:
        payments = self._persistent_store.get("payments")
        if not isinstance(payments, list):
            self._persistent_store["payments"] = []
            return

        dirty = False
        for entry in payments:
            if not isinstance(entry, dict):
                continue
            record_id = entry.get("id")
            if record_id:
                record_id_str = str(record_id)
            else:
                record_id_str = self._generate_payment_id()
                entry["id"] = record_id_str
                dirty = True
            self._known_payment_ids.add(record_id_str)
            attachments = entry.get("attachments")
            cleaned: list[dict[str, str]] = []
            if isinstance(attachments, list):
                for item in attachments:
                    if not isinstance(item, dict):
                        continue
                    kind = item.get("kind")
                    file_id = item.get("file_id")
                    if not kind or not file_id:
                        continue
                    cleaned.append(
                        {
                            "kind": str(kind),
                            "file_id": str(file_id),
                            "caption": str(item.get("caption", "")),
                            "preview_base64": str(item.get("preview_base64", "")),
                            "preview_mime": str(item.get("preview_mime", "")),
                        }
                    )
            entry["attachments"] = cleaned
        if dirty:
            self._save_persistent_state()

    def _generate_catalog_identifier(self, prefix: str, existing: set[str]) -> str:
        while True:
            candidate = f"{prefix}-{random.randint(100000, 999999)}"
            if candidate not in existing:
                return candidate

    def _normalise_program_entry(
        self, item: Any, existing_ids: set[str]
    ) -> tuple[Optional[dict[str, Any]], bool]:
        dirty = False
        if not isinstance(item, dict):
            return None, dirty

        title_candidate = item.get("title") or item.get("label") or ""
        title = str(title_candidate).strip()
        if not title:
            return None, dirty

        identifier_candidate = str(item.get("id") or item.get("key") or "").strip()
        if not identifier_candidate or identifier_candidate in existing_ids:
            identifier_candidate = self._generate_catalog_identifier("prog", existing_ids)
            dirty = True

        body_parts: list[str] = []
        body_value = item.get("body")
        if isinstance(body_value, str) and body_value.strip():
            body_parts.append(body_value.strip())
        description = item.get("description")
        if isinstance(description, str) and description.strip() and description.strip() not in body_parts:
            if body_parts:
                body_parts.append("")
            body_parts.append(description.strip())

        extras: list[str] = []
        for field in ("audience", "teacher", "schedule"):
            value = item.get(field)
            if isinstance(value, str) and value.strip():
                extras.append(value.strip())
        if extras:
            if body_parts:
                body_parts.append("")
            body_parts.extend(extras)

        if not body_parts:
            body_parts.append(title)

        photo_file_id = str(item.get("photo_file_id", "") or "").strip()
        photo_url = str(item.get("photo_url", "") or "").strip()
        code = str(item.get("code", "") or "").strip()

        variants_value = item.get("variants")
        variants: list[dict[str, str]] = []
        if isinstance(variants_value, list):
            for option in variants_value:
                if not isinstance(option, dict):
                    continue
                button = str(
                    option.get("button")
                    or option.get("label")
                    or option.get("title")
                    or ""
                ).strip()
                stored_raw = option.get("stored") or option.get("value")
                stored = str(stored_raw or button).strip()
                if not button:
                    continue
                variants.append({"button": button, "stored": stored or button})
        if not variants and (code == "french" or title == self.FRENCH_PROGRAM_LABEL):
            variants = [
                {"button": option["button"], "stored": option.get("stored", option["button"])}
                for option in self.FRENCH_PROGRAM_VARIANTS
            ]

        entry = {
            "id": identifier_candidate,
            "title": title,
            "body": "\n".join(body_parts).strip(),
            "photo_file_id": photo_file_id,
            "photo_url": photo_url,
            "code": code,
            "variants": variants,
        }
        return entry, dirty

    def _ensure_program_catalog(self) -> None:
        programs_raw = self._persistent_store.get("programs")
        normalized: list[dict[str, Any]] = []
        dirty = False
        existing_ids: set[str] = set()

        if isinstance(programs_raw, list):
            for item in programs_raw:
                entry, entry_dirty = self._normalise_program_entry(item, existing_ids)
                if entry is None:
                    continue
                if not entry.get("photo_file_id"):
                    entry["photo_file_id"] = ""
                if not entry.get("photo_url"):
                    entry["photo_url"] = ""
                normalized.append(entry)
                existing_ids.add(entry["id"])
                if entry_dirty:
                    dirty = True

        if not normalized:
            normalized = [
                {
                    "id": item.get("id", self._generate_catalog_identifier("prog", existing_ids)),
                    "title": item.get("title", ""),
                    "body": item.get("body", ""),
                    "photo_file_id": item.get("photo_file_id", ""),
                    "photo_url": item.get("photo_url", ""),
                    "code": item.get("code", ""),
                    "variants": [
                        {
                            "button": option["button"],
                            "stored": option.get("stored", option["button"]),
                        }
                        for option in (item.get("variants") or ())
                    ]
                    if item.get("variants")
                    else (
                        [
                            {
                                "button": option["button"],
                                "stored": option.get("stored", option["button"]),
                            }
                            for option in self.FRENCH_PROGRAM_VARIANTS
                        ]
                        if item.get("code") == "french"
                        else []
                    ),
                }
                for item in self.DEFAULT_PROGRAMS
            ]
            for entry in normalized:
                existing_ids.add(entry["id"])
            dirty = True

        self._persistent_store["programs"] = normalized
        if dirty:
            self._save_persistent_state()

    def _normalise_teacher_entry(
        self, item: Any, existing_ids: set[str]
    ) -> tuple[Optional[dict[str, Any]], bool]:
        dirty = False
        if not isinstance(item, dict):
            return None, dirty

        name_candidate = item.get("name") or ""
        name = str(name_candidate).strip()
        if not name:
            return None, dirty

        identifier_candidate = str(item.get("id") or item.get("key") or "").strip()
        if not identifier_candidate or identifier_candidate in existing_ids:
            identifier_candidate = self._generate_catalog_identifier("teacher", existing_ids)
            dirty = True

        bio_parts: list[str] = []
        for field in ("bio", "description"):
            value = item.get(field)
            if isinstance(value, str) and value.strip():
                bio_parts.append(value.strip())
        if not bio_parts:
            bio_parts.append(name)

        photo_file_id = str(item.get("photo_file_id", "") or "").strip()
        photo_url = str(item.get("photo_url", "") or "").strip()

        entry = {
            "id": identifier_candidate,
            "name": name,
            "bio": "\n".join(bio_parts).strip(),
            "photo_file_id": photo_file_id,
            "photo_url": photo_url,
        }
        return entry, dirty

    def _ensure_teacher_directory(self) -> None:
        teachers_raw = self._persistent_store.get("teachers")
        normalized: list[dict[str, Any]] = []
        dirty = False
        existing_ids: set[str] = set()

        if isinstance(teachers_raw, list):
            for item in teachers_raw:
                entry, entry_dirty = self._normalise_teacher_entry(item, existing_ids)
                if entry is None:
                    continue
                if not entry.get("photo_file_id"):
                    entry["photo_file_id"] = ""
                if not entry.get("photo_url"):
                    entry["photo_url"] = ""
                normalized.append(entry)
                existing_ids.add(entry["id"])
                if entry_dirty:
                    dirty = True

        if not normalized:
            normalized = [
                {
                    "id": item.get("id", self._generate_catalog_identifier("teacher", existing_ids)),
                    "name": item.get("name", ""),
                    "bio": item.get("bio", ""),
                    "photo_file_id": item.get("photo_file_id", ""),
                    "photo_url": item.get("photo_url", ""),
                }
                for item in self.DEFAULT_TEACHERS
            ]
            for entry in normalized:
                existing_ids.add(entry["id"])
            dirty = True

        self._persistent_store["teachers"] = normalized
        if dirty:
            self._save_persistent_state()

    def _program_catalog(self) -> list[dict[str, Any]]:
        programs = self._persistent_store.get("programs")
        if isinstance(programs, list):
            return programs
        return []

    def _program_variants(self, program: Optional[dict[str, Any]]) -> list[dict[str, str]]:
        if not isinstance(program, dict):
            return []
        variants_value = program.get("variants")
        result: list[dict[str, str]] = []
        if isinstance(variants_value, list):
            for option in variants_value:
                if not isinstance(option, dict):
                    continue
                button = str(option.get("button", "") or "").strip()
                stored_raw = option.get("stored") or option.get("value")
                stored = str(stored_raw or button).strip()
                if not button:
                    continue
                result.append({"button": button, "stored": stored or button})
        return result

    def _teacher_directory(self) -> list[dict[str, Any]]:
        teachers = self._persistent_store.get("teachers")
        if isinstance(teachers, list):
            return teachers
        return []

    def _generate_registration_id(self) -> str:
        while True:
            candidate = datetime.utcnow().strftime("%Y%m%d%H%M%S") + f"-{random.randint(1000, 9999)}"
            if candidate not in self._known_registration_ids:
                self._known_registration_ids.add(candidate)
                return candidate

    def _generate_payment_id(self) -> str:
        while True:
            candidate = "PAY-" + datetime.utcnow().strftime("%Y%m%d%H%M%S") + f"-{random.randint(1000, 9999)}"
            if candidate not in self._known_payment_ids:
                self._known_payment_ids.add(candidate)
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

        payments_raw = data.get("payments")
        payments: list[dict[str, Any]] = []
        if isinstance(payments_raw, list):
            for item in payments_raw:
                if not isinstance(item, dict):
                    continue
                attachments_payload = item.get("attachments")
                attachments: list[dict[str, str]] = []
                if isinstance(attachments_payload, list):
                    for entry in attachments_payload:
                        if not isinstance(entry, dict):
                            continue
                        kind = entry.get("kind")
                        file_id = entry.get("file_id")
                        if not kind or not file_id:
                            continue
                        attachments.append(
                            {
                                "kind": str(kind),
                                "file_id": str(file_id),
                                "caption": str(entry.get("caption", "")),
                                "preview_base64": str(entry.get("preview_base64", "")),
                                "preview_mime": str(entry.get("preview_mime", "")),
                            }
                        )
                payments.append(
                    {
                        "id": str(item.get("id", "")),
                        "program": str(item.get("program", "")),
                        "full_name": str(item.get("full_name", "")),
                        "chat_id": item.get("chat_id"),
                        "submitted_by": str(item.get("submitted_by", "")),
                        "submitted_by_id": item.get("submitted_by_id"),
                        "created_at": str(item.get("created_at", "")),
                        "attachments": attachments,
                    }
                )
        data["payments"] = payments

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
                    "school": _text("school"),
                    "class": _text("class"),
                    "contact_name": _text("contact_name"),
                    "phone": _text("phone"),
                    "last_program": _text("last_program"),
                    "last_comment": _text("last_comment"),
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
                                "child_name": _item_text(item, "child_name"),
                                "school": _item_text(item, "school"),
                                "class": _item_text(item, "class"),
                                "contact_name": _item_text(item, "contact_name"),
                                "phone": _item_text(item, "phone"),
                                "comment": _item_text(item, "comment"),
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
                "school": entry.get("school", ""),
                "class": entry.get("class", ""),
                "contact_name": entry.get("contact_name", ""),
                "phone": entry.get("phone", ""),
                "program": entry.get("last_program", ""),
                "comment": entry.get("last_comment", ""),
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
            ("school", "school"),
            ("class", "class"),
            ("contact_name", "contact_name"),
            ("phone", "phone"),
            ("comment", "last_comment"),
        ):
            value = str(data.get(source_key, ""))
            if entry.get(target_key) != value:
                entry[target_key] = value
                changed = True

        for source_key, target_key in (("program", "last_program"),):
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
            "child_name": str(record.get("child_name", "")),
            "school": str(record.get("school", "")),
            "class": str(record.get("class", "")),
            "contact_name": str(record.get("contact_name", "")),
            "phone": str(record.get("phone", "")),
            "comment": str(record.get("comment", "")),
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
            for field in ("child_name", "school", "class", "contact_name", "phone"):
                value = snapshot.get(field, "")
                if value and entry.get(field) != value:
                    entry[field] = value
                    changed = True
            comment = snapshot.get("comment", "")
            if comment and entry.get("last_comment") != comment:
                entry["last_comment"] = comment
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
                    for field, target in (
                        ("program", "last_program"),
                        ("child_name", "child_name"),
                        ("school", "school"),
                        ("class", "class"),
                        ("contact_name", "contact_name"),
                        ("phone", "phone"),
                        ("comment", "last_comment"),
                    ):
                        value = str(latest.get(field, ""))
                        if entry.get(target) != value:
                            entry[target] = value
                            changed = True
                else:
                    for target in (
                        "last_program",
                        "child_name",
                        "school",
                        "class",
                        "contact_name",
                        "phone",
                        "last_comment",
                    ):
                        if entry.get(target):
                            entry[target] = ""
                            changed = True
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

        if isinstance(chat, Update):
            update_obj = chat
            chat = update_obj.effective_chat
            if user is None:
                user = update_obj.effective_user

        chat_id = _coerce_chat_id_from_object(chat)
        is_admin = self._is_admin_identity(chat=chat, user=user)
        keyboard = self._admin_keyboard() if is_admin else self._user_keyboard()
        if is_admin:
            return AdminProfile(chat_id=chat_id, keyboard=keyboard)
        return UserProfile(chat_id=chat_id, keyboard=keyboard)

    def _user_keyboard(self) -> list[list[str]]:
        return [list(row) for row in self.MAIN_MENU_LAYOUT]

    def _admin_keyboard(self) -> list[list[str]]:
        keyboard = self._user_keyboard()
        keyboard.append([self.ADMIN_MENU_BUTTON])
        return keyboard

    def is_admin_chat(self, chat: Any) -> bool:
        """Return ``True`` when ``chat`` belongs to an administrator."""

        return self._is_admin_identity(chat=chat)

    def is_admin_user(self, user: Any) -> bool:
        """Return ``True`` when ``user`` is recognised as an administrator."""

        return self._is_admin_identity(user=user)

    def broadcast_to_admins(self, update: Optional[Update] = None) -> set[int]:
        """Return the set of administrator chat ids for broadcast helpers."""

        return set(self._runtime_admin_ids)

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

        with warnings.catch_warnings():
            if PTBUserWarning is not None:
                warnings.simplefilter("ignore", PTBUserWarning)
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
                            pattern=r"^reg_program:\d+$",
                        ),
                        CallbackQueryHandler(
                            self._registration_collect_program_variant,
                            pattern=r"^reg_variant:\d+$",
                        ),
                        CallbackQueryHandler(
                            self._registration_variant_back_to_program,
                            pattern=r"^reg_variant:back$",
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
                        MessageHandler(
                            filters.TEXT & ~filters.COMMAND,
                            self._registration_collect_child_name,
                        ),
                    ],
                    self.REGISTRATION_SCHOOL: [
                        MessageHandler(
                            filters.Regex(self._exact_match_regex(self.MAIN_MENU_BUTTON)),
                            self._registration_cancel,
                        ),
                        MessageHandler(
                            filters.Regex(self._exact_match_regex(self.BACK_BUTTON)),
                            self._registration_back_to_child_name,
                        ),
                        MessageHandler(
                            filters.TEXT & ~filters.COMMAND,
                            self._registration_collect_school,
                        ),
                    ],
                    self.REGISTRATION_CLASS: [
                        MessageHandler(
                            filters.Regex(self._exact_match_regex(self.MAIN_MENU_BUTTON)),
                            self._registration_cancel,
                        ),
                        MessageHandler(
                            filters.Regex(self._exact_match_regex(self.BACK_BUTTON)),
                            self._registration_back_to_school,
                        ),
                        MessageHandler(
                            filters.TEXT & ~filters.COMMAND,
                            self._registration_collect_class,
                        ),
                    ],
                    self.REGISTRATION_CONTACT_NAME: [
                        MessageHandler(
                            filters.Regex(self._exact_match_regex(self.MAIN_MENU_BUTTON)),
                            self._registration_cancel,
                        ),
                        MessageHandler(
                            filters.Regex(self._exact_match_regex(self.BACK_BUTTON)),
                            self._registration_back_to_class,
                        ),
                        MessageHandler(
                            filters.TEXT & ~filters.COMMAND,
                            self._registration_collect_contact_name,
                        ),
                    ],
                    self.REGISTRATION_PHONE: [
                        MessageHandler(
                            filters.Regex(self._exact_match_regex(self.MAIN_MENU_BUTTON)),
                            self._registration_cancel,
                        ),
                        MessageHandler(
                            filters.Regex(self._exact_match_regex(self.BACK_BUTTON)),
                            self._registration_back_to_contact,
                        ),
                        MessageHandler(
                            filters.TEXT & ~filters.COMMAND,
                            self._registration_collect_phone_text,
                        ),
                    ],
                    self.REGISTRATION_COMMENT: [
                        MessageHandler(
                            filters.Regex(self._exact_match_regex(self.MAIN_MENU_BUTTON)),
                            self._registration_cancel,
                        ),
                        MessageHandler(
                            filters.Regex(self._exact_match_regex(self.BACK_BUTTON)),
                            self._registration_back_to_phone,
                        ),
                        MessageHandler(
                            filters.TEXT & ~filters.COMMAND,
                            self._registration_collect_comment,
                        ),
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

        with warnings.catch_warnings():
            if PTBUserWarning is not None:
                warnings.simplefilter("ignore", PTBUserWarning)
            payment_report = ConversationHandler(
                entry_points=[
                    MessageHandler(
                        filters.Regex(self._exact_match_regex(self.PAYMENT_REPORT_BUTTON)),
                        self._start_payment_report,
                    )
                ],
                states={
                self.PAYMENT_REPORT_PROGRAM: [
                    CallbackQueryHandler(
                        self._payment_report_collect_program,
                        pattern=r"^pay_program:\d+$",
                    ),
                    CallbackQueryHandler(
                        self._payment_report_cancel_from_program,
                        pattern=r"^pay_back:menu$",
                    ),
                    MessageHandler(
                        filters.TEXT & ~filters.COMMAND,
                        self._payment_report_prompt_program,
                    ),
                ],
                self.PAYMENT_REPORT_NAME: [
                    MessageHandler(
                        filters.Regex(self._exact_match_regex(self.MAIN_MENU_BUTTON)),
                        self._payment_report_cancel,
                    ),
                    MessageHandler(
                        filters.Regex(self._exact_match_regex(self.BACK_BUTTON)),
                        self._payment_report_back_to_program,
                    ),
                    MessageHandler(
                        filters.TEXT & ~filters.COMMAND,
                        self._payment_report_collect_name,
                    ),
                ],
                self.PAYMENT_REPORT_MEDIA: [
                    MessageHandler(
                        filters.Regex(self._exact_match_regex(self.MAIN_MENU_BUTTON)),
                        self._payment_report_cancel,
                    ),
                    MessageHandler(
                        filters.Regex(self._exact_match_regex(self.BACK_BUTTON)),
                        self._payment_report_back_to_name,
                    ),
                    MessageHandler(~filters.COMMAND, self._payment_report_collect_media),
                ],
                },
                fallbacks=[
                    CommandHandler("cancel", self._payment_report_cancel),
                    MessageHandler(
                        filters.Regex(self._exact_match_regex(self.MAIN_MENU_BUTTON)),
                        self._payment_report_cancel,
                    ),
                ],
                allow_reentry=True,
            )

        with warnings.catch_warnings():
            if PTBUserWarning is not None:
                warnings.simplefilter("ignore", PTBUserWarning)
            cancellation = ConversationHandler(
                entry_points=[
                    MessageHandler(
                        filters.Regex(self._exact_match_regex(self.CANCELLATION_BUTTON)),
                        self._start_cancellation,
                    )
                ],
                states={
                self.CANCELLATION_PROGRAM: [
                    CallbackQueryHandler(
                        self._cancellation_collect_program,
                        pattern=r"^absence_program:\d+$",
                    ),
                    CallbackQueryHandler(
                        self._cancellation_cancel_from_program,
                        pattern=r"^absence_back:menu$",
                    ),
                    MessageHandler(
                        filters.TEXT & ~filters.COMMAND,
                        self._cancellation_prompt_program,
                    ),
                ],
                self.CANCELLATION_CONTACT: [
                    MessageHandler(
                        filters.Regex(self._exact_match_regex(self.MAIN_MENU_BUTTON)),
                        self._cancellation_cancel,
                    ),
                    MessageHandler(
                        filters.Regex(self._exact_match_regex(self.BACK_BUTTON)),
                        self._cancellation_back_to_program,
                    ),
                    MessageHandler(
                        filters.TEXT & ~filters.COMMAND,
                        self._cancellation_collect_contact,
                    ),
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
        application.add_handler(payment_report)
        application.add_handler(cancellation)
        application.add_handler(CallbackQueryHandler(self._about_show_french_variant, pattern=r"^about_variant:"))
        application.add_handler(CallbackQueryHandler(self._about_show_direction, pattern=r"^about:"))
        application.add_handler(CallbackQueryHandler(self._teacher_show_profile, pattern=r"^teacher:"))
        application.add_handler(CallbackQueryHandler(self._admin_about_callback, pattern=r"^admin_about:"))
        application.add_handler(CallbackQueryHandler(self._admin_teacher_callback, pattern=r"^admin_teacher:"))
        application.add_handler(MessageHandler(~filters.COMMAND, self._handle_message))

    def _exact_match_regex(self, text: str) -> str:
        return rf"^{re.escape(text)}$"

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
            [self.ADMIN_MANAGE_ADMINS_BUTTON],
            [self.ADMIN_EDIT_SCHEDULE_BUTTON],
            [self.ADMIN_EDIT_ABOUT_BUTTON],
            [self.ADMIN_EDIT_TEACHERS_BUTTON],
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

    def _remove_dynamic_admin(
        self, context: ContextTypes.DEFAULT_TYPE, admin_id: int
    ) -> bool:
        storage = self._application_data(context)
        existing = storage.get("dynamic_admins")
        if not isinstance(existing, set):
            existing = self._refresh_admin_cache(context)
        if admin_id not in existing:
            return False
        existing.remove(admin_id)
        storage["dynamic_admins"] = existing
        self._runtime_admin_ids.discard(admin_id)
        self._save_persistent_state()
        return True

    def _admin_manage_admins_instruction(
        self, context: ContextTypes.DEFAULT_TYPE
    ) -> str:
        dynamic_ids = sorted(self._refresh_admin_cache(context))
        base_ids = sorted(self.admin_chat_ids)

        def _format_ids(items: Iterable[int]) -> str:
            formatted = [str(item) for item in items]
            return ", ".join(formatted) if formatted else "—"

        return (
            "Управление администраторами.\n"
            "Отправьте +ID, чтобы добавить администратора, или -ID, чтобы удалить его.\n"
            "Можно указать несколько идентификаторов через пробел или на отдельных строках.\n"
            "Получить chat_id можно через @TheGetAnyID_bot.\n\n"
            f"Основные администраторы: {_format_ids(base_ids)}\n"
            f"Добавлены через бот: {_format_ids(dynamic_ids)}"
        )

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
    ) -> dict[str, Any]:
        chat = update.effective_chat
        user = update.effective_user
        record_id = data.get("id") or self._generate_registration_id()
        program_label = str(data.get("program", ""))

        record = {
            "id": record_id,
            "program": program_label,
            "child_name": data.get("child_name", ""),
            "school": data.get("school", ""),
            "class": data.get("class", ""),
            "contact_name": data.get("contact_name", ""),
            "phone": data.get("phone", ""),
            "comment": data.get("comment", ""),
            "chat_id": _coerce_chat_id_from_object(chat) if chat else None,
            "chat_title": getattr(chat, "title", None) if chat else None,
            "submitted_by": getattr(user, "full_name", None) if user else None,
            "submitted_by_id": getattr(user, "id", None) if user else None,
            "created_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
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

    def _store_payment_report(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        data: dict[str, Any],
        attachments: list[dict[str, str]],
    ) -> dict[str, Any]:
        chat = update.effective_chat
        user = update.effective_user
        record_id = self._generate_payment_id()
        record = {
            "id": record_id,
            "program": data.get("program", ""),
            "full_name": data.get("full_name", ""),
            "chat_id": _coerce_chat_id_from_object(chat) if chat else None,
            "submitted_by": getattr(user, "full_name", None) or "",
            "submitted_by_id": getattr(user, "id", None),
            "created_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
            "attachments": attachments,
        }

        payments = self._application_data(context).setdefault("payments", [])
        if isinstance(payments, list):
            payments.append(record)
        else:
            self._application_data(context)["payments"] = [record]

        self._save_persistent_state()
        return record

    def _find_registration_by_id(
        self, context: ContextTypes.DEFAULT_TYPE, registration_id: str
    ) -> Optional[dict[str, Any]]:
        registrations = self._application_data(context).get("registrations")
        if not isinstance(registrations, list):
            return None
        target = registration_id.strip()
        if not target:
            return None
        for record in registrations:
            if not isinstance(record, dict):
                continue
            if str(record.get("id")) == target:
                return record
        return None

    def _find_payment_report_by_id(
        self, context: ContextTypes.DEFAULT_TYPE, payment_id: str
    ) -> Optional[dict[str, Any]]:
        payments = self._application_data(context).get("payments")
        if not isinstance(payments, list):
            return None
        target = payment_id.strip()
        if not target:
            return None
        for record in payments:
            if not isinstance(record, dict):
                continue
            if str(record.get("id")) == target:
                return record
        return None

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

        if removed:
            self._save_persistent_state()


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
        if attachment.caption:
            return f"{title}: {attachment.caption}"
        return f"{title} во вложении"

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
                await self._admin_share_registrations_table(update, context)
                return
            if payload.startswith("payment_"):
                if not self._is_admin_update(update, context):
                    await self._reply(
                        update,
                        "Просмотр вложений доступен только администраторам.",
                        reply_markup=self._main_menu_markup_for(update, context),
                    )
                    return
                remainder = payload.split("payment_", 1)[1]
                registration_id, attachment_index = self._parse_payment_deeplink_payload(
                    remainder
                )
                handled = await self._send_registration_payment_media(
                    update,
                    context,
                    registration_id,
                    attachment_index=attachment_index,
                )
                if handled:
                    return
                handled = await self._send_payment_report_media(
                    update,
                    context,
                    registration_id,
                    attachment_index=attachment_index,
                )
                if handled:
                    return

        await self._send_greeting(update, context)

    async def _show_main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Show the menu without repeating the full greeting."""

        self._remember_chat(update, context)
        message = (
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
            "🎉 🇷🇺 Здравствуйте и добро пожаловать в студию «Конфетти»!\n"
            "Мы обожаем Францию и французский — и готовы делиться этой любовью с каждым.\n\n"
            "🎉 🇫🇷 Bonjour et bienvenue dans la compagnie «Confetti» !\n\n"
            "Nous adorons la France et le français — et nous sommes prêts à partager cet amour à chacun.\n\n"
            "👉 Пожалуйста, выберите раздел в меню ниже."
        )
        if self._is_admin_update(update, context):
            greeting += "\n\n🛠 У вас есть доступ к админ-панели — нажмите кнопку ниже, чтобы управлять контентом."
        await self._reply(update, greeting, reply_markup=self._main_menu_markup_for(update, context))

    def _attachment_to_input_media(self, attachment: MediaAttachment):
        try:
            if attachment.kind == "photo":
                return InputMediaPhoto(attachment.file_id, caption=attachment.caption)
            if attachment.kind == "video":
                return InputMediaVideo(attachment.file_id, caption=attachment.caption)
            if attachment.kind == "animation":
                return InputMediaAnimation(attachment.file_id, caption=attachment.caption)
            if attachment.kind == "document":
                return InputMediaDocument(attachment.file_id, caption=attachment.caption)
        except NameError:  # pragma: no cover - optional telegram dependency
            return None
        return None

    def _split_once_for_limit(self, text: str, limit: int) -> tuple[str, str]:
        """Return the first chunk within ``limit`` and the remaining text."""

        trimmed = text.strip()
        if not trimmed:
            return "", ""

        if len(trimmed) <= limit:
            return trimmed, ""

        split_at = trimmed.rfind("\n\n", 0, limit + 1)
        if split_at == -1:
            split_at = trimmed.rfind("\n", 0, limit + 1)
        if split_at == -1:
            split_at = trimmed.rfind(" ", 0, limit + 1)
        if split_at == -1 or split_at <= 0 or split_at < int(limit * 0.5):
            split_at = limit

        head = trimmed[:split_at].rstrip()
        if not head:
            head = trimmed[:limit]
            split_at = len(head)

        remainder = trimmed[split_at:].lstrip()
        return head, remainder

    def _split_text_for_limit(self, text: str, limit: int) -> list[str]:
        """Split ``text`` into chunks no longer than ``limit`` characters."""

        chunks: list[str] = []
        remaining = text.strip()
        while remaining:
            head, tail = self._split_once_for_limit(remaining, limit)
            if not head:
                break
            chunks.append(head)
            remaining = tail
        return chunks

    def _prepare_media_caption(self, attachment: MediaAttachment) -> list[str]:
        """Ensure media caption respects Telegram limits and return overflow text."""

        caption = (attachment.caption or "").strip()
        if not caption:
            attachment.caption = None
            return []

        if len(caption) <= self.CAPTION_LIMIT:
            attachment.caption = caption
            return []

        head, overflow = self._split_once_for_limit(caption, self.CAPTION_LIMIT - 1)
        trimmed = head[: self.CAPTION_LIMIT - 1].rstrip()
        if not trimmed:
            trimmed = caption[: self.CAPTION_LIMIT - 1].rstrip()

        if overflow:
            trimmed = trimmed[: self.CAPTION_LIMIT - 1].rstrip()
            trimmed = f"{trimmed}…"
        attachment.caption = trimmed

        LOGGER.warning(
            "Caption for %s truncated from %s to %s characters to stay within Telegram limits.",
            attachment.kind,
            len(caption),
            len(attachment.caption or ""),
        )

        return []

    def _resolve_media_reference(
        self,
        payload: dict[str, Any],
        *,
        file_key: str,
        url_key: str,
    ) -> Optional[str]:
        if not isinstance(payload, dict):
            return None

        url_value = payload.get(url_key)
        if isinstance(url_value, str) and url_value.strip():
            return url_value.strip()

        file_value = payload.get(file_key)
        if isinstance(file_value, str) and file_value.strip():
            return file_value.strip()

    def _select_photo_file_id(
        self, attachments: Sequence[MediaAttachment]
    ) -> Optional[str]:
        for attachment in attachments:
            if not isinstance(attachment, MediaAttachment):
                continue
            if attachment.kind == "photo" and attachment.file_id:
                return attachment.file_id
        return None

        return None

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

        extra_texts: list[str] = []
        if media:
            normalized_media: list[MediaAttachment] = []
            for attachment in media:
                clone = MediaAttachment(
                    kind=attachment.kind,
                    file_id=attachment.file_id,
                    caption=attachment.caption,
                )
                extra_texts.extend(self._prepare_media_caption(clone))
                normalized_media.append(clone)
            media = normalized_media

        if (
            prefer_edit
            and callback
            and callback.message is not None
            and (inline_markup is not None or reply_markup is None)
        ):
            try:
                if text is not None:
                    target_message = callback.message
                    edited = False
                    if any(
                        getattr(target_message, attribute, None)
                        for attribute in ("photo", "video", "animation", "document")
                    ):
                        try:
                            await target_message.edit_caption(text, reply_markup=inline_markup)
                        except Exception as exc:  # pragma: no cover - Telegram runtime dependent
                            LOGGER.debug("Failed to edit caption: %s", exc)
                        else:
                            edited = True
                    if not edited:
                        await target_message.edit_text(text, reply_markup=inline_markup)
                    markup_used = inline_markup is not None
                    text = None
                    target = target_message
                elif media:
                    if len(media) == 1:
                        input_media = self._attachment_to_input_media(media[0])
                        if input_media is not None:
                            try:
                                await callback.message.edit_media(
                                    input_media,
                                    reply_markup=inline_markup,
                                )
                            except Exception as exc:  # pragma: no cover - Telegram runtime dependent
                                LOGGER.debug("Failed to edit media: %s", exc)
                                caption = media[0].caption if media[0].caption else None
                                if caption:
                                    try:
                                        await callback.message.edit_text(
                                            caption,
                                            reply_markup=inline_markup,
                                        )
                                    except Exception as text_exc:  # pragma: no cover - Telegram runtime dependent
                                        LOGGER.debug("Failed to edit media caption as text: %s", text_exc)
                                    else:
                                        markup_used = inline_markup is not None
                                        media = []
                                        target = callback.message
                            else:
                                markup_used = inline_markup is not None
                                media = []
                                target = callback.message
                    if media:
                        LOGGER.debug("Unable to edit media in place, falling back to new message")
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
                should_attach_markup = (
                    not markup_used and reply_markup is not None and index == 0
                )
                if should_attach_markup:
                    extra["reply_markup"] = reply_markup
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
                    if attachment.caption:
                        fallback_kwargs: dict[str, Any] = {}
                        if should_attach_markup and reply_markup is not None:
                            fallback_kwargs["reply_markup"] = reply_markup
                        try:
                            await target.reply_text(attachment.caption, **fallback_kwargs)
                        except Exception as text_exc:  # pragma: no cover - network dependent
                            LOGGER.warning(
                                "Failed to send fallback text for media %s: %s",
                                attachment.kind,
                                text_exc,
                            )
                        else:
                            if should_attach_markup:
                                markup_used = True
                    continue
                if should_attach_markup:
                    markup_used = True
        elif reply_markup is not None and not markup_used and target is not None:
            await target.reply_text("", reply_markup=reply_markup)

        if extra_texts:
            if target is None:
                LOGGER.warning("Dropping %s overflow text segments; no target message available.", len(extra_texts))
            else:
                for overflow_text in extra_texts:
                    await target.reply_text(overflow_text)

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
                    "preview_base64": attachment.preview_base64 or "",
                    "preview_mime": attachment.preview_mime or "",
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
            preview_base64 = entry.get("preview_base64") or None
            preview_mime = entry.get("preview_mime") or None
            attachments.append(
                MediaAttachment(
                    kind=kind,
                    file_id=file_id,
                    caption=caption,
                    preview_base64=preview_base64,
                    preview_mime=preview_mime,
                )
            )
        return attachments

    async def _serialise_payment_media(
        self, context: ContextTypes.DEFAULT_TYPE, attachments: list[MediaAttachment]
    ) -> list[dict[str, str]]:
        """Convert payment attachments to a JSON-friendly structure."""

        serialised: list[dict[str, str]] = []
        for attachment in attachments:
            serialised.append(
                {
                    "kind": attachment.kind,
                    "file_id": attachment.file_id,
                    "caption": attachment.caption or "",
                    "preview_base64": attachment.preview_base64 or "",
                    "preview_mime": attachment.preview_mime or "",
                }
            )
        return serialised

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
        if not self._program_catalog():
            await self._reply(
                update,
                "Список направлений временно недоступен. Пожалуйста, свяжитесь с администратором.",
                reply_markup=self._main_menu_markup_for(update, context),
            )
            context.user_data.pop("registration", None)
            return ConversationHandler.END
        await self._reply(
            update,
            self._registration_program_prompt(),
            reply_markup=self._program_inline_keyboard(),
            prefer_edit=update.callback_query is not None,
        )
        return self.REGISTRATION_PROGRAM

    def _program_inline_keyboard(self) -> "InlineKeyboardMarkup":
        programs = self._program_catalog()
        buttons = [
            [
                InlineKeyboardButton(
                    program.get("title", f"Направление {index + 1}"),
                    callback_data=f"reg_program:{index}",
                )
            ]
            for index, program in enumerate(programs)
        ]
        if buttons:
            buttons.append([InlineKeyboardButton(self.BACK_BUTTON, callback_data="reg_back:menu")])
        else:
            buttons = [[InlineKeyboardButton(self.BACK_BUTTON, callback_data="reg_back:menu")]]
        return InlineKeyboardMarkup(buttons)

    def _compose_french_variant_label(self, base_label: str, option: dict[str, str]) -> str:
        base = str(base_label or "").strip()
        variant = str(option.get("stored") or option.get("button") or "").strip()
        if not variant:
            return base
        if base and variant.lower().startswith(base.lower()):
            return variant
        if base:
            return f"{base} — {variant}"
        return variant

    def _french_variant_keyboard(
        self, variants: Sequence[dict[str, str]]
    ) -> "InlineKeyboardMarkup":
        buttons = [
            [InlineKeyboardButton(option["button"], callback_data=f"reg_variant:{index}")]
            for index, option in enumerate(variants)
        ]
        buttons.append([InlineKeyboardButton(self.BACK_BUTTON, callback_data="reg_variant:back")])
        return InlineKeyboardMarkup(buttons)

    def _about_inline_keyboard(self) -> "InlineKeyboardMarkup":
        programs = self._program_catalog()
        buttons = [
            [
                InlineKeyboardButton(
                    program.get("title", f"Направление {index + 1}"),
                    callback_data=f"about:{index}",
                )
            ]
            for index, program in enumerate(programs)
        ]
        if not buttons:
            buttons = [[InlineKeyboardButton(self.BACK_BUTTON, callback_data="about:back")]]
        return InlineKeyboardMarkup(buttons)

    def _about_french_variant_keyboard(
        self, program_index: int, variants: Sequence[dict[str, str]]
    ) -> "InlineKeyboardMarkup":
        buttons = [
            [
                InlineKeyboardButton(
                    option["button"],
                    callback_data=f"about_variant:{program_index}:{index}",
                )
            ]
            for index, option in enumerate(variants)
        ]
        buttons.append([InlineKeyboardButton(self.BACK_BUTTON, callback_data="about_variant:back")])
        return InlineKeyboardMarkup(buttons)

    def _teacher_inline_keyboard(self) -> "InlineKeyboardMarkup":
        teachers = self._teacher_directory()
        buttons = [
            [
                InlineKeyboardButton(
                    teacher.get("name", f"Педагог {index + 1}"),
                    callback_data=f"teacher:{teacher['id']}",
                )
            ]
            for index, teacher in enumerate(teachers)
        ]
        if not buttons:
            buttons = [[InlineKeyboardButton(self.BACK_BUTTON, callback_data="teacher:back")]]
        return InlineKeyboardMarkup(buttons)

    def _format_program_details(self, program: Dict[str, Any]) -> str:
        title = str(program.get("title", ""))
        body = str(program.get("body", ""))
        lines: list[str] = []
        if title:
            lines.append(title)
        if body.strip():
            if lines:
                lines.append("")
            lines.append(body.strip())
        return "\n".join(lines).strip()

    async def _registration_prompt_program_buttons(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        if not self._program_catalog():
            await self._reply(
                update,
                "Список направлений временно недоступен. Пожалуйста, свяжитесь с администратором.",
                reply_markup=self._main_menu_markup_for(update, context),
            )
            context.user_data.pop("registration", None)
            return ConversationHandler.END
        await self._reply(
            update,
            self._registration_program_prompt(),
            reply_markup=self._program_inline_keyboard(),
            prefer_edit=update.callback_query is not None,
        )
        return self.REGISTRATION_PROGRAM

    async def _registration_prompt_french_variant(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        *,
        details: Optional[str] = None,
        variants: Sequence[dict[str, str]],
    ) -> None:
        lines: list[str] = []
        if details and details.strip():
            lines.append("Вы выбрали программу:")
            lines.append(details.strip())
            lines.append("")
        lines.append("Выберите подходящую группу для занятий.")
        await self._reply(
            update,
            "\n".join(lines),
            reply_markup=self._french_variant_keyboard(variants),
            prefer_edit=update.callback_query is not None,
        )

    def _prefill_registration_defaults(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        defaults = self._get_user_defaults(update.effective_user)
        if not defaults:
            return
        registration = context.user_data.setdefault("registration", {})
        for key in ("child_name", "school", "class", "contact_name", "phone", "comment"):
            value = defaults.get(key)
            if value and not registration.get(key):
                registration[key] = value

    async def _registration_collect_program(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        query = update.callback_query
        message = update.message

        program_label = ""
        details = ""
        programs = self._program_catalog()
        selected_program: Optional[dict[str, Any]] = None
        if query is not None:
            data = query.data or ""
            try:
                index = int(data.split(":", 1)[1])
            except (IndexError, ValueError):
                await query.answer("Не удалось определить программу.", show_alert=True)
                return self.REGISTRATION_PROGRAM
            if not 0 <= index < len(programs):
                await query.answer("Программа недоступна.", show_alert=True)
                return self.REGISTRATION_PROGRAM
            program = programs[index]
            await query.answer()
            program_label = str(program.get("title", ""))
            selected_program = program
        else:
            program_label = (message.text if message else "").strip()
            program = next(
                (item for item in programs if str(item.get("title", "")).strip() == program_label),
                None,
            )
            if not program:
                await self._registration_prompt_program_buttons(update, context)
                return self.REGISTRATION_PROGRAM
            selected_program = program
        if selected_program is not None:
            details = self._format_program_details(selected_program)
        if query is None and details:
            await self._reply(update, f"Вы выбрали программу:\n{details}")

        registration = context.user_data.setdefault("registration", {})
        registration.pop("teacher", None)

        variants = self._program_variants(selected_program)

        if variants:
            registration["program_base"] = program_label
            registration["program_variants"] = [dict(option) for option in variants]
            registration.pop("program", None)
            await self._registration_prompt_french_variant(
                update,
                context,
                details=details,
                variants=variants,
            )
            return self.REGISTRATION_PROGRAM

        if query is not None and details:
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

        registration.pop("program_base", None)
        registration["program"] = program_label
        self._prefill_registration_defaults(update, context)

        return await self._registration_prompt_child_name(update, context)

    async def _registration_collect_program_variant(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        query = update.callback_query
        if query is None:
            return self.REGISTRATION_PROGRAM

        data = query.data or ""
        try:
            index = int(data.split(":", 1)[1])
        except (IndexError, ValueError):
            await query.answer("Не удалось определить группу.", show_alert=True)
            return self.REGISTRATION_PROGRAM
        registration = context.user_data.setdefault("registration", {})
        stored_variants = registration.get("program_variants")
        variants_list: list[dict[str, str]] = []
        if isinstance(stored_variants, list):
            for option in stored_variants:
                if isinstance(option, dict) and option.get("button"):
                    variants_list.append({
                        "button": str(option.get("button", "")),
                        "stored": str(option.get("stored") or option.get("button") or ""),
                    })
        if not variants_list:
            programs = self._program_catalog()
            for program in programs:
                program_variants = self._program_variants(program)
                if not program_variants:
                    continue
                variants_list = [dict(option) for option in program_variants]
                registration["program_variants"] = [dict(option) for option in program_variants]
                break
        if not 0 <= index < len(variants_list):
            await query.answer("Группа недоступна.", show_alert=True)
            return self.REGISTRATION_PROGRAM

        option = variants_list[index]
        base_label = registration.pop("program_base", "")
        if not base_label:
            programs = self._program_catalog()
            for program in programs:
                variants = self._program_variants(program)
                if any(
                    candidate.get("button") == option.get("button")
                    and candidate.get("stored") == option.get("stored")
                    for candidate in variants
                ):
                    title_candidate = str(program.get("title", ""))
                    if title_candidate:
                        base_label = title_candidate
                    break
        if not base_label:
            base_label = self.FRENCH_PROGRAM_LABEL
        registration["program"] = self._compose_french_variant_label(base_label, option)
        registration.pop("program_variants", None)

        await self._reply(
            update,
            f"Вы выбрали программу:\n{registration['program']}",
            prefer_edit=True,
        )

        self._prefill_registration_defaults(update, context)

        return await self._registration_prompt_child_name(update, context)

    async def _registration_variant_back_to_program(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        query = update.callback_query
        if query is not None:
            await query.answer()
        registration = context.user_data.setdefault("registration", {})
        for key in (
            "program",
            "program_base",
            "program_variants",
            "teacher",
            "child_name",
            "school",
            "class",
            "contact_name",
            "phone",
            "comment",
        ):
            registration.pop(key, None)
        return await self._registration_prompt_program_buttons(update, context)

    async def _registration_prompt_child_name(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, *, remind: bool = False
    ) -> int:
        registration = context.user_data.setdefault("registration", {})
        program = registration.get("program", "направление")
        if remind and registration.get("child_name"):
            message = (
                f"Сейчас указано имя: {registration.get('child_name', '—')}.")
            message += "\nВведите имя и фамилию ребёнка для записи."
        else:
            message = (
                f"Отлично! Напишите, пожалуйста, имя и фамилию ребёнка для "
                f"участия в программе «{program}»."
            )
        await self._reply(update, message, reply_markup=self._back_keyboard())
        return self.REGISTRATION_CHILD_NAME

    async def _registration_prompt_school(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, *, remind: bool = False
    ) -> int:
        registration = context.user_data.setdefault("registration", {})
        child_name = registration.get("child_name", "—")
        if remind and registration.get("school"):
            message = (
                f"Участник: {child_name}.\n"
                f"Сейчас указана школа: {registration.get('school', '—')}.")
            message += "\nУточните школу ребёнка."
        else:
            message = (
                f"Имя участника: {child_name}.\n"
                "Укажите, пожалуйста, школу ребёнка."
            )
        await self._reply(update, message, reply_markup=self._back_keyboard())
        return self.REGISTRATION_SCHOOL

    async def _registration_prompt_class(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, *, remind: bool = False
    ) -> int:
        registration = context.user_data.setdefault("registration", {})
        child_name = registration.get("child_name", "—")
        school = registration.get("school", "—")
        if remind and registration.get("class"):
            message = (
                f"Имя участника: {child_name}.\n"
                f"Школа: {school}.\n"
                f"Текущий класс: {registration.get('class', '—')}.")
            message += "\nУкажите актуальный класс."
        else:
            message = (
                f"Мы записали: {child_name}, школа {school}.\n"
                "Напишите, пожалуйста, класс ребёнка."
            )
        await self._reply(update, message, reply_markup=self._back_keyboard())
        return self.REGISTRATION_CLASS

    async def _registration_prompt_contact_name(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, *, remind: bool = False
    ) -> int:
        registration = context.user_data.setdefault("registration", {})
        if remind and registration.get("contact_name"):
            message = (
                f"Сейчас указано контактное лицо: {registration.get('contact_name', '—')}.")
            message += "\nВведите имя человека для связи."
        else:
            message = "Укажите имя контактного лица для связи."
        await self._reply(update, message, reply_markup=self._back_keyboard())
        return self.REGISTRATION_CONTACT_NAME

    async def _registration_prompt_phone(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, *, remind: bool = False
    ) -> int:
        registration = context.user_data.setdefault("registration", {})
        contact = registration.get("contact_name", "—")
        if remind and registration.get("phone"):
            message = (
                f"Контактное лицо: {contact}.\n"
                f"Сейчас указан номер: {registration.get('phone', '—')}.")
            message += "\nВведите актуальный номер телефона."
        else:
            message = (
                f"Контактное лицо: {contact}.\n"
                "Введите номер телефона для связи."
            )
        await self._reply(update, message, reply_markup=self._phone_keyboard())
        return self.REGISTRATION_PHONE

    async def _registration_prompt_comment(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, *, remind: bool = False
    ) -> int:
        registration = context.user_data.setdefault("registration", {})
        if remind and registration.get("comment"):
            message = (
                f"Текущий комментарий: {registration.get('comment', '—')}.")
            message += "\nЕсли комментарий не нужен, напишите «Нет»."
        else:
            message = (
                "Добавьте комментарий или особые пожелания. Если нет особых пожеланий, "
                "просто напишите: «Нет»."
            )
        await self._reply(update, message, reply_markup=self._back_keyboard())
        return self.REGISTRATION_COMMENT

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
        for key in (
            "program",
            "program_base",
            "teacher",
            "child_name",
            "school",
            "class",
            "contact_name",
            "phone",
            "comment",
        ):
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
        registration = context.user_data.setdefault("registration", {})
        for key in ("child_name", "school", "class", "contact_name", "phone", "comment"):
            registration.pop(key, None)
        return await self._registration_prompt_child_name(update, context, remind=True)

    async def _registration_back_to_school(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        registration = context.user_data.setdefault("registration", {})
        for key in ("school", "class", "contact_name", "phone", "comment"):
            registration.pop(key, None)
        return await self._registration_prompt_school(update, context, remind=True)

    async def _registration_back_to_class(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        registration = context.user_data.setdefault("registration", {})
        for key in ("class", "contact_name", "phone", "comment"):
            registration.pop(key, None)
        return await self._registration_prompt_class(update, context, remind=True)

    async def _registration_back_to_contact(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        registration = context.user_data.setdefault("registration", {})
        for key in ("contact_name", "phone", "comment"):
            registration.pop(key, None)
        return await self._registration_prompt_contact_name(update, context, remind=True)

    async def _registration_back_to_phone(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        registration = context.user_data.setdefault("registration", {})
        for key in ("phone", "comment"):
            registration.pop(key, None)
        return await self._registration_prompt_phone(update, context, remind=True)

    async def _registration_collect_child_name(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        text = (update.message.text or "").strip()
        if text == self.MAIN_MENU_BUTTON:
            return await self._registration_cancel(update, context)
        if text == self.BACK_BUTTON:
            return await self._registration_back_to_program(update, context)
        context.user_data.setdefault("registration", {})["child_name"] = text
        return await self._registration_prompt_school(update, context)

    async def _registration_collect_school(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        text = (update.message.text or "").strip()
        if text == self.MAIN_MENU_BUTTON:
            return await self._registration_cancel(update, context)
        if text == self.BACK_BUTTON:
            return await self._registration_back_to_child_name(update, context)
        context.user_data.setdefault("registration", {})["school"] = text
        return await self._registration_prompt_class(update, context)

    async def _registration_collect_class(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        text = (update.message.text or "").strip()
        if text == self.MAIN_MENU_BUTTON:
            return await self._registration_cancel(update, context)
        if text == self.BACK_BUTTON:
            return await self._registration_back_to_school(update, context)
        context.user_data.setdefault("registration", {})["class"] = text
        return await self._registration_prompt_contact_name(update, context)

    async def _registration_collect_contact_name(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        text = (update.message.text or "").strip()
        if text == self.MAIN_MENU_BUTTON:
            return await self._registration_cancel(update, context)
        if text == self.BACK_BUTTON:
            return await self._registration_back_to_class(update, context)
        context.user_data.setdefault("registration", {})["contact_name"] = text
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

    def _payment_keyboard(self) -> ReplyKeyboardMarkup:
        keyboard = [[KeyboardButton(self.BACK_BUTTON), KeyboardButton(self.MAIN_MENU_BUTTON)]]
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)

    def _absence_intro(self) -> str:
        return (
            "Выберите направление, по которому хотите сообщить об отсутствии.\n\n"
            "⚠️К сожалению, в студии не предусмотрены компенсации и отработки, "
            "так как языковые группы небольшие. Если занятие состоялось, оно подлежит оплате."
        )

    def _absence_program_keyboard(self) -> "InlineKeyboardMarkup":
        programs = self._program_catalog()
        buttons = [
            [
                InlineKeyboardButton(
                    program.get("title", f"Направление {index + 1}"),
                    callback_data=f"absence_program:{index}",
                )
            ]
            for index, program in enumerate(programs)
        ]
        buttons.append([InlineKeyboardButton(self.BACK_BUTTON, callback_data="absence_back:menu")])
        return InlineKeyboardMarkup(buttons)

    async def _absence_prompt_contact(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        await self._reply(
            update,
            "Напишите фамилию, имя и при необходимости отчество ребёнка, который "
            "пропустит занятие.",
            reply_markup=self._back_keyboard(),
        )
        return self.CANCELLATION_CONTACT

    async def _registration_collect_phone_text(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        text = (update.message.text or "").strip()
        if text == self.MAIN_MENU_BUTTON:
            return await self._registration_cancel(update, context)
        if text == self.BACK_BUTTON:
            return await self._registration_back_to_contact(update, context)
        context.user_data.setdefault("registration", {})["phone"] = text
        return await self._registration_prompt_comment(update, context)

    async def _registration_collect_comment(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        text = (update.message.text or "").strip()
        if text == self.MAIN_MENU_BUTTON:
            return await self._registration_cancel(update, context)
        if text == self.BACK_BUTTON:
            return await self._registration_back_to_phone(update, context)
        context.user_data.setdefault("registration", {})["comment"] = text
        await self._send_registration_summary(update, context)
        await self._show_main_menu(update, context)
        return ConversationHandler.END

    # ------------------------------------------------------------------
    # Payment report conversation

    def _payment_report_intro(self) -> str:
        return (
            "Выберите направление, за которое хотите сообщить об оплате.\n"
            "Нажмите на кнопку ниже, чтобы выбрать программу."
        )

    def _payment_program_catalog(self) -> list[dict[str, str]]:
        return [{"title": title} for title in self.PAYMENT_PROGRAM_OPTIONS]

    def _payment_program_keyboard(self) -> "InlineKeyboardMarkup":
        programs = self._payment_program_catalog()
        buttons = [
            [
                InlineKeyboardButton(
                    program.get("title", f"Направление {index + 1}"),
                    callback_data=f"pay_program:{index}",
                )
            ]
            for index, program in enumerate(programs)
        ]
        buttons.append([InlineKeyboardButton(self.BACK_BUTTON, callback_data="pay_back:menu")])
        return InlineKeyboardMarkup(buttons)

    async def _start_payment_report(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        self._remember_chat(update, context)
        context.user_data["payment_report"] = {}
        if not self._payment_program_catalog():
            await self._reply(
                update,
                "Список направлений временно недоступен. Пожалуйста, свяжитесь с администратором.",
                reply_markup=self._main_menu_markup_for(update, context),
            )
            context.user_data.pop("payment_report", None)
            return ConversationHandler.END
        await self._reply(
            update,
            self._payment_report_intro(),
            reply_markup=self._payment_program_keyboard(),
        )
        return self.PAYMENT_REPORT_PROGRAM

    async def _payment_report_prompt_program(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        if not self._payment_program_catalog():
            await self._reply(
                update,
                "Список направлений временно недоступен. Пожалуйста, свяжитесь с администратором.",
                reply_markup=self._main_menu_markup_for(update, context),
            )
            context.user_data.pop("payment_report", None)
            return ConversationHandler.END
        await self._reply(
            update,
            self._payment_report_intro(),
            reply_markup=self._payment_program_keyboard(),
            prefer_edit=update.callback_query is not None,
        )
        return self.PAYMENT_REPORT_PROGRAM

    async def _payment_report_collect_program(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        query = update.callback_query
        if query is None:
            return await self._payment_report_prompt_program(update, context)

        data = query.data or ""
        try:
            index = int(data.split(":", 1)[1])
        except (IndexError, ValueError):
            await query.answer("Не удалось определить направление.", show_alert=True)
            return self.PAYMENT_REPORT_PROGRAM

        programs = self._payment_program_catalog()
        if not 0 <= index < len(programs):
            await query.answer("Направление недоступно.", show_alert=True)
            return self.PAYMENT_REPORT_PROGRAM

        program = programs[index]
        await query.answer()
        title = str(program.get("title", f"Направление {index + 1}"))
        try:  # pragma: no cover - depends on telegram runtime
            await query.edit_message_reply_markup(None)
        except Exception:
            pass
        context.user_data.setdefault("payment_report", {})["program"] = title
        return await self._payment_report_prompt_name(update, context)

    async def _payment_report_cancel_from_program(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        query = update.callback_query
        if query is not None:
            await query.answer()
        await self._payment_report_cancel(update, context)
        return ConversationHandler.END

    async def _payment_report_prompt_name(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        *,
        remind: bool = False,
    ) -> int:
        data = context.user_data.setdefault("payment_report", {})
        program = data.get("program", "направление")
        if remind and data.get("full_name"):
            message = (
                f"Сейчас указано имя: {data.get('full_name', '—')}.\n"
                "Введите фамилию и имя плательщика ещё раз."
            )
        else:
            message = (
                f"Вы выбрали: {program}.\n"
                "Напишите, пожалуйста, фамилию и имя плательщика."
            )
        await self._reply(update, message, reply_markup=self._back_keyboard())
        return self.PAYMENT_REPORT_NAME

    async def _payment_report_collect_name(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        text = (update.message.text or "").strip()
        if text == self.MAIN_MENU_BUTTON:
            return await self._payment_report_cancel(update, context)
        if text == self.BACK_BUTTON:
            return await self._payment_report_back_to_program(update, context)
        if not text:
            await self._reply(
                update,
                "Пожалуйста, укажите фамилию и имя плательщика.",
                reply_markup=self._back_keyboard(),
            )
            return self.PAYMENT_REPORT_NAME
        context.user_data.setdefault("payment_report", {})["full_name"] = text
        return await self._payment_report_prompt_media(update, context)

    async def _payment_report_back_to_program(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        context.user_data.setdefault("payment_report", {}).pop("program", None)
        return await self._payment_report_prompt_program(update, context)

    async def _payment_report_prompt_media(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        message = (
            "Загрузите фото или скан подтверждения оплаты.\n\n"
            "Можно прикрепить несколько изображений, если нужно."
        )
        await self._reply(
            update,
            message,
            reply_markup=self._payment_keyboard(),
        )
        return self.PAYMENT_REPORT_MEDIA

    async def _payment_report_back_to_name(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        context.user_data.setdefault("payment_report", {}).pop("attachments", None)
        return await self._payment_report_prompt_name(update, context, remind=True)

    async def _payment_report_collect_media(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        data = context.user_data.setdefault("payment_report", {})
        text, attachments = self._extract_message_payload(update.message)

        if text == self.MAIN_MENU_BUTTON:
            return await self._payment_report_cancel(update, context)

        if text == self.BACK_BUTTON:
            return await self._payment_report_back_to_name(update, context)

        if not attachments:
            await self._reply(
                update,
                "Пожалуйста, прикрепите фото подтверждения оплаты.",
                reply_markup=self._payment_keyboard(),
            )
            return self.PAYMENT_REPORT_MEDIA

        if not any(item.kind == "photo" for item in attachments):
            await self._reply(
                update,
                "Нужно отправить хотя бы одну фотографию чека или квитанции.",
                reply_markup=self._payment_keyboard(),
            )
            return self.PAYMENT_REPORT_MEDIA

        serialised = await self._serialise_payment_media(context, attachments)
        data["attachments"] = serialised
        await self._complete_payment_report(update, context, attachments)
        return ConversationHandler.END

    async def _payment_report_cancel(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        context.user_data.pop("payment_report", None)
        await self._reply(
            update,
            "Сообщение об оплате отменено.",
            reply_markup=self._main_menu_markup_for(update, context),
        )
        return ConversationHandler.END

    async def _complete_payment_report(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        attachments: list[MediaAttachment],
    ) -> None:
        data = context.user_data.setdefault("payment_report", {})
        stored = self._store_payment_report(update, context, data, data.get("attachments", []))
        confirmation = (
            "Спасибо! Мы зафиксировали подтверждение оплаты.\n\n"
            f"📚 Направление: {stored.get('program', '—')}\n"
            f"👤 Плательщик: {stored.get('full_name', '—')}\n"
            f"🕒 Отправлено: {stored.get('created_at', '—')}"
        )
        await self._reply(
            update,
            confirmation,
            reply_markup=self._main_menu_markup_for(update, context),
        )
        admin_message = (
            "💳 Новое подтверждение оплаты\n"
            f"📚 Направление: {stored.get('program', '—')}\n"
            f"👤 Плательщик: {stored.get('full_name', '—')}\n"
            f"🕒 Отправлено: {stored.get('created_at', '—')}\n"
            f"👤 Отправил: {stored.get('submitted_by', '—')}"
        )
        await self._notify_admins(context, admin_message, media=attachments or None)
        context.user_data.pop("payment_report", None)
        await self._show_main_menu(update, context)

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
        context.user_data["absence"] = {}
        if not self._program_catalog():
            await self._reply(
                update,
                "Список направлений временно недоступен. Пожалуйста, свяжитесь с администратором.",
                reply_markup=self._main_menu_markup_for(update, context),
            )
            context.user_data.pop("absence", None)
            return ConversationHandler.END
        await self._reply(
            update,
            self._absence_intro(),
            reply_markup=self._absence_program_keyboard(),
        )
        return self.CANCELLATION_PROGRAM

    async def _cancellation_prompt_program(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        if not self._program_catalog():
            await self._reply(
                update,
                "Список направлений временно недоступен. Пожалуйста, свяжитесь с администратором.",
                reply_markup=self._main_menu_markup_for(update, context),
            )
            context.user_data.pop("absence", None)
            return ConversationHandler.END
        await self._reply(
            update,
            self._absence_intro(),
            reply_markup=self._absence_program_keyboard(),
            prefer_edit=update.callback_query is not None,
        )
        return self.CANCELLATION_PROGRAM

    async def _cancellation_collect_program(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        query = update.callback_query
        if query is None or not query.data:
            return await self._cancellation_prompt_program(update, context)
        try:
            index = int(query.data.split(":", 1)[1])
        except (IndexError, ValueError):
            return await self._cancellation_prompt_program(update, context)
        programs = self._program_catalog()
        if not 0 <= index < len(programs):
            return await self._cancellation_prompt_program(update, context)

        await query.answer()

        program = programs[index]
        data = context.user_data.setdefault("absence", {})
        data.clear()
        data["program"] = str(program.get("title", ""))

        try:  # pragma: no cover - depends on telegram runtime
            await query.edit_message_reply_markup(None)
        except Exception:
            pass

        return await self._absence_prompt_contact(update, context)

    async def _cancellation_cancel_from_program(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        return await self._cancellation_cancel(update, context)

    async def _cancellation_back_to_program(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        data = context.user_data.setdefault("absence", {})
        for key in ("child_name",):
            data.pop(key, None)
        data.pop("program", None)
        return await self._cancellation_prompt_program(update, context)

    async def _cancellation_collect_contact(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        text = (update.message.text or "").strip()
        if text == self.MAIN_MENU_BUTTON:
            return await self._cancellation_cancel(update, context)
        if text == self.BACK_BUTTON:
            return await self._cancellation_back_to_program(update, context)

        if not text:
            await self._reply(
                update,
                "Пожалуйста, укажите фамилию и имя ребёнка, который пропустит занятие.",
                reply_markup=self._back_keyboard(),
            )
            return self.CANCELLATION_CONTACT

        data = context.user_data.setdefault("absence", {})
        data["child_name"] = text
        return await self._complete_absence_report(update, context)

    async def _complete_absence_report(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        data = context.user_data.setdefault("absence", {})

        confirmation = "✅ Спасибо! Мы зафиксировали отсутствие."
        await self._reply(
            update,
            confirmation,
            reply_markup=self._main_menu_markup_for(update, context),
        )

        admin_message = (
            "🚨 Сообщение об отсутствии\n"
            f"📚 Направление: {data.get('program', '—')}\n"
            f"👦 Ребёнок: {data.get('child_name', '—')}"
        )
        await self._notify_admins(context, admin_message)

        context.user_data.pop("absence", None)
        await self._show_main_menu(update, context)
        return ConversationHandler.END

    async def _cancellation_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        context.user_data.pop("absence", None)
        await self._reply(
            update,
            "Сообщение об отсутствии не отправлено.",
            reply_markup=self._main_menu_markup_for(update, context),
        )
        return ConversationHandler.END

    async def _send_registration_summary(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        data = context.user_data.get("registration", {})
        program = data.get("program", "—")
        child = data.get("child_name", "—")
        school = data.get("school", "—")
        child_class = data.get("class", "—")
        contact = data.get("contact_name", "—")
        phone = data.get("phone", "—")
        comment = data.get("comment", "—")
        summary_lines = [
            "Заявка отправлена!",
            "",
            f"📚 Программа: {program}",
            f"👦 Ребёнок: {child}",
            f"🏫 Школа: {school}",
            f"🎓 Класс: {child_class}",
            f"👤 Контактное лицо: {contact}",
            f"📱 Телефон: {phone}",
        ]
        if comment and comment.strip():
            summary_lines.append(f"📝 Комментарий: {comment}")
        summary_lines.append("")
        summary_lines.append("Мы свяжемся с вами в ближайшее время.")

        await self._reply(
            update,
            "\n".join(summary_lines),
            reply_markup=self._main_menu_markup_for(update, context),
        )
        record = self._store_registration(update, context, data)

        admin_lines = [
            "🆕 Новая заявка",
            f"📚 Программа: {program}",
            f"👦 Ребёнок: {child}",
            f"🏫 Школа: {school}",
            f"🎓 Класс: {child_class}",
            f"👤 Контактное лицо: {contact}",
            f"📱 Телефон: {phone}",
        ]
        if comment and comment.strip():
            admin_lines.append(f"📝 Комментарий: {comment}")

        await self._notify_admins(context, "\n".join(admin_lines))
        context.user_data.pop("registration", None)

    async def _admin_show_about_menu(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        *,
        notice: Optional[str] = None,
        prefer_edit: bool = False,
    ) -> None:
        programs = self._program_catalog()
        lines: list[str] = []
        if notice:
            lines.append(notice)
            lines.append("")
        lines.append("Управление разделом «О студии».")
        if programs:
            lines.append("Выберите действие, чтобы обновить вступление или направления.")
        else:
            lines.append("Список направлений пуст — добавьте новое направление.")
        keyboard: list[list[InlineKeyboardButton]] = [
            [InlineKeyboardButton("📝 Редактировать вступление", callback_data="admin_about:intro")],
            [InlineKeyboardButton("➕ Добавить направление", callback_data="admin_about:add")],
        ]
        for index, program in enumerate(programs):
            title = program.get("title") or f"Направление {index + 1}"
            keyboard.append(
                [InlineKeyboardButton(title, callback_data=f"admin_about:edit:{index}")]
            )
        keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="admin_about:back")])
        await self._reply(
            update,
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup(keyboard),
            prefer_edit=prefer_edit or update.callback_query is not None,
        )

    async def _admin_show_program_detail(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        index: int,
        *,
        notice: Optional[str] = None,
        prefer_edit: bool = False,
    ) -> None:
        programs = self._program_catalog()
        effective_prefer_edit = prefer_edit or update.callback_query is not None
        if not 0 <= index < len(programs):
            await self._admin_show_about_menu(
                update,
                context,
                notice="Направление не найдено.",
                prefer_edit=effective_prefer_edit,
            )
            return
        program = programs[index]
        title = program.get("title") or f"Направление {index + 1}"
        body = str(program.get("body", ""))
        if program.get("photo_file_id"):
            photo_note = "Используется загруженное фото."
        elif program.get("photo_url"):
            photo_note = "Используется ссылка на фото."
        else:
            photo_note = "Фото не добавлено."
        lines: list[str] = []
        if notice:
            lines.append(notice)
            lines.append("")
        lines.append(f"Название: {title or '—'}")
        if body.strip():
            lines.append("")
            lines.append(body.strip())
        else:
            lines.append("")
            lines.append("Описание пока не заполнено.")
        lines.append("")
        lines.append(f"📷 {photo_note}")
        lines.append("")
        lines.append("Выберите действие:")
        keyboard = [
            [InlineKeyboardButton("✏️ Изменить название", callback_data=f"admin_about:rename:{index}")],
            [InlineKeyboardButton("📝 Обновить описание", callback_data=f"admin_about:body:{index}")],
            [InlineKeyboardButton("🖼 Обновить фото", callback_data=f"admin_about:photo:{index}")],
            [InlineKeyboardButton("🗑 Удалить", callback_data=f"admin_about:delete:{index}")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="admin_about:menu")],
        ]
        if self._program_variants(program):
            keyboard.insert(
                1,
                [InlineKeyboardButton("🎯 Управлять группами", callback_data=f"admin_about:variants:{index}")],
            )
        await self._reply(
            update,
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup(keyboard),
            prefer_edit=effective_prefer_edit,
        )

    async def _admin_show_program_variants(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        index: int,
        *,
        notice: Optional[str] = None,
        prefer_edit: bool = False,
    ) -> None:
        programs = self._program_catalog()
        effective_prefer_edit = prefer_edit or update.callback_query is not None
        if not 0 <= index < len(programs):
            await self._admin_show_about_menu(
                update,
                context,
                notice="Направление не найдено.",
                prefer_edit=effective_prefer_edit,
            )
            return
        program = programs[index]
        variants = self._program_variants(program)
        if not variants:
            await self._admin_show_program_detail(
                update,
                context,
                index,
                notice="Для этого направления пока нет отдельных групп.",
                prefer_edit=effective_prefer_edit,
            )
            return

        title = program.get("title") or f"Направление {index + 1}"
        lines: list[str] = []
        if notice:
            lines.append(notice)
            lines.append("")
        lines.append(f"Группы направления «{title}».")
        lines.append("Выберите вариант, чтобы обновить название кнопки и подпись для таблиц.")
        lines.append("")
        for option in variants:
            button_label = option.get("button") or "Без названия"
            stored_label = option.get("stored") or button_label
            if stored_label == button_label:
                lines.append(f"• {button_label}")
            else:
                lines.append(f"• {button_label} (в таблицах: {stored_label})")

        keyboard = [
            [
                InlineKeyboardButton(
                    option.get("button") or f"Группа {idx + 1}",
                    callback_data=f"admin_about:variant:{index}:{idx}",
                )
            ]
            for idx, option in enumerate(variants)
        ]
        keyboard.append(
            [InlineKeyboardButton("⬅️ Назад", callback_data=f"admin_about:edit:{index}")]
        )

        await self._reply(
            update,
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup(keyboard),
            prefer_edit=effective_prefer_edit,
        )

    async def _admin_prompt_add_program(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        context.chat_data["pending_admin_action"] = {"type": "add_program"}
        message = (
            "Отправьте новое направление.\n"
            "Первая строка — название, далее описание.\n"
            "Можно приложить одно фото."
        )
        await self._reply(
            update,
            message + self.ADMIN_CANCEL_PROMPT,
            reply_markup=self._admin_action_keyboard(),
        )

    async def _admin_prompt_program_rename(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, index: int
    ) -> None:
        programs = self._program_catalog()
        if not 0 <= index < len(programs):
            await self._admin_show_about_menu(update, context, notice="Направление не найдено.")
            return
        title = programs[index].get("title") or f"Направление {index + 1}"
        context.chat_data["pending_admin_action"] = {
            "type": "rename_program",
            "index": index,
        }
        message = (
            f"Введите новое название для направления «{title}»."
            + self.ADMIN_CANCEL_PROMPT
        )
        await self._reply(
            update,
            message,
            reply_markup=self._admin_action_keyboard(),
        )

    async def _admin_prompt_program_body(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, index: int
    ) -> None:
        programs = self._program_catalog()
        if not 0 <= index < len(programs):
            await self._admin_show_about_menu(update, context, notice="Направление не найдено.")
            return
        title = programs[index].get("title") or f"Направление {index + 1}"
        context.chat_data["pending_admin_action"] = {
            "type": "program_body",
            "index": index,
        }
        message = (
            f"Отправьте новый текст для направления «{title}».\n"
            "Чтобы очистить описание, напишите «Удалить»."
            + self.ADMIN_CANCEL_PROMPT
        )
        await self._reply(
            update,
            message,
            reply_markup=self._admin_action_keyboard(),
        )

    async def _admin_prompt_program_photo(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, index: int
    ) -> None:
        programs = self._program_catalog()
        if not 0 <= index < len(programs):
            await self._admin_show_about_menu(update, context, notice="Направление не найдено.")
            return
        title = programs[index].get("title") or f"Направление {index + 1}"
        context.chat_data["pending_admin_action"] = {
            "type": "program_photo",
            "index": index,
        }
        message = (
            f"Пришлите новое фото для направления «{title}».\n"
            "Чтобы удалить изображение, напишите «Удалить».\n"
            "Можно отправить ссылку (http…)."
            + self.ADMIN_CANCEL_PROMPT
        )
        await self._reply(
            update,
            message,
            reply_markup=self._admin_action_keyboard(),
        )

    async def _admin_prompt_program_variant(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, index: int, variant_index: int
    ) -> None:
        programs = self._program_catalog()
        if not 0 <= index < len(programs):
            await self._admin_show_about_menu(update, context, notice="Направление не найдено.")
            return
        program = programs[index]
        variants = self._program_variants(program)
        if not 0 <= variant_index < len(variants):
            await self._admin_show_program_variants(
                update,
                context,
                index,
                notice="Группа не найдена.",
            )
            return
        title = program.get("title") or f"Направление {index + 1}"
        current_button = variants[variant_index].get("button") or ""
        current_stored = variants[variant_index].get("stored") or current_button
        context.chat_data["pending_admin_action"] = {
            "type": "variant_update",
            "program_index": index,
            "variant_index": variant_index,
        }
        message_parts = [
            f"Введите новый текст для группы «{current_button or f'Группа {variant_index + 1}'}» направления «{title}».",
            "Первая строка — подпись кнопки.",
            "Вторая строка (необязательно) — название для таблиц и заявок.",
        ]
        if current_stored and current_stored != current_button:
            message_parts.append(
                f"Сейчас в таблицах используется формулировка: {current_stored}."
            )
        message = "\n".join(message_parts) + self.ADMIN_CANCEL_PROMPT
        await self._reply(
            update,
            message,
            reply_markup=self._admin_action_keyboard(),
        )

    async def _admin_add_program(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        *,
        text: str,
        attachments: list[MediaAttachment],
    ) -> bool:
        trimmed = text.strip()
        if not trimmed:
            await self._reply(
                update,
                "Пожалуйста, укажите название направления.",
                reply_markup=self._admin_action_keyboard(),
            )
            return False
        lines = [line.strip() for line in trimmed.splitlines()]
        title = lines[0]
        if not title:
            await self._reply(
                update,
                "Название не может быть пустым.",
                reply_markup=self._admin_action_keyboard(),
            )
            return False
        body = "\n".join(line for line in lines[1:] if line).strip()
        programs = self._program_catalog()
        existing_ids = {
            str(item.get("id", ""))
            for item in programs
            if isinstance(item, dict) and item.get("id")
        }
        new_id = self._generate_catalog_identifier("prog", existing_ids)
        photo_file_id = self._select_photo_file_id(attachments)
        programs.append(
            {
                "id": new_id,
                "title": title,
                "body": body,
                "photo_file_id": photo_file_id or "",
                "photo_url": "",
                "code": "",
            }
        )
        self._save_persistent_state()
        await self._admin_show_about_menu(
            update,
            context,
            notice=f"Направление «{title}» добавлено.",
        )
        return True

    async def _admin_rename_program(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        index: int,
        *,
        text: str,
    ) -> bool:
        programs = self._program_catalog()
        if not 0 <= index < len(programs):
            await self._admin_show_about_menu(update, context, notice="Направление не найдено.")
            return True
        trimmed = text.strip()
        if not trimmed:
            await self._reply(
                update,
                "Название не может быть пустым.",
                reply_markup=self._admin_action_keyboard(),
            )
            return False
        programs[index]["title"] = trimmed
        self._save_persistent_state()
        await self._admin_show_program_detail(
            update,
            context,
            index,
            notice="Название обновлено.",
        )
        return True

    async def _admin_update_program_body(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        index: int,
        *,
        text: str,
    ) -> bool:
        programs = self._program_catalog()
        if not 0 <= index < len(programs):
            await self._admin_show_about_menu(update, context, notice="Направление не найдено.")
            return True
        trimmed = text.strip()
        lower = trimmed.lower()
        if trimmed and lower not in {"удалить", "нет", "очистить", "-"}:
            programs[index]["body"] = trimmed
            notice = "Описание обновлено."
        elif lower in {"удалить", "нет", "очистить", "-"}:
            programs[index]["body"] = ""
            notice = "Описание очищено."
        else:
            await self._reply(
                update,
                "Отправьте текст описания или напишите «Удалить».",
                reply_markup=self._admin_action_keyboard(),
            )
            return False
        self._save_persistent_state()
        await self._admin_show_program_detail(
            update,
            context,
            index,
            notice=notice,
        )
        return True

    async def _admin_update_program_photo(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        index: int,
        *,
        text: str,
        attachments: list[MediaAttachment],
    ) -> bool:
        programs = self._program_catalog()
        if not 0 <= index < len(programs):
            await self._admin_show_about_menu(update, context, notice="Направление не найдено.")
            return True
        program = programs[index]
        trimmed = text.strip()
        lower = trimmed.lower()
        photo_file_id = self._select_photo_file_id(attachments)
        notice: str
        if photo_file_id:
            program["photo_file_id"] = photo_file_id
            program["photo_url"] = ""
            notice = "Фото обновлено."
        elif trimmed.startswith("http"):
            program["photo_file_id"] = ""
            program["photo_url"] = trimmed
            notice = "Ссылка на фото обновлена."
        elif lower in {"удалить", "нет", "очистить", "-"}:
            program["photo_file_id"] = ""
            program["photo_url"] = ""
            notice = "Фото удалено."
        else:
            await self._reply(
                update,
                "Пришлите фото, ссылку или напишите «Удалить».",
                reply_markup=self._admin_action_keyboard(),
            )
            return False
        self._save_persistent_state()
        await self._admin_show_program_detail(
            update,
            context,
            index,
            notice=notice,
        )
        return True

    async def _admin_update_program_variant(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        program_index: int,
        variant_index: int,
        *,
        text: str,
    ) -> bool:
        programs = self._program_catalog()
        if not 0 <= program_index < len(programs):
            await self._admin_show_about_menu(update, context, notice="Направление не найдено.")
            return True
        program = programs[program_index]
        variants = self._program_variants(program)
        if not 0 <= variant_index < len(variants):
            await self._admin_show_program_variants(
                update,
                context,
                program_index,
                notice="Группа не найдена.",
                prefer_edit=True,
            )
            return True

        trimmed = text.strip()
        if not trimmed:
            await self._reply(
                update,
                "Пожалуйста, отправьте новый текст для группы.",
                reply_markup=self._admin_action_keyboard(),
            )
            return False

        lines = [line.strip() for line in trimmed.splitlines() if line.strip()]
        button_label = lines[0]
        stored_label = lines[1] if len(lines) > 1 else lines[0]

        program.setdefault("variants", [])
        program_variants = program["variants"]
        if not isinstance(program_variants, list):
            program_variants = []
            program["variants"] = program_variants

        while len(program_variants) < len(variants):
            program_variants.append({"button": "", "stored": ""})

        program_variants[variant_index] = {
            "button": button_label,
            "stored": stored_label or button_label,
        }

        self._save_persistent_state()
        await self._admin_show_program_variants(
            update,
            context,
            program_index,
            notice="Группа обновлена.",
            prefer_edit=True,
        )
        return True

    async def _admin_delete_program(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        index: int,
    ) -> None:
        programs = self._program_catalog()
        prefer_edit = update.callback_query is not None
        if not 0 <= index < len(programs):
            await self._admin_show_about_menu(
                update,
                context,
                notice="Направление не найдено.",
                prefer_edit=prefer_edit,
            )
            return
        removed = programs.pop(index)
        title = removed.get("title") or "Направление"
        self._save_persistent_state()
        await self._admin_show_about_menu(
            update,
            context,
            notice=f"«{title}» удалено.",
            prefer_edit=prefer_edit,
        )

    async def _admin_show_teachers_menu(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        *,
        notice: Optional[str] = None,
        prefer_edit: bool = False,
    ) -> None:
        teachers = self._teacher_directory()
        lines: list[str] = []
        if notice:
            lines.append(notice)
            lines.append("")
        lines.append("Управление разделом «Преподаватели».")
        if teachers:
            lines.append("Выберите педагога для редактирования или добавьте нового.")
        else:
            lines.append("Список преподавателей пуст — добавьте новую запись.")
        keyboard: list[list[InlineKeyboardButton]] = [
            [InlineKeyboardButton("📝 Редактировать вступление", callback_data="admin_teacher:intro")],
            [InlineKeyboardButton("➕ Добавить преподавателя", callback_data="admin_teacher:add")],
        ]
        for index, teacher in enumerate(teachers):
            name = teacher.get("name") or f"Педагог {index + 1}"
            keyboard.append(
                [InlineKeyboardButton(name, callback_data=f"admin_teacher:edit:{index}")]
            )
        keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="admin_teacher:back")])
        await self._reply(
            update,
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup(keyboard),
            prefer_edit=prefer_edit or update.callback_query is not None,
        )

    async def _admin_show_teacher_detail(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        index: int,
        *,
        notice: Optional[str] = None,
        prefer_edit: bool = False,
    ) -> None:
        teachers = self._teacher_directory()
        effective_prefer_edit = prefer_edit or update.callback_query is not None
        if not 0 <= index < len(teachers):
            await self._admin_show_teachers_menu(
                update,
                context,
                notice="Преподаватель не найден.",
                prefer_edit=effective_prefer_edit,
            )
            return
        teacher = teachers[index]
        name = teacher.get("name") or f"Педагог {index + 1}"
        bio = str(teacher.get("bio", ""))
        if teacher.get("photo_file_id"):
            photo_note = "Используется загруженное фото."
        elif teacher.get("photo_url"):
            photo_note = "Используется ссылка на фото."
        else:
            photo_note = "Фото не добавлено."
        lines: list[str] = []
        if notice:
            lines.append(notice)
            lines.append("")
        lines.append(f"Имя: {name}")
        if bio.strip():
            lines.append("")
            lines.append(bio.strip())
        else:
            lines.append("")
            lines.append("Описание пока не заполнено.")
        lines.append("")
        lines.append(f"📷 {photo_note}")
        lines.append("")
        lines.append("Выберите действие:")
        keyboard = [
            [InlineKeyboardButton("✏️ Изменить имя", callback_data=f"admin_teacher:rename:{index}")],
            [InlineKeyboardButton("📝 Обновить описание", callback_data=f"admin_teacher:bio:{index}")],
            [InlineKeyboardButton("🖼 Обновить фото", callback_data=f"admin_teacher:photo:{index}")],
            [InlineKeyboardButton("🗑 Удалить", callback_data=f"admin_teacher:delete:{index}")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="admin_teacher:menu")],
        ]
        await self._reply(
            update,
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup(keyboard),
            prefer_edit=effective_prefer_edit,
        )

    async def _admin_prompt_add_teacher(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        context.chat_data["pending_admin_action"] = {"type": "add_teacher"}
        message = (
            "Отправьте информацию о новом преподавателе.\n"
            "Первая строка — имя, далее описание.\n"
            "Можно приложить фото."
        )
        await self._reply(
            update,
            message + self.ADMIN_CANCEL_PROMPT,
            reply_markup=self._admin_action_keyboard(),
        )

    async def _admin_prompt_teacher_rename(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, index: int
    ) -> None:
        teachers = self._teacher_directory()
        if not 0 <= index < len(teachers):
            await self._admin_show_teachers_menu(update, context, notice="Преподаватель не найден.")
            return
        name = teachers[index].get("name") or f"Педагог {index + 1}"
        context.chat_data["pending_admin_action"] = {
            "type": "rename_teacher",
            "index": index,
        }
        message = (
            f"Введите новое имя для преподавателя «{name}»."
            + self.ADMIN_CANCEL_PROMPT
        )
        await self._reply(
            update,
            message,
            reply_markup=self._admin_action_keyboard(),
        )

    async def _admin_prompt_teacher_bio(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, index: int
    ) -> None:
        teachers = self._teacher_directory()
        if not 0 <= index < len(teachers):
            await self._admin_show_teachers_menu(update, context, notice="Преподаватель не найден.")
            return
        name = teachers[index].get("name") or f"Педагог {index + 1}"
        context.chat_data["pending_admin_action"] = {
            "type": "teacher_bio",
            "index": index,
        }
        message = (
            f"Отправьте новое описание для преподавателя «{name}».\n"
            "Чтобы очистить описание, напишите «Удалить»."
            + self.ADMIN_CANCEL_PROMPT
        )
        await self._reply(
            update,
            message,
            reply_markup=self._admin_action_keyboard(),
        )

    async def _admin_prompt_teacher_photo(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, index: int
    ) -> None:
        teachers = self._teacher_directory()
        if not 0 <= index < len(teachers):
            await self._admin_show_teachers_menu(update, context, notice="Преподаватель не найден.")
            return
        name = teachers[index].get("name") or f"Педагог {index + 1}"
        context.chat_data["pending_admin_action"] = {
            "type": "teacher_photo",
            "index": index,
        }
        message = (
            f"Пришлите новое фото для преподавателя «{name}».\n"
            "Чтобы удалить изображение, напишите «Удалить».\n"
            "Можно отправить ссылку (http…)."
            + self.ADMIN_CANCEL_PROMPT
        )
        await self._reply(
            update,
            message,
            reply_markup=self._admin_action_keyboard(),
        )

    async def _admin_add_teacher(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        *,
        text: str,
        attachments: list[MediaAttachment],
    ) -> bool:
        trimmed = text.strip()
        if not trimmed:
            await self._reply(
                update,
                "Пожалуйста, укажите имя преподавателя.",
                reply_markup=self._admin_action_keyboard(),
            )
            return False
        lines = [line.strip() for line in trimmed.splitlines()]
        name = lines[0]
        if not name:
            await self._reply(
                update,
                "Имя не может быть пустым.",
                reply_markup=self._admin_action_keyboard(),
            )
            return False
        bio = "\n".join(line for line in lines[1:] if line).strip()
        teachers = self._teacher_directory()
        existing_ids = {
            str(item.get("id", ""))
            for item in teachers
            if isinstance(item, dict) and item.get("id")
        }
        new_id = self._generate_catalog_identifier("teacher", existing_ids)
        photo_file_id = self._select_photo_file_id(attachments)
        teachers.append(
            {
                "id": new_id,
                "name": name,
                "bio": bio,
                "photo_file_id": photo_file_id or "",
                "photo_url": "",
            }
        )
        self._save_persistent_state()
        await self._admin_show_teachers_menu(
            update,
            context,
            notice=f"Преподаватель «{name}» добавлен.",
        )
        return True

    async def _admin_rename_teacher(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        index: int,
        *,
        text: str,
    ) -> bool:
        teachers = self._teacher_directory()
        if not 0 <= index < len(teachers):
            await self._admin_show_teachers_menu(update, context, notice="Преподаватель не найден.")
            return True
        trimmed = text.strip()
        if not trimmed:
            await self._reply(
                update,
                "Имя не может быть пустым.",
                reply_markup=self._admin_action_keyboard(),
            )
            return False
        teachers[index]["name"] = trimmed
        self._save_persistent_state()
        await self._admin_show_teacher_detail(
            update,
            context,
            index,
            notice="Имя обновлено.",
        )
        return True

    async def _admin_update_teacher_bio(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        index: int,
        *,
        text: str,
    ) -> bool:
        teachers = self._teacher_directory()
        if not 0 <= index < len(teachers):
            await self._admin_show_teachers_menu(update, context, notice="Преподаватель не найден.")
            return True
        trimmed = text.strip()
        lower = trimmed.lower()
        if trimmed and lower not in {"удалить", "нет", "очистить", "-"}:
            teachers[index]["bio"] = trimmed
            notice = "Описание обновлено."
        elif lower in {"удалить", "нет", "очистить", "-"}:
            teachers[index]["bio"] = ""
            notice = "Описание очищено."
        else:
            await self._reply(
                update,
                "Отправьте текст описания или напишите «Удалить».",
                reply_markup=self._admin_action_keyboard(),
            )
            return False
        self._save_persistent_state()
        await self._admin_show_teacher_detail(
            update,
            context,
            index,
            notice=notice,
        )
        return True

    async def _admin_update_teacher_photo(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        index: int,
        *,
        text: str,
        attachments: list[MediaAttachment],
    ) -> bool:
        teachers = self._teacher_directory()
        if not 0 <= index < len(teachers):
            await self._admin_show_teachers_menu(update, context, notice="Преподаватель не найден.")
            return True
        teacher = teachers[index]
        trimmed = text.strip()
        lower = trimmed.lower()
        photo_file_id = self._select_photo_file_id(attachments)
        notice: str
        if photo_file_id:
            teacher["photo_file_id"] = photo_file_id
            teacher["photo_url"] = ""
            notice = "Фото обновлено."
        elif trimmed.startswith("http"):
            teacher["photo_file_id"] = ""
            teacher["photo_url"] = trimmed
            notice = "Ссылка на фото обновлена."
        elif lower in {"удалить", "нет", "очистить", "-"}:
            teacher["photo_file_id"] = ""
            teacher["photo_url"] = ""
            notice = "Фото удалено."
        else:
            await self._reply(
                update,
                "Пришлите фото, ссылку или напишите «Удалить».",
                reply_markup=self._admin_action_keyboard(),
            )
            return False
        self._save_persistent_state()
        await self._admin_show_teacher_detail(
            update,
            context,
            index,
            notice=notice,
        )
        return True

    async def _admin_delete_teacher(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        index: int,
    ) -> None:
        teachers = self._teacher_directory()
        prefer_edit = update.callback_query is not None
        if not 0 <= index < len(teachers):
            await self._admin_show_teachers_menu(
                update,
                context,
                notice="Преподаватель не найден.",
                prefer_edit=prefer_edit,
            )
            return
        removed = teachers.pop(index)
        name = removed.get("name") or "Преподаватель"
        self._save_persistent_state()
        await self._admin_show_teachers_menu(
            update,
            context,
            notice=f"«{name}» удалён.",
            prefer_edit=prefer_edit,
        )

    async def _admin_about_callback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        query = update.callback_query
        if query is None:
            return
        if not self._is_admin_update(update, context):
            await query.answer("Недоступно.", show_alert=True)
            return
        parts = (query.data or "").split(":")
        action = parts[1] if len(parts) > 1 else ""
        argument = parts[2] if len(parts) > 2 else ""
        extra = parts[3] if len(parts) > 3 else ""

        def _parse_index(token: str) -> Optional[int]:
            try:
                return int(token)
            except (TypeError, ValueError):
                return None

        if action == "intro":
            await query.answer()
            await self._prompt_admin_content_edit(
                update,
                context,
                field="about",
                instruction="Отправьте обновлённый блок «О студии» (текст, фото, видео)."
                + self.ADMIN_CANCEL_PROMPT,
            )
            return
        if action == "add":
            await query.answer()
            await self._admin_prompt_add_program(update, context)
            return
        if action == "edit":
            index = _parse_index(argument)
            if index is None:
                await query.answer("Не удалось открыть направление.", show_alert=True)
                return
            await query.answer()
            await self._admin_show_program_detail(update, context, index, prefer_edit=True)
            return
        if action == "variants":
            index = _parse_index(argument)
            if index is None:
                await query.answer("Не удалось открыть список групп.", show_alert=True)
                return
            await query.answer()
            await self._admin_show_program_variants(update, context, index, prefer_edit=True)
            return
        if action == "variant":
            index = _parse_index(argument)
            variant_index = _parse_index(extra)
            if index is None or variant_index is None:
                await query.answer("Не удалось определить группу.", show_alert=True)
                return
            await query.answer()
            await self._admin_prompt_program_variant(update, context, index, variant_index)
            return
        if action == "rename":
            index = _parse_index(argument)
            if index is None:
                await query.answer("Не удалось определить направление.", show_alert=True)
                return
            await query.answer()
            await self._admin_prompt_program_rename(update, context, index)
            return
        if action == "body":
            index = _parse_index(argument)
            if index is None:
                await query.answer("Не удалось определить направление.", show_alert=True)
                return
            await query.answer()
            await self._admin_prompt_program_body(update, context, index)
            return
        if action == "photo":
            index = _parse_index(argument)
            if index is None:
                await query.answer("Не удалось определить направление.", show_alert=True)
                return
            await query.answer()
            await self._admin_prompt_program_photo(update, context, index)
            return
        if action == "delete":
            index = _parse_index(argument)
            if index is None:
                await query.answer("Не удалось определить направление.", show_alert=True)
                return
            await query.answer()
            await self._admin_delete_program(update, context, index)
            return
        if action == "menu":
            await query.answer()
            await self._admin_show_about_menu(update, context, prefer_edit=True)
            return
        if action == "back":
            await query.answer()
            await self._reply(
                update,
                "Выберите раздел админ-панели.",
                reply_markup=self._admin_menu_markup(),
                prefer_edit=True,
            )
            return
        await query.answer("Действие недоступно.", show_alert=True)

    async def _admin_teacher_callback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        query = update.callback_query
        if query is None:
            return
        if not self._is_admin_update(update, context):
            await query.answer("Недоступно.", show_alert=True)
            return
        parts = (query.data or "").split(":")
        action = parts[1] if len(parts) > 1 else ""
        argument = parts[2] if len(parts) > 2 else ""

        def _parse_index(token: str) -> Optional[int]:
            try:
                return int(token)
            except (TypeError, ValueError):
                return None

        if action == "intro":
            await query.answer()
            await self._prompt_admin_content_edit(
                update,
                context,
                field="teachers",
                instruction="Поделитесь новым описанием преподавателей и медиа."
                + self.ADMIN_CANCEL_PROMPT,
            )
            return
        if action == "add":
            await query.answer()
            await self._admin_prompt_add_teacher(update, context)
            return
        if action == "edit":
            index = _parse_index(argument)
            if index is None:
                await query.answer("Не удалось открыть карточку преподавателя.", show_alert=True)
                return
            await query.answer()
            await self._admin_show_teacher_detail(update, context, index, prefer_edit=True)
            return
        if action == "rename":
            index = _parse_index(argument)
            if index is None:
                await query.answer("Не удалось определить преподавателя.", show_alert=True)
                return
            await query.answer()
            await self._admin_prompt_teacher_rename(update, context, index)
            return
        if action == "bio":
            index = _parse_index(argument)
            if index is None:
                await query.answer("Не удалось определить преподавателя.", show_alert=True)
                return
            await query.answer()
            await self._admin_prompt_teacher_bio(update, context, index)
            return
        if action == "photo":
            index = _parse_index(argument)
            if index is None:
                await query.answer("Не удалось определить преподавателя.", show_alert=True)
                return
            await query.answer()
            await self._admin_prompt_teacher_photo(update, context, index)
            return
        if action == "delete":
            index = _parse_index(argument)
            if index is None:
                await query.answer("Не удалось определить преподавателя.", show_alert=True)
                return
            await query.answer()
            await self._admin_delete_teacher(update, context, index)
            return
        if action == "menu":
            await query.answer()
            await self._admin_show_teachers_menu(update, context, prefer_edit=True)
            return
        if action == "back":
            await query.answer()
            await self._reply(
                update,
                "Выберите раздел админ-панели.",
                reply_markup=self._admin_menu_markup(),
                prefer_edit=True,
            )
            return
        await query.answer("Действие недоступно.", show_alert=True)

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
            if command_text == self.ADMIN_MANAGE_ADMINS_BUTTON:
                context.chat_data["pending_admin_action"] = {"type": "manage_admins"}
                message = self._admin_manage_admins_instruction(context)
                await self._reply(
                    update,
                    message + self.ADMIN_CANCEL_PROMPT,
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
                context.chat_data.pop("pending_admin_action", None)
                await self._admin_show_about_menu(update, context)
                return
            if command_text == self.ADMIN_EDIT_TEACHERS_BUTTON:
                context.chat_data.pop("pending_admin_action", None)
                await self._admin_show_teachers_menu(update, context)
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
        if action_type == "add_program":
            if await self._admin_add_program(update, context, text=text, attachments=attachments):
                context.chat_data.pop("pending_admin_action", None)
            else:
                context.chat_data["pending_admin_action"] = pending
            return
        if action_type == "rename_program":
            index = pending.get("index")
            if isinstance(index, int) and await self._admin_rename_program(update, context, index, text=text):
                context.chat_data.pop("pending_admin_action", None)
            else:
                context.chat_data["pending_admin_action"] = pending
            return
        if action_type == "program_body":
            index = pending.get("index")
            if isinstance(index, int) and await self._admin_update_program_body(update, context, index, text=text):
                context.chat_data.pop("pending_admin_action", None)
            else:
                context.chat_data["pending_admin_action"] = pending
            return
        if action_type == "program_photo":
            index = pending.get("index")
            if isinstance(index, int) and await self._admin_update_program_photo(
                update,
                context,
                index,
                text=text,
                attachments=attachments,
            ):
                context.chat_data.pop("pending_admin_action", None)
            else:
                context.chat_data["pending_admin_action"] = pending
            return
        if action_type == "variant_update":
            program_index = pending.get("program_index")
            variant_index = pending.get("variant_index")
            if (
                isinstance(program_index, int)
                and isinstance(variant_index, int)
                and await self._admin_update_program_variant(
                    update,
                    context,
                    program_index,
                    variant_index,
                    text=text,
                )
            ):
                context.chat_data.pop("pending_admin_action", None)
            else:
                context.chat_data["pending_admin_action"] = pending
            return
        if action_type == "add_teacher":
            if await self._admin_add_teacher(update, context, text=text, attachments=attachments):
                context.chat_data.pop("pending_admin_action", None)
            else:
                context.chat_data["pending_admin_action"] = pending
            return
        if action_type == "rename_teacher":
            index = pending.get("index")
            if isinstance(index, int) and await self._admin_rename_teacher(update, context, index, text=text):
                context.chat_data.pop("pending_admin_action", None)
            else:
                context.chat_data["pending_admin_action"] = pending
            return
        if action_type == "teacher_bio":
            index = pending.get("index")
            if isinstance(index, int) and await self._admin_update_teacher_bio(update, context, index, text=text):
                context.chat_data.pop("pending_admin_action", None)
            else:
                context.chat_data["pending_admin_action"] = pending
            return
        if action_type == "teacher_photo":
            index = pending.get("index")
            if isinstance(index, int) and await self._admin_update_teacher_photo(
                update,
                context,
                index,
                text=text,
                attachments=attachments,
            ):
                context.chat_data.pop("pending_admin_action", None)
            else:
                context.chat_data["pending_admin_action"] = pending
            return
        if action_type == "manage_admins":
            await self._admin_manage_admins(update, context, text)
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

    async def _admin_manage_admins(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, payload: str
    ) -> None:
        self._refresh_admin_cache(context)
        tokens = [part.strip() for part in re.split(r"[\s,]+", payload or "") if part.strip()]
        operations: list[tuple[str, int]] = []
        invalid_tokens: list[str] = []

        for token in tokens:
            action = "add"
            value = token
            if token.startswith("+"):
                value = token[1:]
            elif token.startswith("-"):
                action = "remove"
                value = token[1:]
            if not value:
                invalid_tokens.append(token)
                continue
            try:
                admin_id = _coerce_chat_id(value)
            except ValueError:
                invalid_tokens.append(token)
                continue
            operations.append((action, admin_id))

        if not operations and not invalid_tokens:
            context.chat_data["pending_admin_action"] = {"type": "manage_admins"}
            message = (
                "Не удалось распознать chat_id. Пожалуйста, укажите номер без пробелов"
                " или используйте знаки + и - перед идентификатором."
            )
            message += "\n\n" + self._admin_manage_admins_instruction(context)
            await self._reply(
                update,
                message + self.ADMIN_CANCEL_PROMPT,
                reply_markup=self._admin_action_keyboard(),
            )
            return

        added: list[int] = []
        removed: list[int] = []
        skipped_existing: list[int] = []
        protected: list[int] = []
        missing: list[int] = []

        for action, admin_id in operations:
            if action == "add":
                if admin_id in self._runtime_admin_ids:
                    skipped_existing.append(admin_id)
                    continue
                self._store_dynamic_admin(context, admin_id)
                added.append(admin_id)
            else:
                if admin_id in self.admin_chat_ids:
                    protected.append(admin_id)
                    continue
                if self._remove_dynamic_admin(context, admin_id):
                    removed.append(admin_id)
                else:
                    missing.append(admin_id)

        summary_lines = ["Обновление списка администраторов завершено."]
        if added:
            summary_lines.append(
                "✅ Добавлены: " + ", ".join(str(item) for item in sorted(added))
            )
        if removed:
            summary_lines.append(
                "🗑 Удалены: " + ", ".join(str(item) for item in sorted(removed))
            )
        if skipped_existing:
            summary_lines.append(
                "ℹ️ Уже были администраторами: "
                + ", ".join(str(item) for item in sorted(skipped_existing))
            )
        if protected:
            summary_lines.append(
                "🔒 Нельзя удалить (зафиксированы в настройках бота): "
                + ", ".join(str(item) for item in sorted(protected))
            )
        if missing:
            summary_lines.append(
                "⚠️ Не найдены среди динамических администраторов: "
                + ", ".join(str(item) for item in sorted(missing))
            )
        if invalid_tokens:
            summary_lines.append(
                "❗️ Не удалось распознать: " + ", ".join(invalid_tokens)
            )

        summary_lines.append("")
        summary_lines.append(self._admin_manage_admins_instruction(context))

        context.chat_data["pending_admin_action"] = {"type": "manage_admins"}
        await self._reply(
            update,
            "\n".join(summary_lines) + self.ADMIN_CANCEL_PROMPT,
            reply_markup=self._admin_action_keyboard(),
        )

        if added or removed:
            updates = []
            if added:
                updates.append(
                    "Добавлены: " + ", ".join(str(item) for item in sorted(added))
                )
            if removed:
                updates.append(
                    "Удалены: " + ", ".join(str(item) for item in sorted(removed))
                )
            if updates:
                await self._notify_admins(
                    context,
                    "👑 Обновление прав администраторов:\n" + "\n".join(updates),
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
        registrations_data = self._application_data(context).get("registrations", [])
        if not isinstance(registrations_data, list):
            registrations = []
        else:
            registrations = [item for item in registrations_data if isinstance(item, dict)]

        payments_data = self._application_data(context).get("payments", [])
        if not isinstance(payments_data, list):
            payments = []
        else:
            payments = [item for item in payments_data if isinstance(item, dict)]

        if not registrations and not payments:
            await self._reply(
                update,
                "Заявок и сообщений об оплате пока нет.",
                reply_markup=self._admin_menu_markup(),
            )
            return

        bot_username = await self._ensure_bot_username(context)

        table_rows = self._build_registration_table_rows(
            registrations,
            bot_username=bot_username,
        )
        _export_path, generated_at = self._export_registrations_excel(
            context,
            table_rows,
        )
        sheet_result = await self._sync_google_sheet(
            table_rows,
            kind="registrations",
            column_widths=self.REGISTRATION_EXPORT_COLUMN_WIDTHS,
        )
        payment_rows = self._build_payment_report_table_rows(
            payments,
            bot_username=bot_username,
        )
        _payments_export_path, payments_generated_at = self._export_payments_excel(
            context,
            payment_rows,
        )
        payments_sheet_result = await self._sync_google_sheet(
            payment_rows,
            kind="payments",
            column_widths=self.PAYMENT_EXPORT_COLUMN_WIDTHS,
            spreadsheet_env=self.PAYMENTS_SPREADSHEET_ENV,
            default_spreadsheet_id=self.DEFAULT_PAYMENTS_SPREADSHEET_ID,
        )
        preview_lines = self._format_registrations_preview(registrations)

        message_parts = [
            "📊 Экспорт данных готов!\n",
            f"🗂 Заявок: {len(registrations)} (обновлено {generated_at})",
            f"💳 Сообщений об оплате: {len(payments)} (обновлено {payments_generated_at})",
        ]
        if preview_lines:
            message_parts.append("")
            message_parts.extend(preview_lines)
        if sheet_result.url:
            message_parts.append("")
            message_parts.append(f"🌐 Таблица заявок: {sheet_result.url}")
            if sheet_result.updated:
                message_parts.append(
                    "Таблица обновлена автоматически и содержит актуальные данные."
                )
            else:
                message_parts.append(
                    "⚠️ Не удалось обновить таблицу автоматически. Проверьте доступ Google Sheets; ссылка ведёт на последнюю доступную версию."
                )
        else:
            message_parts.append("")
            message_parts.append(
                "⚠️ Облачная таблица недоступна: проверьте настройки сервисного аккаунта."
            )
        if payments_sheet_result.url:
            message_parts.append("")
            message_parts.append(f"💳 Таблица оплат: {payments_sheet_result.url}")
            if payments_sheet_result.updated:
                message_parts.append(
                    "Таблица оплат обновлена автоматически."
                )
            else:
                message_parts.append(
                    "⚠️ Не удалось обновить таблицу оплат автоматически."
                )
        else:
            message_parts.append("")
            message_parts.append(
                "⚠️ Облачная таблица оплат недоступна: проверьте настройки сервисного аккаунта."
            )

        await self._reply(
            update,
            "\n".join(message_parts),
            reply_markup=self._admin_menu_markup(),
        )

    def _build_registration_table_rows(
        self,
        registrations: list[dict[str, Any]],
        *,
        bot_username: Optional[str],
    ) -> list[list[_XlsxCell]]:
        header = (
            "Дата заявки",
            "Программа",
            "Участник",
            "Школа",
            "Класс",
            "Контактное лицо",
            "Телефон",
            "Комментарий",
        )

        def make_cell(value: Any) -> _XlsxCell:
            if isinstance(value, _XlsxCell):
                return value
            if value is None:
                return _XlsxCell("")
            return _XlsxCell(str(value))

        rows: list[list[_XlsxCell]] = [
            [make_cell(title) for title in header]
        ]

        for record in registrations:
            rows.append(
                [
                    make_cell(record.get("created_at") or ""),
                    make_cell(record.get("program") or ""),
                    make_cell(record.get("child_name") or ""),
                    make_cell(record.get("school") or ""),
                    make_cell(record.get("class") or ""),
                    make_cell(record.get("contact_name") or ""),
                    make_cell(record.get("phone") or ""),
                    make_cell(record.get("comment") or ""),
                ]
            )

        return rows

    def _build_payment_report_table_rows(
        self,
        payments: list[dict[str, Any]],
        *,
        bot_username: Optional[str],
    ) -> list[list[_XlsxCell]]:
        header = (
            "Дата сообщения",
            "Направление",
            "Плательщик",
            "Фото оплаты",
            "Отправитель",
        )

        def make_cell(value: Any) -> _XlsxCell:
            if isinstance(value, _XlsxCell):
                return value
            if value is None:
                return _XlsxCell("")
            return _XlsxCell(str(value))

        rows: list[list[_XlsxCell]] = [[make_cell(title) for title in header]]

        for record in payments:
            attachments = self._dicts_to_attachments(record.get("attachments"))
            payment_id = str(record.get("id") or "")
            link_cell = self._build_payment_link_cell(
                bot_username=bot_username,
                registration_id=payment_id,
                attachments=attachments,
                payment_note="",
            )

            rows.append(
                [
                    make_cell(record.get("created_at") or ""),
                    make_cell(record.get("program") or ""),
                    make_cell(record.get("full_name") or ""),
                    make_cell(link_cell),
                    make_cell(record.get("submitted_by") or ""),
                ]
            )

        return rows

    def _export_registrations_excel(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        table_rows: Sequence[Sequence[_XlsxCell]],
    ) -> tuple[Path, str]:
        builder = _SimpleXlsxBuilder(
            sheet_name="Заявки",
            column_widths=self.REGISTRATION_EXPORT_COLUMN_WIDTHS,
        )

        for row in table_rows:
            builder.add_row(row)

        generated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        export_path = Path("data") / "exports" / "confetti_registrations.xlsx"
        builder.to_file(export_path)

        storage = self._application_data(context)
        exports_meta = storage.setdefault("exports", {})
        registrations_meta = {
            "generated_at": generated_at,
            "path": str(export_path),
        }
        if isinstance(exports_meta, dict):
            exports_meta["registrations"] = registrations_meta
        else:
            storage["exports"] = {"registrations": registrations_meta}

        self._save_persistent_state()

        return export_path, generated_at

    def _export_payments_excel(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        table_rows: Sequence[Sequence[_XlsxCell]],
    ) -> tuple[Path, str]:
        builder = _SimpleXlsxBuilder(
            sheet_name="Оплаты",
            column_widths=self.PAYMENT_EXPORT_COLUMN_WIDTHS,
        )

        for row in table_rows:
            builder.add_row(row)

        generated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        export_path = Path("data") / "exports" / "confetti_payments.xlsx"
        builder.to_file(export_path)

        storage = self._application_data(context)
        exports_meta = storage.setdefault("exports", {})
        payments_meta = {
            "generated_at": generated_at,
            "path": str(export_path),
        }
        if isinstance(exports_meta, dict):
            exports_meta["payments"] = payments_meta
        else:
            storage["exports"] = {"payments": payments_meta}

        self._save_persistent_state()

        return export_path, generated_at

    def _ensure_google_sheets_exporter(
        self,
        *,
        kind: str,
        spreadsheet_id: Optional[str] = None,
        spreadsheet_env: str = "CONFETTI_GOOGLE_SHEETS_ID",
        default_spreadsheet_id: Optional[str] = None,
    ) -> Optional["_GoogleSheetsExporter"]:
        if kind in self._google_sheets_exporters:
            return self._google_sheets_exporters[kind]
        exporter = _GoogleSheetsExporter.from_env(
            spreadsheet_id=spreadsheet_id,
            spreadsheet_env=spreadsheet_env,
            default_spreadsheet_id=default_spreadsheet_id,
        )
        self._google_sheets_exporters[kind] = exporter
        return exporter

    async def _sync_google_sheet(
        self,
        table_rows: Sequence[Sequence[_XlsxCell]],
        *,
        kind: str,
        column_widths: Sequence[float],
        spreadsheet_id: Optional[str] = None,
        spreadsheet_env: str = "CONFETTI_GOOGLE_SHEETS_ID",
        default_spreadsheet_id: Optional[str] = None,
    ) -> _GoogleSheetSyncResult:
        exporter = self._ensure_google_sheets_exporter(
            kind=kind,
            spreadsheet_id=spreadsheet_id,
            spreadsheet_env=spreadsheet_env,
            default_spreadsheet_id=default_spreadsheet_id,
        )
        if exporter is None:
            return _GoogleSheetSyncResult(url=None, updated=False)

        loop = asyncio.get_running_loop()
        try:
            url = await loop.run_in_executor(
                None,
                exporter.sync,
                table_rows,
                tuple(column_widths),
            )
        except Exception as exc:  # pragma: no cover - network dependent
            LOGGER.warning("Не удалось обновить Google Sheets: %s", exc)
            fallback_url = self._last_google_sheet_urls.get(kind) or exporter.url
            if fallback_url:
                self._last_google_sheet_urls[kind] = fallback_url
            return _GoogleSheetSyncResult(url=fallback_url, updated=False, error=str(exc))

        if url:
            self._last_google_sheet_urls[kind] = url
            return _GoogleSheetSyncResult(url=url, updated=True)

        if self._last_google_sheet_urls.get(kind):
            return _GoogleSheetSyncResult(url=self._last_google_sheet_urls[kind], updated=True)

        fallback_url = exporter.url
        if fallback_url:
            self._last_google_sheet_urls[kind] = fallback_url
        return _GoogleSheetSyncResult(url=fallback_url, updated=True)

    def _build_payment_link_cell(
        self,
        *,
        bot_username: Optional[str],
        registration_id: str,
        attachments: list[MediaAttachment],
        payment_note: str,
    ) -> _XlsxCell:
        has_attachments = bool(attachments)

        if bot_username and registration_id and has_attachments:
            url = self._build_payment_deeplink(bot_username, registration_id, None)
            total = len(attachments)
            label_lines: list[str] = []

            if total == 1:
                label_lines.append(self._format_payment_link_label(attachments[0], 0, total))
            else:
                label_lines.append(f"Подтверждение оплаты — файлов: {total}")
                for index, attachment in enumerate(attachments, start=1):
                    label_lines.append(f"{index}. {self._format_payment_link_label(attachment, index - 1, total)}")

            if payment_note:
                label_lines.append(payment_note)

            label_text = "\n".join(label_lines)
            return _XlsxCell(text=label_text, formula=self._hyperlink_formula(url, label_text))

        text_lines: list[str] = []
        if has_attachments:
            text_lines.append("Фото оплаты доступно во вложениях бота")
        else:
            text_lines.append("Оплата ожидается")

        if payment_note:
            if text_lines:
                text_lines.append("")
            text_lines.append(payment_note)

        cell_text = "\n".join(text_lines).strip()

        return _XlsxCell(cell_text)

    @staticmethod
    def _hyperlink_formula(url: str, label: str) -> str:
        safe_url = url.replace('"', '""')
        if "\n" in label:
            segments = [segment.replace('"', '""') for segment in label.split("\n")]
            label_expr = '&CHAR(10)&'.join(f'"{segment}"' for segment in segments)
            return f'HYPERLINK("{safe_url}",{label_expr})'
        safe_label = label.replace('"', '""')
        return f'HYPERLINK("{safe_url}","{safe_label}")'

    @staticmethod
    def _build_payment_deeplink(
        bot_username: str,
        registration_id: str,
        attachment_index: Optional[int],
    ) -> str:
        base = f"https://t.me/{bot_username}?start=payment_{registration_id}"
        if attachment_index is None:
            return base
        return f"{base}_{attachment_index + 1}"

    @staticmethod
    def _format_payment_link_label(
        attachment: MediaAttachment,
        index: int,
        total: int,
    ) -> str:
        labels = {
            "photo": "Фото",
            "video": "Видео",
            "animation": "GIF",
            "document": "Файл",
            "video_note": "Видео-заметка",
            "audio": "Аудио",
            "voice": "Голос",
        }
        base = labels.get(attachment.kind, attachment.kind or "Вложение")
        if total > 1:
            base = f"{base} {index + 1}"
        if attachment.caption:
            base = f"{base} ({attachment.caption})"
        return base

    @staticmethod
    def _parse_payment_deeplink_payload(
        payload: str,
    ) -> tuple[str, Optional[int]]:
        candidate = payload.strip()
        if not candidate:
            return "", None

        if "_" in candidate:
            base, suffix = candidate.rsplit("_", 1)
            if suffix.isdigit():
                index = int(suffix) - 1
                if index >= 0:
                    return base, index

        return candidate, None

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

    async def _ensure_bot_username(
        self, context: ContextTypes.DEFAULT_TYPE
    ) -> Optional[str]:
        if self._bot_username:
            return self._bot_username

        try:
            me = await context.bot.get_me()
        except Exception as exc:  # pragma: no cover - network dependent
            LOGGER.debug("Failed to resolve bot username: %s", exc)
            return None

        username = getattr(me, "username", None)
        if not username:
            return None

        self._bot_username = username
        return username

    async def _send_registration_payment_media(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        registration_id: str,
        *,
        attachment_index: Optional[int] = None,
    ) -> bool:
        record = self._find_registration_by_id(context, registration_id)
        if record is None:
            await self._reply(
                update,
                "Не удалось найти заявку с таким идентификатором.",
                reply_markup=self._admin_menu_markup(),
            )
            return False

        attachments = self._dicts_to_attachments(record.get("payment_media"))
        if not attachments:
            await self._reply(
                update,
                "Для этой заявки не прикреплены файлы оплаты.",
                reply_markup=self._admin_menu_markup(),
            )
            return False

        selected_attachments = attachments
        if attachment_index is not None:
            if 0 <= attachment_index < len(attachments):
                selected_attachments = [attachments[attachment_index]]
            else:
                await self._reply(
                    update,
                    "Не удалось найти вложение с указанным номером.",
                    reply_markup=self._admin_menu_markup(),
                )
                return False

        summary_lines = [
            "💳 Вложения по заявке",
            f"👦 Участник: {record.get('child_name', '—')} ({record.get('class', '—')})",
            f"📚 Программа: {record.get('program', '—')}",
            f"🗓 Создана: {record.get('created_at', '—')}",
            f"📎 Файлов: {len(attachments)}",
        ]

        if attachment_index is not None and len(attachments) > 1:
            summary_lines.append(
                f"🔍 Показан файл {attachment_index + 1} из {len(attachments)}"
            )

        chat = update.effective_chat
        try:
            chat_id = _coerce_chat_id_from_object(chat) if chat else None
        except ValueError:
            chat_id = None

        if chat_id is None:
            return False

        try:
            await self._send_payload_to_chat(
                context,
                chat_id,
                text="\n".join(summary_lines),
                media=selected_attachments,
                reply_markup=self._admin_menu_markup(),
            )
        except Exception as exc:  # pragma: no cover - network dependent
            LOGGER.warning("Не удалось отправить вложения заявки %s: %s", registration_id, exc)
            return False

        return True

    async def _send_payment_report_media(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        payment_id: str,
        *,
        attachment_index: Optional[int] = None,
    ) -> bool:
        record = self._find_payment_report_by_id(context, payment_id)
        if record is None:
            await self._reply(
                update,
                "Не удалось найти подтверждение оплаты с таким идентификатором.",
                reply_markup=self._admin_menu_markup(),
            )
            return False

        attachments = self._dicts_to_attachments(record.get("attachments"))
        if not attachments:
            await self._reply(
                update,
                "Для этой записи нет сохранённых вложений.",
                reply_markup=self._admin_menu_markup(),
            )
            return False

        selected_attachments = attachments
        if attachment_index is not None:
            if 0 <= attachment_index < len(attachments):
                selected_attachments = [attachments[attachment_index]]
            else:
                await self._reply(
                    update,
                    "Не удалось найти вложение с указанным номером.",
                    reply_markup=self._admin_menu_markup(),
                )
                return False

        summary_lines = [
            "💳 Подтверждение оплаты",
            f"📚 Направление: {record.get('program', '—')}",
            f"👤 Плательщик: {record.get('full_name', '—')}",
            f"🕒 Отправлено: {record.get('created_at', '—')}",
            f"📎 Файлов: {len(attachments)}",
        ]

        if attachment_index is not None and len(attachments) > 1:
            summary_lines.append(
                f"🔍 Показан файл {attachment_index + 1} из {len(attachments)}"
            )

        chat = update.effective_chat
        try:
            chat_id = _coerce_chat_id_from_object(chat) if chat else None
        except ValueError:
            chat_id = None

        if chat_id is None:
            return False

        try:
            await self._send_payload_to_chat(
                context,
                chat_id,
                text="\n".join(summary_lines),
                media=selected_attachments,
                reply_markup=self._admin_menu_markup(),
            )
        except Exception as exc:  # pragma: no cover - network dependent
            LOGGER.warning("Не удалось отправить вложения подтверждения %s: %s", payment_id, exc)
            return False

        return True

    def _extract_media_directives(self, text: str) -> tuple[str, list[MediaAttachment]]:
        if not text.strip():
            return text.strip(), []

        attachments: list[MediaAttachment] = []
        cleaned_lines: list[str] = []
        for raw_line in text.splitlines():
            directive = self.MEDIA_DIRECTIVE_PATTERN.match(raw_line.strip())
            if directive:
                kind = directive.group("kind").lower()
                url = directive.group("url")
                if not url:
                    continue
                caption = directive.group("caption")
                attachments.append(
                    MediaAttachment(
                        kind=kind,
                        file_id=url,
                        caption=caption.strip() if caption else None,
                    )
                )
                continue
            cleaned_lines.append(raw_line)

        cleaned_text = "\n".join(cleaned_lines).strip()
        return cleaned_text, attachments

    async def _admin_apply_content_update(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        field: str,
        *,
        text: str,
        attachments: list[MediaAttachment],
    ) -> None:
        cleaned_text, url_attachments = self._extract_media_directives(text)
        combined_media = [
            MediaAttachment(kind=item.kind, file_id=item.file_id, caption=item.caption)
            for item in attachments
        ]
        combined_media.extend(url_attachments)
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
            text=cleaned_text.strip(),
            media=combined_media,
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
            media=combined_media or None,
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
            "📞 Контакты": self._send_contacts,
            "📚 Слово дня": self._send_vocabulary,
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
        intro = content.about.text.strip() if content.about.text else "О студии"
        message = intro + "\n\nВыберите направление, чтобы узнать подробности."
        media: list[MediaAttachment] = []
        if content.about.media:
            first = content.about.media[0]
            media.append(
                MediaAttachment(kind=first.kind, file_id=first.file_id, caption=message)
            )

        if media:
            await self._reply(
                update,
                text=None,
                reply_markup=self._about_inline_keyboard(),
                media=media,
                prefer_edit=update.callback_query is not None,
            )
            return

        await self._reply(
            update,
            message,
            reply_markup=self._about_inline_keyboard(),
            prefer_edit=update.callback_query is not None,
        )

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
            school = str(record.get("school", ""))
            grade = str(record.get("class", ""))
            contact = str(record.get("contact_name", ""))
            phone = str(record.get("phone", ""))
            comment = str(record.get("comment", ""))
            created_at = str(record.get("created_at", ""))

            entry_lines = [f"{index}. {program}"]
            if child:
                entry_lines.append(f"👦 {child}")
            if school:
                entry_lines.append(f"🏫 {school}")
            if grade:
                entry_lines.append(f"🎓 {grade}")
            if contact:
                entry_lines.append(f"👤 {contact}")
            if phone:
                entry_lines.append(f"📱 {phone}")
            if comment:
                entry_lines.append(f"📝 {comment}")
            if created_at:
                entry_lines.append(f"📅 Заявка от: {created_at}")
            lines.append("\n".join(entry_lines))

        text = "📋 Ваши заявки:\n\n" + "\n\n".join(lines)
        await self._reply(update, text, reply_markup=reply_markup)

    async def _send_teachers(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        content = self._get_content(context)
        intro = content.teachers.text.strip() if content.teachers.text else "Наши преподаватели — увлечённые и опытные педагоги."
        media: list[MediaAttachment] = []
        if content.teachers.media:
            first = content.teachers.media[0]
            media.append(
                MediaAttachment(kind=first.kind, file_id=first.file_id, caption=intro)
            )

        if media:
            await self._reply(
                update,
                text=None,
                reply_markup=self._teacher_inline_keyboard(),
                media=media,
                prefer_edit=update.callback_query is not None,
            )
            return

        await self._reply(
            update,
            intro,
            reply_markup=self._teacher_inline_keyboard(),
            prefer_edit=update.callback_query is not None,
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
        if key == "back":
            await query.answer()
            await self._reply(
                update,
                "Список преподавателей обновляется.",
                reply_markup=self._teacher_inline_keyboard(),
                prefer_edit=True,
            )
            return

        teachers = self._teacher_directory()
        teacher = next((item for item in teachers if item.get("id") == key), None)
        if teacher is None:
            await query.answer("Педагог не найден.", show_alert=True)
            return

        await query.answer()
        name = teacher.get("name", "Преподаватель")
        bio = teacher.get("bio") or teacher.get("description") or ""
        caption = f"{name}\n\n{bio}".strip()
        keyboard = self._teacher_inline_keyboard()
        photo_reference = self._resolve_media_reference(
            teacher,
            file_key="photo_file_id",
            url_key="photo_url",
        )

        if photo_reference:
            await self._reply(
                update,
                text=None,
                reply_markup=keyboard,
                media=[
                    MediaAttachment(
                        kind="photo",
                        file_id=photo_reference,
                        caption=caption,
                    )
                ],
                prefer_edit=update.callback_query is not None,
            )
            return

        await self._reply(
            update,
            caption + "\n\n",
            reply_markup=keyboard,
            prefer_edit=True,
        )

    async def _about_show_direction(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        query = update.callback_query
        if query is None:
            return

        data = (query.data or "").split(":", 1)
        if len(data) != 2:
            await query.answer("Не удалось показать направление.", show_alert=True)
            return

        key = data[1]
        if key == "back":
            await query.answer()
            await self._send_about(update, context)
            return
        try:
            index = int(key)
        except ValueError:
            await query.answer("Неизвестное направление.", show_alert=True)
            return

        programs = self._program_catalog()
        if not 0 <= index < len(programs):
            await query.answer("Направление не найдено.", show_alert=True)
            return

        program = programs[index]
        await query.answer()

        variants = self._program_variants(program)
        if variants:
            await self._about_prompt_french_variants(
                update,
                context,
                program,
                index,
                variants=variants,
            )
            return

        overview = self._format_program_details(program)
        photo_reference = self._resolve_media_reference(
            program,
            file_key="photo_file_id",
            url_key="photo_url",
        )
        if photo_reference:
            await self._reply(
                update,
                text=None,
                reply_markup=self._about_inline_keyboard(),
                media=[
                    MediaAttachment(
                        kind="photo",
                        file_id=photo_reference,
                        caption=overview,
                    )
                ],
                prefer_edit=update.callback_query is not None,
            )
            return

        await self._reply(
            update,
            overview + "\n\n",
            reply_markup=self._about_inline_keyboard(),
            prefer_edit=True,
        )

    async def _about_prompt_french_variants(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        program: Dict[str, Any],
        program_index: int,
        *,
        variants: Sequence[dict[str, str]],
    ) -> None:
        overview = self._format_program_details(program)
        instruction = "Выберите подходящую группу:"
        caption = overview.strip()
        if caption:
            caption = f"{caption}\n\n{instruction}"
        else:
            caption = instruction
        photo_reference = self._resolve_media_reference(
            program,
            file_key="photo_file_id",
            url_key="photo_url",
        )
        keyboard = self._about_french_variant_keyboard(program_index, variants)
        if photo_reference:
            await self._reply(
                update,
                text=None,
                reply_markup=keyboard,
                media=[
                    MediaAttachment(
                        kind="photo",
                        file_id=photo_reference,
                        caption=caption,
                    )
                ],
                prefer_edit=True,
            )
            return

        await self._reply(
            update,
            caption + "\n\n",
            reply_markup=keyboard,
            prefer_edit=True,
        )

    async def _about_show_french_variant(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        query = update.callback_query
        if query is None:
            return

        payload = (query.data or "").split(":", 2)
        if len(payload) < 2:
            await query.answer("Не удалось определить группу.", show_alert=True)
            return

        action = payload[1]
        if action == "back":
            await query.answer()
            await self._send_about(update, context)
            return

        if len(payload) != 3:
            await query.answer("Не удалось определить группу.", show_alert=True)
            return

        try:
            program_index = int(action)
            variant_index = int(payload[2])
        except ValueError:
            await query.answer("Не удалось определить группу.", show_alert=True)
            return

        programs = self._program_catalog()
        if not 0 <= program_index < len(programs):
            await query.answer("Направление не найдено.", show_alert=True)
            return

        program = programs[program_index]
        variants = self._program_variants(program)
        if not 0 <= variant_index < len(variants):
            await query.answer("Группа не найдена.", show_alert=True)
            return

        option = variants[variant_index]

        full_title = self._compose_french_variant_label(str(program.get("title", "")), option)
        body = str(program.get("body", "")).strip()
        caption = full_title.strip()
        if body:
            caption = f"{caption}\n\n{body}" if caption else body

        photo_reference = self._resolve_media_reference(
            program,
            file_key="photo_file_id",
            url_key="photo_url",
        )
        keyboard = self._about_french_variant_keyboard(program_index, variants)

        await self._reply(
            update,
            text=None if photo_reference else caption + "\n\n",
            reply_markup=keyboard,
            media=(
                [
                    MediaAttachment(
                        kind="photo",
                        file_id=photo_reference,
                        caption=caption,
                    )
                ]
                if photo_reference
                else None
            ),
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
    keyboard: list[list[str]] = field(default_factory=list)
    role: str = field(init=False, default="user")

    @property
    def is_admin(self) -> bool:
        return False

    def __getitem__(self, key: str) -> Any:
        if key == "chat_id":
            return self.chat_id
        if key == "keyboard":
            return self.keyboard
        if key == "is_admin":
            return self.is_admin
        if key == "role":
            return self.role
        raise KeyError(key)


@dataclass(frozen=True)
class AdminProfile(UserProfile):
    """Profile granted elevated permissions."""

    role: str = field(init=False, default="admin")

    @property
    def is_admin(self) -> bool:
        return True


@dataclass
class _XlsxImage:
    data: bytes
    content_type: str
    description: str = ""

    @property
    def extension(self) -> str:
        mapping = {
            "image/jpeg": "jpeg",
            "image/jpg": "jpeg",
            "image/png": "png",
        }
        return mapping.get(self.content_type.lower(), "bin")


@dataclass
class _XlsxCell:
    text: str = ""
    formula: Optional[str] = None
    image: Optional[_XlsxImage] = None

    @classmethod
    def hyperlink(cls, text: str, url: str) -> "_XlsxCell":
        safe_url = url.replace('"', '""')
        safe_text = text.replace('"', '""')
        formula = f'HYPERLINK("{safe_url}","{safe_text}")'
        return cls(text=text, formula=formula)


class _SimpleXlsxBuilder:
    """Minimal XLSX writer for structured admin exports."""

    def __init__(
        self,
        sheet_name: str = "Sheet1",
        *,
        column_widths: Optional[Iterable[float]] = None,
    ) -> None:
        self.sheet_name = self._sanitise_sheet_name(sheet_name)
        self.rows: list[list[_XlsxCell]] = []
        self.column_widths: list[float] = [float(width) for width in column_widths] if column_widths else []
        self._image_anchors: list[tuple[int, int, _XlsxImage]] = []

    def add_row(self, values: Iterable[Any]) -> None:
        row: list[_XlsxCell] = []
        for value in values:
            row.append(self._normalise_cell(value))
        self.rows.append(row)

    def to_file(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        sheet_xml = self._sheet()
        with ZipFile(path, "w", ZIP_DEFLATED) as archive:
            archive.writestr("[Content_Types].xml", self._content_types())
            archive.writestr("_rels/.rels", self._rels_root())
            archive.writestr("xl/workbook.xml", self._workbook())
            archive.writestr("xl/_rels/workbook.xml.rels", self._workbook_rels())
            archive.writestr("xl/styles.xml", self._styles())
            archive.writestr("xl/worksheets/sheet1.xml", sheet_xml)
            if self._image_anchors:
                archive.writestr("xl/worksheets/_rels/sheet1.xml.rels", self._sheet_rels())
                archive.writestr("xl/drawings/drawing1.xml", self._drawing())
                archive.writestr("xl/drawings/_rels/drawing1.xml.rels", self._drawing_rels())
                for index, (_, _, image) in enumerate(self._image_anchors, start=1):
                    archive.writestr(
                        f"xl/media/image{index}.{image.extension}",
                        image.data,
                    )

    def _sheet(self) -> str:
        rows_xml: list[str] = []
        image_anchors: list[tuple[int, int, _XlsxImage]] = []
        for row_index, row in enumerate(self.rows, start=1):
            cells: list[str] = []
            for column_index, value in enumerate(row):
                cell_reference = f"{self._column_letter(column_index)}{row_index}"
                style_index = 1 if row_index == 1 else 2
                style_attr = f' s="{style_index}"'
                text = escape(value.text, {"\n": "&#10;"})
                if value.formula:
                    formula = escape(value.formula)
                    cells.append(
                        f'<c r="{cell_reference}" t="str"{style_attr}><f>{formula}</f><v>{text}</v></c>'
                    )
                else:
                    cells.append(
                        f'<c r="{cell_reference}" t="inlineStr"{style_attr}><is><t>{text}</t></is></c>'
                    )
                if value.image is not None and row_index > 1:
                    image_anchors.append((row_index - 1, column_index, value.image))
            rows_xml.append(f'<row r="{row_index}">{"".join(cells)}</row>')

        sheet_data = "".join(rows_xml)
        cols_xml = ""
        if self.column_widths:
            col_parts = []
            for index, width in enumerate(self.column_widths, start=1):
                col_parts.append(
                    f'<col min="{index}" max="{index}" width="{width}" customWidth="1"/>'
                )
            cols_xml = f"<cols>{''.join(col_parts)}</cols>"
        drawing_ref = ""
        if image_anchors:
            drawing_ref = '<drawing r:id="rId1"/>'
        self._image_anchors = image_anchors
        return (
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
            "<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" "
            "xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">"
            f"{cols_xml}<sheetData>{sheet_data}</sheetData>{drawing_ref}"
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

    def _content_types(self) -> str:
        defaults: dict[str, str] = {
            "rels": "application/vnd.openxmlformats-package.relationships+xml",
            "xml": "application/xml",
        }
        overrides: list[tuple[str, str]] = [
            ("/xl/workbook.xml", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"),
            ("/xl/worksheets/sheet1.xml", "application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"),
            ("/xl/styles.xml", "application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"),
        ]
        if self._image_anchors:
            overrides.append(
                ("/xl/drawings/drawing1.xml", "application/vnd.openxmlformats-officedocument.drawing+xml")
            )
            for _, _, image in self._image_anchors:
                defaults.setdefault(image.extension, image.content_type)

        parts = [
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>",
            "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\">",
        ]
        for extension, content_type in defaults.items():
            parts.append(
                f'<Default Extension="{escape(extension)}" ContentType="{escape(content_type)}"/>'
            )
        for part_name, content_type in overrides:
            parts.append(
                f'<Override PartName="{escape(part_name)}" ContentType="{escape(content_type)}"/>'
            )
        parts.append("</Types>")
        return "".join(parts)

    def _sheet_rels(self) -> str:
        return (
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
            "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">"
            "<Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/drawing\" Target=\"../drawings/drawing1.xml\"/>"
            "</Relationships>"
        )

    def _drawing(self) -> str:
        anchors: list[str] = []
        for index, (row, column, image) in enumerate(self._image_anchors, start=1):
            description = escape(image.description or f"Фото оплаты {index}")
            anchors.append(
                "<xdr:twoCellAnchor>"
                f"<xdr:from><xdr:col>{column}</xdr:col><xdr:colOff>0</xdr:colOff><xdr:row>{row}</xdr:row><xdr:rowOff>0</xdr:rowOff></xdr:from>"
                f"<xdr:to><xdr:col>{column + 1}</xdr:col><xdr:colOff>0</xdr:colOff><xdr:row>{row + 1}</xdr:row><xdr:rowOff>0</xdr:rowOff></xdr:to>"
                "<xdr:pic>"
                "<xdr:nvPicPr>"
                f"<xdr:cNvPr id=\"{index}\" name=\"Image {index}\" descr=\"{description}\"/>"
                "<xdr:cNvPicPr><a:picLocks noChangeAspect=\"1\"/></xdr:cNvPicPr>"
                "</xdr:nvPicPr>"
                "<xdr:blipFill>"
                f"<a:blip r:embed=\"rId{index}\"/>"
                "<a:stretch><a:fillRect/></a:stretch>"
                "</xdr:blipFill>"
                "<xdr:spPr>"
                "<a:prstGeom prst=\"rect\"><a:avLst/></a:prstGeom>"
                "</xdr:spPr>"
                "</xdr:pic>"
                "<xdr:clientData/>"
                "</xdr:twoCellAnchor>"
            )

        return (
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
            "<xdr:wsDr xmlns:xdr=\"http://schemas.openxmlformats.org/drawingml/2006/spreadsheetDrawing\" "
            "xmlns:a=\"http://schemas.openxmlformats.org/drawingml/2006/main\" "
            "xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">"
            f"{''.join(anchors)}"
            "</xdr:wsDr>"
        )

    def _drawing_rels(self) -> str:
        relationships: list[str] = []
        for index, (_, _, image) in enumerate(self._image_anchors, start=1):
            relationships.append(
                f'<Relationship Id="rId{index}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/image" Target="../media/image{index}.{image.extension}"/>'
            )
        return (
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
            "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">"
            f"{''.join(relationships)}"
            "</Relationships>"
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
            "<cellXfs count=\"3\">"
            "<xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\" xfId=\"0\"/>"
            "<xf numFmtId=\"0\" fontId=\"1\" fillId=\"0\" borderId=\"0\" xfId=\"0\" applyFont=\"1\"/>"
            "<xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\" xfId=\"0\" applyAlignment=\"1\"><alignment wrapText=\"1\"/></xf>"
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

    @staticmethod
    def _normalise_cell(value: Any) -> _XlsxCell:
        if isinstance(value, _XlsxCell):
            return value
        if isinstance(value, _XlsxImage):
            return _XlsxCell("", image=value)
        if value is None:
            return _XlsxCell("")
        return _XlsxCell(str(value))


@dataclass
class _GoogleSheetSyncResult:
    url: Optional[str]
    updated: bool
    error: Optional[str] = None


class _GoogleSheetsExporter:
    """Synchronise admin exports with a Google Sheets document."""

    DEFAULT_SPREADSHEET_ID = "1DreSJ4xpKFFtcrJN1IJBJ51MOa7_RcqGXAmKYhWSlfA"
    SERVICE_ACCOUNT_JSON_ENV = "CONFETTI_GOOGLE_SERVICE_ACCOUNT_JSON"
    SERVICE_ACCOUNT_FILE_ENV = "CONFETTI_GOOGLE_SERVICE_ACCOUNT_FILE"
    DEFAULT_SERVICE_ACCOUNT_CANDIDATES: tuple[str, ...] = (
        "confetti_service_account.json",
        "service_account.json",
    )
    SCOPES = (
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
    )

    def __init__(
        self,
        spreadsheet_id: str,
        credentials: GoogleServiceAccountCredentials,
        *,
        service_account_email: Optional[str] = None,
    ) -> None:
        self.spreadsheet_id = spreadsheet_id
        self._credentials = credentials
        self._service_account_email = service_account_email
        self._service: Optional[Any] = None
        self._sheet_id: Optional[int] = None
        self._sheet_title: Optional[str] = None
        self._spreadsheet_url: Optional[str] = None

    @property
    def service_account_email(self) -> Optional[str]:
        return self._service_account_email

    @classmethod
    def from_env(
        cls,
        *,
        spreadsheet_id: Optional[str] = None,
        spreadsheet_env: str = "CONFETTI_GOOGLE_SHEETS_ID",
        default_spreadsheet_id: Optional[str] = None,
    ) -> Optional["_GoogleSheetsExporter"]:
        if (
            GoogleServiceAccountCredentials is None
            or google_build is None
            or GoogleAuthRequest is None
        ):
            LOGGER.info(
                "Библиотеки Google Sheets не установлены или недоступны, экспорт будет только в XLSX."
            )
            return None

        service_account_info: Optional[dict[str, Any]] = None
        service_account_email: Optional[str] = None

        json_blob = os.environ.get(cls.SERVICE_ACCOUNT_JSON_ENV)
        if json_blob:
            json_blob = json_blob.strip()
        if json_blob:
            json_blob = json_blob.lstrip("'\"").rstrip("'\"")
            try:
                service_account_info = json.loads(json_blob)
            except json.JSONDecodeError:
                LOGGER.warning(
                    "Не удалось разобрать JSON сервисного аккаунта из %s.",
                    cls.SERVICE_ACCOUNT_JSON_ENV,
                )
                service_account_info = None
        if service_account_info is None:
            credentials_path = os.environ.get(cls.SERVICE_ACCOUNT_FILE_ENV)
            if credentials_path:
                credentials_path = credentials_path.strip()
            if credentials_path:
                try:
                    payload = Path(credentials_path).read_text(encoding="utf-8")
                    service_account_info = json.loads(payload)
                except (OSError, json.JSONDecodeError) as exc:
                    LOGGER.warning(
                        "Не удалось прочитать сервисный аккаунт из файла %s: %s",
                        credentials_path,
                        exc,
                    )
                    service_account_info = None

        if service_account_info is None:
            base_dir = Path(__file__).resolve().parent
            search_roots = (
                Path.cwd(),
                base_dir,
                base_dir / "data",
            )
            for root in search_roots:
                for filename in cls.DEFAULT_SERVICE_ACCOUNT_CANDIDATES:
                    candidate = root / filename
                    if not candidate.exists() or not candidate.is_file():
                        continue
                    try:
                        payload = candidate.read_text(encoding="utf-8")
                        service_account_info = json.loads(payload)
                        LOGGER.info(
                            "Загружен сервисный аккаунт Google из файла %s.",
                            candidate,
                        )
                        break
                    except (OSError, json.JSONDecodeError) as exc:
                        LOGGER.warning(
                            "Не удалось прочитать сервисный аккаунт из файла %s: %s",
                            candidate,
                            exc,
                        )
                        service_account_info = None
                if service_account_info is not None:
                    break

        if not service_account_info:
            LOGGER.info(
                "Сервисный аккаунт Google не настроен, пропускаем синхронизацию Google Sheets."
            )
            return None

        service_account_email = service_account_info.get("client_email")

        try:
            credentials = GoogleServiceAccountCredentials.from_service_account_info(
                service_account_info,
                scopes=cls.SCOPES,
            )
        except (ValueError, TypeError) as exc:
            LOGGER.warning("Некорректные данные сервисного аккаунта: %s", exc)
            return None

        if spreadsheet_id:
            spreadsheet_id = spreadsheet_id.strip()
        if not spreadsheet_id:
            env_candidate = os.environ.get(spreadsheet_env)
            if env_candidate:
                env_candidate = env_candidate.strip()
            spreadsheet_id = env_candidate
        if not spreadsheet_id:
            spreadsheet_id = default_spreadsheet_id or cls.DEFAULT_SPREADSHEET_ID

        return cls(
            spreadsheet_id,
            credentials,
            service_account_email=service_account_email,
        )

    def sync(
        self,
        rows: Sequence[Sequence[_XlsxCell]],
        column_widths: Sequence[float],
    ) -> Optional[str]:
        if not rows:
            return self._spreadsheet_url or self._build_spreadsheet_url()

        service = self._build_service()
        self._ensure_sheet_metadata(service)

        sheet_title = self._sheet_title or "Заявки"
        escaped_title = sheet_title.replace("'", "''")
        clear_range = f"'{escaped_title}'"
        update_range = f"'{escaped_title}'!A1"

        normalised_rows = [self._normalise_row(row) for row in rows]
        values = [
            [cell.formula if cell.formula else cell.text for cell in row]
            for row in normalised_rows
        ]

        try:
            service.spreadsheets().values().clear(
                spreadsheetId=self.spreadsheet_id,
                range=clear_range,
            ).execute()
            service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id,
                range=update_range,
                valueInputOption="USER_ENTERED",
                body={"values": values},
            ).execute()
            column_count = max(len(row) for row in normalised_rows)
            self._apply_formatting(service, column_count, column_widths)
        except GoogleHttpError as exc:  # pragma: no cover - network dependent
            LOGGER.warning("Ошибка обновления Google Sheets: %s", exc)
            return None

        return self._spreadsheet_url or self._build_spreadsheet_url()

    def _build_service(self) -> Any:
        if self._service is not None:
            return self._service

        try:
            self._credentials.refresh(GoogleAuthRequest())
        except Exception as exc:  # pragma: no cover - network dependent
            LOGGER.warning("Не удалось обновить OAuth-токен Google: %s", exc)
            raise

        self._service = google_build(
            "sheets",
            "v4",
            credentials=self._credentials,
            cache_discovery=False,
        )
        return self._service

    def _ensure_sheet_metadata(self, service: Any) -> None:
        if self._sheet_title and self._sheet_id is not None and self._spreadsheet_url:
            return

        try:
            metadata = service.spreadsheets().get(
                spreadsheetId=self.spreadsheet_id,
            ).execute()
        except GoogleHttpError as exc:  # pragma: no cover - network dependent
            LOGGER.warning("Не удалось получить свойства Google Sheets: %s", exc)
            raise

        self._spreadsheet_url = metadata.get("spreadsheetUrl")
        sheets = metadata.get("sheets") or []
        preferred = "Заявки"
        fallback: Optional[dict[str, Any]] = None
        for sheet in sheets:
            props = sheet.get("properties") or {}
            if fallback is None:
                fallback = props
            if props.get("title") == preferred:
                self._sheet_id = props.get("sheetId")
                self._sheet_title = props.get("title")
                break
        else:
            if fallback:
                self._sheet_id = fallback.get("sheetId")
                self._sheet_title = fallback.get("title")

        if self._sheet_title is None:
            self._sheet_title = preferred

    def _apply_formatting(
        self,
        service: Any,
        column_count: int,
        column_widths: Sequence[float],
    ) -> None:
        if self._sheet_id is None or column_count <= 0:
            return

        requests: list[dict[str, Any]] = [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": self._sheet_id,
                        "gridProperties": {"frozenRowCount": 1},
                    },
                    "fields": "gridProperties.frozenRowCount",
                }
            },
            {
                "repeatCell": {
                    "range": {
                        "sheetId": self._sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {"bold": True},
                            "horizontalAlignment": "CENTER",
                            "wrapStrategy": "WRAP",
                        }
                    },
                    "fields": "userEnteredFormat(textFormat,horizontalAlignment,wrapStrategy)",
                }
            },
            {
                "repeatCell": {
                    "range": {
                        "sheetId": self._sheet_id,
                        "startRowIndex": 1,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "wrapStrategy": "WRAP",
                        }
                    },
                    "fields": "userEnteredFormat.wrapStrategy",
                }
            },
        ]

        width_values = list(column_widths)
        for index in range(column_count):
            if width_values:
                base_width = width_values[index] if index < len(width_values) else width_values[-1]
            else:
                base_width = 20.0
            requests.append(
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": self._sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": index,
                            "endIndex": index + 1,
                        },
                        "properties": {"pixelSize": self._column_width_to_pixels(base_width)},
                        "fields": "pixelSize",
                    }
                }
            )

        try:
            service.spreadsheets().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={"requests": requests},
            ).execute()
        except GoogleHttpError:  # pragma: no cover - network dependent
            LOGGER.debug("Не удалось применить форматирование для Google Sheets.")

    @staticmethod
    def _column_width_to_pixels(width: float) -> int:
        width = max(width, 1.0)
        return max(int(round(width * 7 + 5)), 40)

    @staticmethod
    def _build_spreadsheet_url_from_id(spreadsheet_id: str) -> str:
        return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"

    def _build_spreadsheet_url(self) -> str:
        return self._spreadsheet_url or self._build_spreadsheet_url_from_id(self.spreadsheet_id)

    @property
    def url(self) -> str:
        return self._build_spreadsheet_url()

    @staticmethod
    def _normalise_row(row: Sequence[_XlsxCell]) -> list[_XlsxCell]:
        normalised: list[_XlsxCell] = []
        for cell in row:
            if isinstance(cell, _XlsxCell):
                normalised.append(cell)
            elif isinstance(cell, _XlsxImage):
                normalised.append(_XlsxCell("", image=cell))
            elif cell is None:
                normalised.append(_XlsxCell(""))
            else:
                normalised.append(_XlsxCell(str(cell)))
        return normalised


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

    if sys.platform.startswith("win"):
        # python-telegram-bot relies on selector event loops which are not the default
        # on Windows since Python 3.8+.  Switching to the selector policy prevents the
        # application from hanging inside ``run_polling`` during shutdown.
        try:  # pragma: no cover - specific to Windows runtime
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        except AttributeError:
            pass

    logging.basicConfig(level=logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("telegram.ext").setLevel(logging.WARNING)
    logging.getLogger("telegram.vendor.ptb.urllib3").setLevel(logging.WARNING)

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

    pending_key: Optional[str] = None
    pending_quote: Optional[str] = None
    pending_value_lines: list[str] = []

    for line in content.splitlines():
        if pending_key is not None and pending_quote is not None:
            pending_value_lines.append(line)
            if _line_closes_multiline_value(line, pending_quote):
                closing_line = pending_value_lines[-1].rstrip()
                closing_line = closing_line[:-1]
                pending_value_lines[-1] = closing_line
                value = "\n".join(pending_value_lines)
                if pending_key not in os.environ:
                    os.environ[pending_key] = value
                pending_key = None
                pending_quote = None
                pending_value_lines = []
            continue

        parsed = _parse_env_assignment(line)
        if not parsed:
            continue
        key, value = parsed
        if key in os.environ:
            continue
        if _value_is_multiline_stub(value):
            pending_key = key
            pending_quote = value[0]
            pending_value_lines = [value[1:]]
            continue
        os.environ[key] = value

    if pending_key is not None and pending_quote is not None:
        LOGGER.warning(
            "Environment variable %s appears to have an unterminated multi-line value in %s",
            pending_key,
            path,
        )


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


def _value_is_multiline_stub(value: str) -> bool:
    if not value:
        return False
    if value[0] not in {'"', "'"}:
        return False
    if len(value) == 1:
        return True
    return value[-1] != value[0]


def _line_closes_multiline_value(line: str, quote: str) -> bool:
    stripped = line.rstrip()
    if not stripped.endswith(quote):
        return False
    escape_count = 0
    for char in reversed(stripped[:-1]):
        if char == "\\":
            escape_count += 1
        else:
            break
    return escape_count % 2 == 0


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

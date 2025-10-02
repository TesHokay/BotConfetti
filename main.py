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
import io
import json
import logging
import mimetypes
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
from typing import TYPE_CHECKING, Any, Dict, Optional, Union
from xml.sax.saxutils import escape

try:  # pragma: no cover - optional dependency for JPEG conversion
    from PIL import Image
except ModuleNotFoundError:  # pragma: no cover - pillow may be absent in tests
    Image = None  # type: ignore[assignment]

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
    ADMIN_MANAGE_ADMINS_BUTTON = "👤 Редактировать администраторов"
    ADMIN_EDIT_SCHEDULE_BUTTON = "🗓 Редактировать расписание"
    ADMIN_EDIT_ABOUT_BUTTON = "ℹ️ Редактировать информацию"
    ADMIN_EDIT_TEACHERS_BUTTON = "👩‍🏫 Редактировать преподавателей"
    ADMIN_EDIT_ALBUM_BUTTON = "📸 Редактировать фотоальбом"
    ADMIN_EDIT_CONTACTS_BUTTON = "📞 Редактировать контакты"
    ADMIN_EDIT_VOCABULARY_BUTTON = "📚 Редактировать словарь"
    ADMIN_CANCEL_KEYWORDS = ("отмена", "annuler", "cancel")
    ADMIN_CANCEL_PROMPT = f"\n\nЧтобы отменить, нажмите «{BACK_BUTTON}» или напишите «Отмена»."

    EXPORT_COLUMN_WIDTHS = (
        20,
        36,
        30,
        22,
        18,
        26,
        36,
        24,
    )

    MAIN_MENU_LAYOUT = (
        (REGISTRATION_BUTTON, "📅 Расписание"),
        ("ℹ️ О студии", "👩‍🏫 Преподаватели"),
        (REGISTRATION_LIST_BUTTON, "📞 Контакты"),
        ("📚 Полезные слова", CANCELLATION_BUTTON),
    )

    TIME_OF_DAY_OPTIONS = (
        "☀️ Утро (10:00 - 12:00)",
        "🌤 День (14:00 – 16:00)",
        "🌙 Вечер (18:00 – 20:00)",
    )

    PROGRAMS = (
        {
            "label": "📚 Французский каждый день",
            "audience": "С 3 по 11 класс",
            "teacher": "Преподаватель: Настыч Ксения Викторовна",
            "schedule": "Дни занятий: вторник или четверг вечером",
            "description": (
                "Интенсивная языковая практика в будни. Ученики погружаются "
                "в язык через общение, игры и проекты, закрепляя школьную программу "
                "и расширяя словарный запас."
            ),
            "photo_url": "https://storage.yandexcloud.net/bigbob/kazd.png",
        },
        {
            "label": "🎭 Театр на французском (вечер)",
            "teacher": "Преподаватель: Настыч Ксения Викторовна",
            "schedule": "Дни занятий: вторник или четверг вечером",
            "description": (
                "Театральная студия для тех, кто любит сцену и французский язык. "
                "Готовим постановки, работаем над произношением и учимся импровизировать "
                "на французском."
            ),
            "photo_url": "https://storage.yandexcloud.net/bigbob/photo_2025-09-29_16-01-53(1).jpg",
        },
        {
            "label": "📚 Воскресный французский",
            "audience": "1–4 класс",
            "teacher": "Преподаватель: Банникова Анастасия Дмитриевна",
            "schedule": "Дни занятий: воскресенье",
            "description": (
                "Уютные воскресные встречи для младших школьников. Развиваем речь "
                "через творчество, песни и игры, знакомимся с французскими традициями."
            ),
            "photo_url": "https://storage.yandexcloud.net/bigbob/voskr.png",
        },
        {
            "label": "🎭 Театр на французском (воскресенье)",
            "teacher": "Преподаватель: Банникова Анастасия Дмитриевна",
            "schedule": "Дни занятий: воскресенье",
            "description": (
                "Театральная студия выходного дня: работа с текстами, пластикой и "
                "эмоциями на французском языке, совместные выступления и фестивали."
            ),
            "photo_url": "https://storage.yandexcloud.net/bigbob/teatr(1).jpg",
        },
        {
            "label": "🇫🇷 Французский по-взрослому",
            "audience": "Группа для взрослых (продолжающие)",
            "teacher": "Преподаватель: Красноборова Людмила Анатольевна",
            "schedule": "Дни занятий: понедельник / четверг / пятница",
            "description": (
                "Курс для тех, кто уже влюблён во французский. Углубляем грамматику, "
                "отрабатываем разговорные ситуации и готовимся к международным экзаменам."
            ),
            "photo_url": "https://storage.yandexcloud.net/bigbob/vzros.png",
        },
        {
            "label": "👩🏼‍🏫 Индивидуальные занятия",
            "audience": "Французский, английский и корейский языки",
            "teacher": "Преподаватели: команда студии и Ксения Вшивкова",
            "schedule": "График подбирается персонально",
            "description": (
                "Персональные уроки под ваши цели: подготовка к экзаменам, "
                "разговорная практика или помощь по школе. Ксения Вшивкова ведёт "
                "индивидуальные занятия по французскому, английскому и корейскому языкам."
            ),
            "photo_url": "https://storage.yandexcloud.net/bigbob/indidvid.png",
        },
        {
            "label": "🍂 Осенний интенсив",
            "audience": "Краткосрочная программа",
            "schedule": "Сезонные смены, даты объявляются дополнительно",
            "description": (
                "Погружение в язык на время каникул: тематические мастер-классы, "
                "театральные проекты и квесты на французском."
            ),
            "photo_url": "https://storage.yandexcloud.net/bigbob/osen.png",
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
            "photo_url": "https://storage.yandexcloud.net/bigbob/nastych.jpg",
        },
        {
            "key": "bannikova",
            "name": "Анастасия Банникова",
            "description": (
                "Ведёт воскресные программы и театральные занятия. "
                "Создаёт дружелюбную атмосферу и помогает детям полюбить французский язык через игру и творчество."
            ),
            "photo_url": "https://storage.yandexcloud.net/bigbob/bannikova.jpg",
        },
        {
            "key": "marinot",
            "name": "Ален Марино",
            "description": (
                "Носитель французского языка с академическим парижским акцентом. "
                "Актёр и душа студии, который общается с учениками только по-французски и погружает в живую культуру."
            ),
            "photo_url": "https://storage.yandexcloud.net/bigbob/marinot.jpg",
        },
        {
            "key": "krasnoborova",
            "name": "Людмила Красноборова",
            "description": (
                "Кандидат филологических наук, доцент ПГНИУ и экзаменатор DALF. "
                "Готовит подростков и взрослых к экзаменам и олимпиадам, сочетая академизм и практику."
            ),
            "photo_url": "https://storage.yandexcloud.net/bigbob/lydmila.jpg",
        },
        {
            "key": "vshivkova",
            "name": "Ксения Вшивкова",
            "description": (
                "Владеет французским, английским и корейским языками. Студентка ПГНИУ (2021–2026), "
                "факультет современного иностранных языков и литератур по направлению «Перевод и переводоведение». "
                "Работает с детьми более четырёх лет. Ведёт групповые занятия по французскому и корейскому, "
                "а также индивидуальные уроки по французскому, английскому и корейскому языкам."
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
        application.add_handler(CallbackQueryHandler(self._about_show_direction, pattern=r"^about:"))
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
            [self.ADMIN_MANAGE_ADMINS_BUTTON],
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
        attachments: Optional[list[MediaAttachment]] = None,
    ) -> dict[str, Any]:
        chat = update.effective_chat
        user = update.effective_user
        record_id = data.get("id") or self._generate_registration_id()
        program_label = data.get("program", "")
        teacher = data.get("teacher") or self._resolve_program_teacher(str(program_label))
        stored_media = data.get("payment_media", [])
        if attachments and stored_media:
            payment_media = stored_media
        elif attachments:
            payment_media = self._attachments_to_dicts(attachments)
        else:
            payment_media = stored_media

        record = {
            "id": record_id,
            "program": program_label,
            "teacher": teacher,
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
            "payment_media": payment_media,
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
        if attachment.caption:
            return f"{title}: {attachment.caption}"
        return f"{title} во вложении"

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
            if payload.startswith("payment_"):
                if not self._is_admin_update(update, context):
                    await self._reply(
                        update,
                        "Просмотр вложений доступен только администраторам.",
                        reply_markup=self._main_menu_markup_for(update, context),
                    )
                    return
                registration_id = payload.split("payment_", 1)[1]
                handled = await self._send_registration_payment_media(
                    update,
                    context,
                    registration_id,
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

    async def _serialise_payment_media(
        self, context: ContextTypes.DEFAULT_TYPE, attachments: list[MediaAttachment]
    ) -> list[dict[str, str]]:
        """Convert payment attachments to a JSON-friendly structure with previews."""

        bot = getattr(context, "bot", None)
        serialised: list[dict[str, str]] = []
        for attachment in attachments:
            entry: dict[str, str] = {
                "kind": attachment.kind,
                "file_id": attachment.file_id,
                "caption": attachment.caption or "",
            }
            if attachment.kind == "photo" and bot is not None:
                try:
                    telegram_file = await bot.get_file(attachment.file_id)
                    buffer = io.BytesIO()
                    download = getattr(telegram_file, "download_to_memory", None)
                    if callable(download):
                        await download(out=buffer)
                    else:
                        await telegram_file.download(out=buffer)  # type: ignore[attr-defined]
                    mime = self._guess_mime_type(getattr(telegram_file, "file_path", None))
                    entry["preview_base64"] = base64.b64encode(buffer.getvalue()).decode("ascii")
                    if mime:
                        entry["preview_mime"] = mime
                except Exception as exc:  # pragma: no cover - network dependent
                    LOGGER.debug("Не удалось скачать фото оплаты: %s", exc)
            serialised.append(entry)
        return serialised

    async def _ensure_payment_previews(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        registrations: list[dict[str, Any]],
    ) -> bool:
        """Populate preview metadata for legacy payment photos."""

        updated = False
        for record in registrations:
            if not isinstance(record, dict):
                continue

            media_payload = record.get("payment_media")
            if not isinstance(media_payload, list) or not media_payload:
                continue

            needs_refresh = False
            for entry in media_payload:
                if not isinstance(entry, dict):
                    continue
                if entry.get("kind") != "photo":
                    continue
                if entry.get("preview_base64"):
                    continue
                needs_refresh = True
                break

            if not needs_refresh:
                continue

            attachments = self._dicts_to_attachments(media_payload)
            if not attachments:
                continue

            record["payment_media"] = await self._serialise_payment_media(context, attachments)
            updated = True

        if updated:
            self._storage_dirty = True
        return updated

    @staticmethod
    def _guess_mime_type(file_path: Optional[str]) -> Optional[str]:
        if not file_path:
            return None
        mime, _ = mimetypes.guess_type(file_path)
        return mime

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

    def _about_inline_keyboard(self) -> "InlineKeyboardMarkup":
        buttons = [
            [InlineKeyboardButton(program["label"], callback_data=f"about:{index}")]
            for index, program in enumerate(self.PROGRAMS)
        ]
        buttons.append([InlineKeyboardButton(self.BACK_BUTTON, callback_data="about:home")])
        return InlineKeyboardMarkup(buttons)

    def _teacher_inline_keyboard(self) -> "InlineKeyboardMarkup":
        buttons = [
            [InlineKeyboardButton(teacher["name"], callback_data=f"teacher:{teacher['key']}")]
            for teacher in self.TEACHERS
        ]
        buttons.append([InlineKeyboardButton(self.BACK_BUTTON, callback_data="teacher:home")])
        return InlineKeyboardMarkup(buttons)

    def _format_program_details(self, program: Dict[str, str]) -> str:
        lines = [program["label"]]
        description = program.get("description")
        if description:
            lines.append("")
            lines.append(description)
        for key in ("audience", "teacher", "schedule"):
            value = program.get(key)
            if value:
                lines.append(value)
        return "\n".join(line for line in lines if line is not None)

    def _resolve_program_teacher(self, program_label: str) -> str:
        for program in self.PROGRAMS:
            if program.get("label") == program_label:
                return program.get("teacher", "") or ""
        return ""

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
        selected_program: Optional[dict[str, str]] = None
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
            selected_program = program
        else:
            program_label = (message.text if message else "").strip()
            program = next((item for item in self.PROGRAMS if item["label"] == program_label), None)
            if not program:
                await self._registration_prompt_program_buttons(update, context)
                return self.REGISTRATION_PROGRAM
            selected_program = program

        registration = context.user_data.setdefault("registration", {})
        registration["program"] = program_label
        teacher = (selected_program or {}).get("teacher") or self._resolve_program_teacher(program_label)
        if teacher:
            registration["teacher"] = teacher
        else:
            registration.pop("teacher", None)

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
        for key in ("program", "teacher", "time", "saved_time", "saved_time_original", "proposed_time"):
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
            data["payment_media"] = await self._serialise_payment_media(context, attachments)
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

        teacher_line = data.get("teacher") or self._resolve_program_teacher(str(data.get("program", "")))

        summary = (
            "Ваша заявка принята!\n\n"
            f"👦 Участник: {data.get('child_name', '—')} ({data.get('class', '—')})\n"
            f"📱 Телефон: {data.get('phone', '—')}\n"
            f"🕒 Время: {data.get('time', '—')}\n"
            f"📚 Программа: {data.get('program', '—')}\n"
            f"💳 {payment_status}\n"
        )
        if teacher_line:
            summary += f"{teacher_line}\n"
        if payment_note:
            summary += f"📝 Комментарий: {payment_note}\n"
        summary += "\nМы свяжемся с вами в ближайшее время."

        await self._reply(update, summary, reply_markup=self._main_menu_markup_for(update, context))
        record = self._store_registration(update, context, data, attachments)

        admin_message = (
            "🆕 Новая заявка\n"
            f"📚 Программа: {data.get('program', '—')}\n"
            f"👦 Участник: {data.get('child_name', '—')} ({data.get('class', '—')})\n"
            f"📱 Телефон: {data.get('phone', '—')}\n"
            f"🕒 Время: {data.get('time', '—')}\n"
            f"💳 Статус оплаты: {'получено' if attachments else 'ожидается'}"
        )
        if teacher_line:
            admin_message += f"\n{teacher_line}"
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
        registrations = self._application_data(context).get("registrations", [])
        if not isinstance(registrations, list) or not registrations:
            await self._reply(
                update,
                "Заявок пока нет.",
                reply_markup=self._admin_menu_markup(),
            )
            return

        if await self._ensure_payment_previews(context, registrations):
            self._save_persistent_state()

        export_path, generated_at = self._export_registrations_excel(
            context,
            registrations,
        )
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
        message_parts.append("")
        message_parts.append(
            "🖼 Фото подтверждения оплаты встроено в столбец «Фото оплаты» и сохраняется в формате JPG."
        )
        if deeplink:
            message_parts.append("")
            message_parts.append(f"🔗 Таблица: {deeplink}")
            message_parts.append(
                "Нажмите ссылку, чтобы в любой момент получить свежую версию."
            )
        message_parts.append("")
        message_parts.append(
            "🔽 Файл с таблицей отправлен ниже. Сохраните его в «Избранном» для быстрого доступа."
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
        builder = _SimpleXlsxBuilder(
            sheet_name="Заявки",
            column_widths=self.EXPORT_COLUMN_WIDTHS,
        )
        header = (
            "Дата заявки",
            "Программа",
            "Участник",
            "Класс / возраст",
            "Телефон",
            "Предпочтительное время",
            "Фото оплаты",
            "Отправитель",
        )
        builder.add_row(header)

        for record in registrations:
            payment_entries = self._dicts_to_attachments(record.get("payment_media"))
            payment_note = record.get("payment_note") or ""
            preview_info = self._extract_payment_preview(record)
            photo_cell = self._build_payment_photo_cell(
                preview_info=preview_info,
                attachments=payment_entries,
                payment_note=payment_note,
            )

            builder.add_row(
                (
                    record.get("created_at") or "",
                    record.get("program") or "",
                    record.get("child_name") or "",
                    record.get("class") or "",
                    record.get("phone") or "",
                    record.get("time") or "",
                    photo_cell,
                    record.get("submitted_by") or "",
                )
            )

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

    def _extract_payment_preview(
        self, record: dict[str, Any]
    ) -> Optional[tuple[Optional[str], bytes, str, str]]:
        media_payload = record.get("payment_media")
        if not isinstance(media_payload, list):
            return None
        for entry in media_payload:
            if not isinstance(entry, dict):
                continue
            encoded = entry.get("preview_base64")
            if not encoded:
                continue
            try:
                data = base64.b64decode(encoded)
            except Exception:
                continue
            mime = entry.get("preview_mime") or "image/jpeg"
            caption = entry.get("caption") or ""
            file_id = entry.get("file_id")
            return (str(file_id) if file_id else None, data, mime, caption)
        return None

    def _ensure_jpeg_preview(self, data: bytes, mime: str) -> tuple[bytes, str]:
        """Return image bytes and mime-type ensuring a JPEG payload when possible."""

        target_mime = "image/jpeg"
        if mime.lower() in {"image/jpeg", "image/jpg"}:
            return data, target_mime

        if Image is None:
            LOGGER.debug("Pillow недоступен — отправляем изображение как есть (%s)", mime)
            return data, mime or target_mime

        try:
            with Image.open(io.BytesIO(data)) as original:
                converted = original.convert("RGB")
                buffer = io.BytesIO()
                converted.save(buffer, format="JPEG", quality=85)
                return buffer.getvalue(), target_mime
        except Exception as exc:  # pragma: no cover - depends on Pillow backend
            LOGGER.debug("Не удалось преобразовать изображение в JPEG: %s", exc)
            return data, mime or target_mime

    def _build_payment_photo_cell(
        self,
        preview_info: Optional[tuple[Optional[str], bytes, str, str]],
        attachments: list[MediaAttachment],
        payment_note: str,
    ) -> _XlsxCell:
        text_chunks: list[str] = []
        if payment_note:
            text_chunks.append(payment_note)

        primary_file_id: Optional[str] = None
        image: Optional[_XlsxImage] = None
        if preview_info is not None:
            primary_file_id, preview_bytes, preview_mime, caption = preview_info
            jpeg_bytes, jpeg_mime = self._ensure_jpeg_preview(preview_bytes, preview_mime)
            image_description = caption or payment_note or "Фото оплаты"
            image = _XlsxImage(
                data=jpeg_bytes,
                content_type=jpeg_mime,
                description=image_description,
            )
            if caption:
                text_chunks.append(caption)

        remaining = attachments
        if primary_file_id:
            remaining = [item for item in attachments if item.file_id != primary_file_id]

        if remaining:
            text_chunks.extend(self._describe_attachment(item) for item in remaining)

        if attachments and not text_chunks:
            text_chunks.append("Фото прикреплено")

        if not text_chunks:
            text_chunks.append("Оплата ожидается")

        cell_text = "\n\n".join(text_chunks).strip()
        return _XlsxCell(cell_text, image=image)

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

    async def _build_registrations_deeplink(
        self, context: ContextTypes.DEFAULT_TYPE
    ) -> Optional[str]:
        username = await self._ensure_bot_username(context)
        if not username:
            return None
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
            path, generated_at, _ = self._export_registrations_excel(
                context,
                registrations,
            )

        try:
            chat_id = _coerce_chat_id_from_object(chat)
        except ValueError:
            return False

        caption = (
            "📊 Таблица заявок студии «Конфетти»\n"
            f"Обновлено: {generated_at}\n"
            "Документ включает все заявки и обновляется при каждом экспорте.\n"
            "Фото подтверждения оплаты встроено прямо в колонку «Фото оплаты» (формат JPG)."
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

    async def _send_registration_payment_media(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        registration_id: str,
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

        summary_lines = [
            "💳 Вложения по заявке",
            f"👦 Участник: {record.get('child_name', '—')} ({record.get('class', '—')})",
            f"📚 Программа: {record.get('program', '—')}",
            f"🗓 Создана: {record.get('created_at', '—')}",
            f"📎 Файлов: {len(attachments)}",
        ]

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
                media=attachments,
                reply_markup=self._admin_menu_markup(),
            )
        except Exception as exc:  # pragma: no cover - network dependent
            LOGGER.warning("Не удалось отправить вложения заявки %s: %s", registration_id, exc)
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
        if key == "home":
            await query.answer()
            await self._send_teachers(update, context)
            return

        teacher = next((item for item in self.TEACHERS if item["key"] == key), None)
        if teacher is None:
            await query.answer("Педагог не найден.", show_alert=True)
            return

        await query.answer()
        caption = f"{teacher['name']}\n\n{teacher['description']}"
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
        if key == "home":
            await query.answer()
            await self._send_about(update, context)
            return

        try:
            index = int(key)
        except ValueError:
            await query.answer("Неизвестное направление.", show_alert=True)
            return

        if not 0 <= index < len(self.PROGRAMS):
            await query.answer("Направление не найдено.", show_alert=True)
            return

        program = self.PROGRAMS[index]
        await query.answer()

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

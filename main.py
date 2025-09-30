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

import logging
import os
import random
import re
from datetime import datetime
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


if TYPE_CHECKING:
    from telegram import KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove, Update
    from telegram.ext import (
        AIORateLimiter as _AIORateLimiter,
        Application,
        ApplicationBuilder,
        CommandHandler,
        ContextTypes,
        ConversationHandler,
        MessageHandler,
        filters,
    )
else:  # pragma: no cover - import depends on environment
    try:
        from telegram import KeyboardButton, ReplyKeyboardMarkup, ReplyKeyboardRemove, Update
        from telegram.ext import (
            Application,
            ApplicationBuilder,
            CommandHandler,
            ContextTypes,
            ConversationHandler,
            MessageHandler,
            filters,
        )
    except ModuleNotFoundError as exc:  # pragma: no cover - environment specific
        TELEGRAM_IMPORT_ERROR = exc
        KeyboardButton = ReplyKeyboardMarkup = ReplyKeyboardRemove = Update = object  # type: ignore[assignment]
        Application = ApplicationBuilder = CommandHandler = ConversationHandler = MessageHandler = object  # type: ignore[assignment]
        ContextTypes = object  # type: ignore[assignment]
        filters = _MissingTelegramModule()  # type: ignore[assignment]
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
                    "🇫🇷 Voici nos horaires actuels :\n"
                    "🇷🇺 Наше актуальное расписание:\n\n"
                    "☀️ Matin / Утро : 10:00 – 12:00\n"
                    "🌤 Après-midi / День : 14:00 – 16:00\n"
                    "🌙 Soir / Вечер : 18:00 – 20:00"
                )
            ),
            about=ContentBlock(
                text=(
                    "🇫🇷 À propos de nous\n"
                    "Notre compagnie existe déjà depuis 8 ans, et pendant ce temps elle est devenue un lieu où les enfants découvrent toute la beauté de la langue et de la culture françaises.\n"
                    "Notre équipe est composée uniquement de professionnels :\n"
                    "• des enseignants avec une formation supérieure spécialisée et des diplômes avec mention,\n"
                    "• des titulaires du certificat international DALF,\n"
                "• des professeurs avec plus de 10 ans d’expérience,\n"
                "• ainsi que des locuteurs natifs qui partagent l’authenticité de la culture française.\n"
                "Chaque année, nous participons à des festivals francophones dans toute la Russie — de Moscou et Saint-Pétersbourg à Ekaterinbourg et Valdaï. Nous nous produisons régulièrement sur les scènes de notre ville (par exemple à l’école n° 22), nous organisons des fêtes populaires en France, et nous clôturons chaque saison par un événement festif attendu par tous nos élèves.\n"
                "Notre objectif principal est simple mais essentiel : 👉 que les enfants tombent amoureux du français ❤️\n\n"
                "🇷🇺 О нас\n"
                "Наша студия существует уже 8 лет, и за это время она стала местом, где дети открывают для себя красоту французского языка и культуры.\n"
                "С нами работают только профессионалы:\n"
                "• преподаватели с высшим профильным образованием и красными дипломами,\n"
                "• обладатели международного сертификата DALF,\n"
                "• педагоги со стажем более 10 лет,\n"
                "• а также носители языка, которые делятся аутентичным французским опытом.\n"
                "Каждый год мы участвуем во франкофонных фестивалях по всей России — от Москвы и Санкт-Петербурга до Екатеринбурга и Валдая. Мы регулярно выступаем на площадках города (например, в школе № 22), организуем праздники, любимые во Франции, и делаем яркое закрытие сезона, которое ждут все наши ученики.\n"
                "Наша главная цель проста и очень важна: 👉 чтобы дети полюбили французский язык ❤️\n\n"
                "🎭 Chez nous, Confetti = fête !\n🎭 У нас Конфетти = это всегда праздник!"
                )
            ),
            teachers=ContentBlock(
                text=(
                    "🇫🇷 Nos enseignants sont passionnés et expérimentés.\n"
                    "🇷🇺 Наши преподаватели — увлечённые и опытные педагоги.\n\n"
                    "👩‍🏫 Ksenia Nastytsch\n"
                    "Enseignante de français avec plus de 20 ans d’expérience.\n"
                    "Diplômée de l’Université d’État de Perm en philologie (français, anglais, allemand et espagnol).\n"
                "Titulaire du certificat international DALF, a effectué des stages en France (Grenoble, Pau, Metz).\n\n"
                "Ксения Настыч\n"
                "Преподаватель французского языка с опытом работы более 20 лет.\n"
                "Окончила Пермский государственный университет по специальности «Филология».\n"
                "Обладатель международного сертификата DALF, проходила стажировки во Франции (Гренобль, По, Мец). Организовывала в течение трёх лет «русские сезоны» в Посольстве России во Франции.\n\n"
                "👩‍🏫 Анастасия Банникова\n\n"
                "🇫🇷 Alain Marinot\nLocuteur natif du français avec un accent académique parisien. Acteur et âme de l’école, il parle exclusivement en français — un grand avantage pour les élèves.\n\n"
                "🇷🇺 Ален Марино\nНоситель французского языка с академическим парижским акцентом. Актёр, душа школы, говорит исключительно по-французски — большая удача для учеников.\n\n"
                "🇫🇷 Lyudmila Anatolievna Krasnoborova\nEnseignante de français, docteur en philologie, maîtresse de conférences à l’Université d’État de Perm (PGNIU).\n"
                "Examinateur DALF, prépare aux examens du baccalauréat russe (ЕГЭ) et aux olympiades.\n\n"
                "🇷🇺 Красноборова Людмила Анатольевна\nПреподаватель французского языка, кандидат филологических наук, доцент ПГНИУ.\n"
                "Экзаменатор DALF, готовит к ЕГЭ и олимпиадам."
                )
            ),
            payment=ContentBlock(
                text=(
                    "🇫🇷 Veuillez envoyer une photo ou un reçu de paiement ici.\n"
                    "🇷🇺 Пожалуйста, отправьте сюда фото или чек об оплате.\n\n"
                    "📌 Après vérification, nous confirmerons votre inscription.\n"
                    "📌 После проверки мы подтвердим вашу запись."
                )
            ),
            album=ContentBlock(
                text=(
                    "🇫🇷 Regardez nos meilleurs moments 🎭\n"
                    "🇷🇺 Посмотрите наши лучшие моменты 🎭\n\n"
                    "👉 https://confetti.ru/album"
                )
            ),
            contacts=ContentBlock(
                text=(
                    "📞 Téléphone : +7 (900) 000-00-00\n"
                    "📧 Email : confetti@example.com\n"
                    "🌐 Site / Сайт : https://confetti.ru\n"
                    "📲 Telegram : @ConfettiAdmin"
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

    REGISTRATION_PROGRAM = 1
    REGISTRATION_CHILD_NAME = 2
    REGISTRATION_CLASS = 3
    REGISTRATION_CONTACT_PERSON = 4
    REGISTRATION_PHONE = 5
    REGISTRATION_TIME = 6
    REGISTRATION_PAYMENT = 7

    CANCELLATION_PROGRAM = 21
    CANCELLATION_REASON = 22

    MAIN_MENU_BUTTON = "⬅️ Главное меню"
    REGISTRATION_BUTTON = "📝 Запись / Inscription"
    CANCELLATION_BUTTON = "❗️ Отменить занятие / Annuler"
    REGISTRATION_SKIP_PAYMENT_BUTTON = "⏭ Пока без оплаты"
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
    ADMIN_CANCEL_PROMPT = (
        "\n\nЧтобы отменить, напишите «Отмена».\nPour annuler, envoyez «Annuler»."
    )

    MAIN_MENU_LAYOUT = (
        (REGISTRATION_BUTTON, "📅 Расписание / Horaires"),
        ("ℹ️ О студии / À propos de nous", "👩‍🏫 Преподаватели / Enseignants"),
        ("📸 Фотоальбом / Album photo", "📞 Контакты / Contact"),
        ("📚 Полезные слова / Vocabulaire", CANCELLATION_BUTTON),
    )

    TIME_OF_DAY_OPTIONS = (
        "☀️ Утро / Matin",
        "🌤 День / Après-midi",
        "🌙 Вечер / Soir",
    )

    PROGRAMS = (
        {
            "label": "📚 français au quotidien / французский каждый день",
            "audience": "С 3 по 11 класс",
            "teacher": "Преподаватель - Настыч Ксения Викторовна",
            "schedule": "Дни занятий: вторник или четверг вечер",
        },
        {
            "label": "🎭 théâtre francophone / театр на французском",
            "teacher": "Преподаватель - Настыч Ксения Викторовна",
            "schedule": "Дни занятий: вторник или четверг вечер",
        },
        {
            "label": "📚 français du dimanche / воскресный французский",
            "audience": "1-4 класс",
            "teacher": "Преподаватель - Банникова Анастасия Дмитриевна",
            "schedule": "Дни занятий: воскресенье",
        },
        {
            "label": "🎭 théâtre francophone / театр на французском (воскресенье)",
            "teacher": "Преподаватель - Банникова Анастасия Дмитриевна",
            "schedule": "Дни занятий: воскресенье",
        },
        {
            "label": "🇫🇷 Français au sérieux / Французский по-взрослому",
            "audience": "Группа для взрослых (продолжающие)",
            "teacher": "Преподаватель - Красноборова Людмила Анатольевна",
            "schedule": "Дни занятий: понедельник / четверг / пятница",
        },
        {
            "label": "👩🏼‍🏫 cours en individuel / Индивидуальные занятия",
        },
        {
            "label": "🍂 Stage d'automne / осенний интенсив",
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
        self._bot_username: Optional[str] = None

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
                    MessageHandler(
                        filters.Regex(self._programs_regex()),
                        self._registration_collect_program,
                    ),
                    MessageHandler(
                        filters.Regex(self._exact_match_regex(self.MAIN_MENU_BUTTON)),
                        self._registration_cancel,
                    ),
                ],
                self.REGISTRATION_CHILD_NAME: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self._registration_collect_child_name),
                ],
                self.REGISTRATION_CLASS: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self._registration_collect_class),
                ],
                self.REGISTRATION_CONTACT_PERSON: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self._registration_collect_contact_person),
                ],
                self.REGISTRATION_PHONE: [
                    MessageHandler(filters.CONTACT, self._registration_collect_phone_contact),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self._registration_collect_phone_text),
                ],
                self.REGISTRATION_TIME: [
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
                        filters.Regex(self._programs_regex()),
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
        application.add_handler(MessageHandler(~filters.COMMAND, self._handle_message))

    def _exact_match_regex(self, text: str) -> str:
        return rf"^{re.escape(text)}$"

    def _programs_regex(self) -> str:
        parts = [re.escape(program["label"]) for program in self.PROGRAMS]
        return rf"^({'|'.join(parts)})$"

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

        if hasattr(context, "application_data"):
            return context.application_data  # type: ignore[attr-defined]

        if hasattr(context, "bot_data"):
            return context.bot_data  # type: ignore[attr-defined]

        application = getattr(context, "application", None)
        if application is not None and hasattr(application, "bot_data"):
            return application.bot_data  # type: ignore[attr-defined]

        # Fallback to a dedicated attribute to avoid repeated lookups if nothing matches.
        storage = getattr(context, "_fallback_application_data", None)
        if isinstance(storage, dict):
            return storage

        storage = {}
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
        existing.add(admin_id)
        storage["dynamic_admins"] = existing
        self._runtime_admin_ids.add(admin_id)
        return existing

    def _remember_chat(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        self._refresh_admin_cache(context)
        chat = update.effective_chat
        if not chat:
            return
        known = self._get_known_chats(context)
        known.add(_coerce_chat_id_from_object(chat))

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
            return converted
        converted: set[int] = set()
        self._application_data(context)["known_chats"] = converted
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
            return restored
        fresh = self.content_template.copy()
        self._application_data(context)["content"] = fresh
        return fresh

    def _store_registration(
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
            "child_name": data.get("child_name", ""),
            "class": data.get("class", ""),
            "contact_person": data.get("contact_person", ""),
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
        if isinstance(registrations, list):
            registrations.append(record)
        else:
            self._application_data(context)["registrations"] = [record]

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

        admin_message = (
            "🚫 Отмена занятия\n"
            f"📚 Программа: {record.get('program', '—')}\n"
            f"📝 Комментарий: {record.get('details', '—')}\n"
            f"👤 Отправил: {record.get('submitted_by', '—')}"
        )
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
                        "Этот раздел доступен только администраторам.\n"
                        "Section réservée aux administrateurs.",
                        reply_markup=self._main_menu_markup_for(update, context),
                    )
                    return
                sent = await self._send_registrations_excel(update, context)
                if sent:
                    await self._reply(
                        update,
                        "Экспорт завершён. Таблица отправлена сообщением выше.\n"
                        "Le tableau vient d'être envoyé dans cette conversation.",
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
            message += (
                "\n\n🛠 Для управления ботом откройте «Админ-панель» в меню."
                "\n🛠 Pour administrer le bot, choisissez «Админ-панель»."
            )
        await self._reply(update, message, reply_markup=self._main_menu_markup_for(update, context))

    async def _show_admin_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._is_admin_update(update, context):
            await self._reply(
                update,
                "Эта панель доступна только администраторам.\n"
                "Ce panneau est réservé aux administrateurs.",
                reply_markup=self._main_menu_markup_for(update, context),
            )
            return
        self._remember_chat(update, context)
        message = (
            "Админ-панель открыта. Выберите действие ниже.\n"
            "Panneau d'administration ouvert — choisissez une action."
        )
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
            greeting += (
                "\n\n🛠 У вас есть доступ к админ-панели — нажмите кнопку ниже, чтобы управлять контентом."
                "\n🛠 Vous pouvez gérer le contenu via le bouton «Админ-панель»."
            )
        await self._reply(update, greeting, reply_markup=self._main_menu_markup_for(update, context))

    async def _reply(
        self,
        update: Update,
        text: Optional[str] = None,
        *,
        reply_markup: Optional[ReplyKeyboardMarkup | ReplyKeyboardRemove] = None,
        media: Optional[list[MediaAttachment]] = None,
    ) -> None:
        message = update.message
        callback = update.callback_query
        target = message or (callback.message if callback else None)

        if callback:
            await callback.answer()

        markup_used = False

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

    async def _start_registration(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        self._remember_chat(update, context)
        context.user_data["registration"] = {}
        message_lines = [
            "🇫🇷 À quel programme souhaitez-vous inscrire votre enfant ou vous inscrire ?",
            "🇷🇺 На какую программу вы хотите записать ребёнка или записать себя?",
        ]
        details = [self._format_program_details(program) for program in self.PROGRAMS]
        await self._reply(
            update,
            "\n".join(message_lines + details),
            reply_markup=self._program_keyboard(),
        )
        return self.REGISTRATION_PROGRAM

    def _program_keyboard(self) -> ReplyKeyboardMarkup:
        keyboard = [[program["label"]] for program in self.PROGRAMS]
        keyboard.append([self.MAIN_MENU_BUTTON])
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)

    def _format_program_details(self, program: Dict[str, str]) -> str:
        parts = [program["label"]]
        for key in ("audience", "teacher", "schedule"):
            if value := program.get(key):
                parts.append(value)
        return "\n".join(parts)

    async def _registration_collect_program(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        program_label = update.message.text
        context.user_data.setdefault("registration", {})["program"] = program_label
        await self._reply(
            update,
            "Merci ! / Спасибо! Напишите, пожалуйста, имя и фамилию ребёнка.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return self.REGISTRATION_CHILD_NAME

    async def _registration_collect_child_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        context.user_data.setdefault("registration", {})["child_name"] = update.message.text.strip()
        await self._reply(
            update,
            "🇫🇷 Indiquez la classe, s'il vous plaît.\n🇷🇺 Укажите, пожалуйста, класс.",
        )
        return self.REGISTRATION_CLASS

    async def _registration_collect_class(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        context.user_data.setdefault("registration", {})["class"] = update.message.text.strip()
        await self._reply(
            update,
            "🇫🇷 Qui est la personne de contact ?\n🇷🇺 Кто будет контактным лицом?",
        )
        return self.REGISTRATION_CONTACT_PERSON

    async def _registration_collect_contact_person(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        context.user_data.setdefault("registration", {})["contact_person"] = update.message.text.strip()
        await self._reply(
            update,
            "🇫🇷 Envoyez le numéro de téléphone (bouton en bas).\n"
            "🇷🇺 Отправьте номер телефона (кнопка внизу).",
            reply_markup=self._phone_keyboard(),
        )
        return self.REGISTRATION_PHONE

    def _phone_keyboard(self) -> ReplyKeyboardMarkup:
        keyboard = [
            [KeyboardButton("📱 Отправить номер", request_contact=True)],
            [KeyboardButton(self.MAIN_MENU_BUTTON)],
        ]
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)

    def _payment_keyboard(self) -> ReplyKeyboardMarkup:
        keyboard = [
            [KeyboardButton(self.REGISTRATION_SKIP_PAYMENT_BUTTON)],
            [KeyboardButton(self.MAIN_MENU_BUTTON)],
        ]
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)

    async def _registration_collect_phone_contact(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        phone_number = update.message.contact.phone_number
        context.user_data.setdefault("registration", {})["phone"] = phone_number
        return await self._prompt_time_of_day(update)

    async def _registration_collect_phone_text(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        text = update.message.text.strip()
        if text == self.MAIN_MENU_BUTTON:
            return await self._registration_cancel(update, context)
        context.user_data.setdefault("registration", {})["phone"] = text
        return await self._prompt_time_of_day(update)

    async def _prompt_time_of_day(self, update: Update) -> int:
        await self._reply(
            update,
            "🇫🇷 Choisissez le moment qui vous convient.\n"
            "🇷🇺 Выберите удобное время занятий.",
            reply_markup=self._time_keyboard(),
        )
        return self.REGISTRATION_TIME

    def _time_keyboard(self) -> ReplyKeyboardMarkup:
        keyboard = [[option] for option in self.TIME_OF_DAY_OPTIONS]
        keyboard.append([self.MAIN_MENU_BUTTON])
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)

    async def _registration_collect_time(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        context.user_data.setdefault("registration", {})["time"] = update.message.text.strip()
        return await self._prompt_payment_request(update, context)

    async def _prompt_payment_request(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        instructions = self._get_content(context).payment
        message = (
            "💳 🇫🇷 Envoyez une confirmation du paiement (photo, vidéo ou fichier).\n"
            "💳 🇷🇺 Отправьте подтверждение оплаты (фото, видео или файл).\n\n"
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
            "❌ Регистрация отменена.\n❌ L'inscription est annulée.",
            reply_markup=self._main_menu_markup_for(update, context),
        )
        return ConversationHandler.END

    # ------------------------------------------------------------------
    # Cancellation conversation

    async def _start_cancellation(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        self._remember_chat(update, context)
        context.user_data["cancellation"] = {}
        message = (
            "❗️ 🇫🇷 Indiquez la séance que vous annulez.\n"
            "❗️ 🇷🇺 Укажите занятие, которое вы пропускаете.\n\n"
            "⚠️ Оплата не возвращается — средства остаются на балансе студии."
        )
        await self._reply(update, message, reply_markup=self._program_keyboard())
        return self.CANCELLATION_PROGRAM

    async def _cancellation_collect_program(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        context.user_data.setdefault("cancellation", {})["program"] = update.message.text.strip()
        await self._reply(
            update,
            "📅 Напишите дату и время пропуска, а также короткий комментарий.\n"
            "📅 Indiquez la date, l'heure et un commentaire, s'il vous plaît.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return self.CANCELLATION_REASON

    async def _cancellation_collect_reason(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> int:
        data = context.user_data.setdefault("cancellation", {})
        text, attachments = self._extract_message_payload(update.message)

        if text == self.MAIN_MENU_BUTTON:
            return await self._cancellation_cancel(update, context)

        if attachments:
            data["evidence"] = self._attachments_to_dicts(attachments)
        data["details"] = text or ""

        await self._store_cancellation(update, context, data, attachments or None)

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
            "Отмена занятия не отправлена.\nAnnulation ignorée.",
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
        payment_status = "✅ Paiement reçu" if attachments else "⏳ Paiement en attente"
        payment_status_ru = "✅ Оплата подтверждена" if attachments else "⏳ Оплата ожидается"

        summary = (
            "🇫🇷 Votre demande a été enregistrée !\n"
            "🇷🇺 Ваша заявка принята!\n\n"
            f"👦 Enfant : {data.get('child_name', '—')} ({data.get('class', '—')})\n"
            f"👤 Contact : {data.get('contact_person', '—')}\n"
            f"📱 Téléphone : {data.get('phone', '—')}\n"
            f"🕒 Heure : {data.get('time', '—')}\n"
            f"📚 Programme : {data.get('program', '—')}\n"
            f"💳 {payment_status} | {payment_status_ru}\n"
        )
        if payment_note:
            summary += f"📝 Remarque : {payment_note}\n"
        summary += (
            "\nNous vous contacterons prochainement.\n"
            "Мы свяжемся с вами в ближайшее время."
        )

        await self._reply(update, summary, reply_markup=self._main_menu_markup_for(update, context))
        self._store_registration(update, context, data, attachments)

        admin_message = (
            "🆕 Новая заявка / Nouvelle inscription\n"
            f"📚 Программа: {data.get('program', '—')}\n"
            f"👦 Участник: {data.get('child_name', '—')} ({data.get('class', '—')})\n"
            f"👤 Контакт: {data.get('contact_person', '—')} | {data.get('phone', '—')}\n"
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
            await self._show_main_menu(update, context)
            return

        profile = self.build_profile(update.effective_chat, update.effective_user)
        pending = context.chat_data.get("pending_admin_action")

        if pending and profile.is_admin:
            if text and text.strip().lower() in self._admin_cancel_tokens:
                context.chat_data.pop("pending_admin_action", None)
                await self._reply(
                    update,
                    "Действие отменено.\nL'action est annulée.",
                    reply_markup=self._admin_menu_markup(),
                )
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
                    "Отправьте сообщение или медиа для рассылки.\n"
                    "Envoyez le message ou les médias à diffuser."
                    + self.ADMIN_CANCEL_PROMPT,
                    reply_markup=ReplyKeyboardRemove(),
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
                    "Entrez le chat_id de l'administrateur."
                    + self.ADMIN_CANCEL_PROMPT,
                    reply_markup=ReplyKeyboardRemove(),
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
                "📌 Пожалуйста, используйте кнопки меню или отправьте текстовое сообщение.\n"
                "📌 Merci d'utiliser le menu en bas de l'écran.",
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
                    "Не удалось определить редактируемый блок.\n"
                    "Impossible d'identifier la section à modifier.",
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
            "Неизвестное действие администратора.\nAction administrateur inconnue.",
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
                "Пожалуйста, отправьте числовой chat_id администратора.\n"
                "Veuillez envoyer un identifiant numérique."
                + self.ADMIN_CANCEL_PROMPT,
                reply_markup=ReplyKeyboardRemove(),
            )
            context.chat_data["pending_admin_action"] = {"type": "add_admin"}
            return

        if admin_id in self._runtime_admin_ids:
            await self._reply(
                update,
                "Этот chat_id уже обладает правами администратора.\n"
                "Cet identifiant est déjà administrateur.",
                reply_markup=self._admin_menu_markup(),
            )
            return

        self._store_dynamic_admin(context, admin_id)
        message = (
            f"✅ Администратор {admin_id} добавлен.\n"
            "✅ Nouvel administrateur ajouté."
        )
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
                "Этот раздел нельзя редактировать.\nCette section ne peut pas être modifiée.",
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
        await self._reply(update, message, reply_markup=ReplyKeyboardRemove())

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
            "\nEnvoyez les entrées sous forme: mot|emoji|traduction|phrase FR|phrase RU."
            f"\n\nТекущий список:\n{sample}"
        )
        await self._reply(update, message + self.ADMIN_CANCEL_PROMPT, reply_markup=ReplyKeyboardRemove())

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
                "Пока нет чатов для рассылки.\nAucun chat connu pour la diffusion.",
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

        result = (
            f"Рассылка завершена: {successes} из {len(known_chats)} чатов.\n"
            f"Diffusion envoyée: {successes} / {len(known_chats)}."
        )
        if failures:
            result += "\nНе удалось доставить сообщения в чаты: " + ", ".join(failures)
        await self._reply(update, result, reply_markup=self._admin_menu_markup())

    async def _admin_share_registrations_table(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        registrations = self._application_data(context).get("registrations", [])
        if not isinstance(registrations, list) or not registrations:
            await self._reply(
                update,
                "Заявок пока нет.\nAucune demande enregistrée pour l'instant.",
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
                "Контактное лицо",
                "Телефон",
                "Предпочтительное время",
                "Оплата",
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
            builder.add_row(
                (
                    record.get("created_at") or "",
                    record.get("program") or "",
                    record.get("child_name") or "",
                    record.get("class") or "",
                    record.get("contact_person") or "",
                    record.get("phone") or "",
                    record.get("time") or "",
                    payment_status,
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

        registrations = self._application_data(context).get("registrations", [])
        if path is None or generated_at is None:
            if not isinstance(registrations, list) or not registrations:
                await self._reply(
                    update,
                    "Заявок пока нет.\nAucune demande enregistrée pour l'instant.",
                    reply_markup=self._admin_menu_markup(),
                )
                return False
            path, generated_at = self._export_registrations_excel(context, registrations)

        try:
            chat_id = _coerce_chat_id_from_object(chat)
        except ValueError:
            return False

        caption = (
            "📊 Tableau des inscriptions Confetti\n"
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
                "Этот раздел нельзя редактировать.\nCette section ne peut pas être modifiée.",
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
            "Раздел обновлён!\nLa section a été mise à jour.",
            reply_markup=self._admin_menu_markup(),
        )
        await self._notify_admins(
            context,
            f"🛠 Раздел «{label}» был обновлён администратором.",
            media=attachments or None,
        )

    async def _admin_apply_vocabulary_update(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, payload: str
    ) -> bool:
        lines = [line.strip() for line in payload.splitlines() if line.strip()]
        if not lines:
            await self._reply(
                update,
                "Отправьте хотя бы одну строку с данными.\nVeuillez fournir au moins une entrée."
                + self.ADMIN_CANCEL_PROMPT,
                reply_markup=ReplyKeyboardRemove(),
            )
            return False

        entries: list[dict[str, str]] = []
        for line in lines:
            parts = [part.strip() for part in line.split("|")]
            if len(parts) != 5:
                await self._reply(
                    update,
                    "Неверный формат. Используйте 5 частей через вертикальную черту.\n"
                    "Format incorrect: 5 éléments séparés par |."
                    + self.ADMIN_CANCEL_PROMPT,
                    reply_markup=ReplyKeyboardRemove(),
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
            f"Обновлено слов: {len(entries)}.\nNombre d'entrées: {len(entries)}.",
            reply_markup=self._admin_menu_markup(),
        )
        return True


    async def _handle_menu_selection(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        text = (update.message.text or "").strip()
        handlers = {
            "📅 Расписание / Horaires": self._send_schedule,
            "ℹ️ О студии / À propos de nous": self._send_about,
            "👩‍🏫 Преподаватели / Enseignants": self._send_teachers,
            "📸 Фотоальбом / Album photo": self._send_album,
            "📞 Контакты / Contact": self._send_contacts,
            "📚 Полезные слова / Vocabulaire": self._send_vocabulary,
        }

        handler = handlers.get(text)
        if handler is None:
            await self._reply(
                update,
                "Пожалуйста, воспользуйтесь меню внизу экрана.\n"
                "Merci de choisir une option dans le menu ci-dessous.",
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
                "📎 Материал доступен во вложениях.\n📎 Contenu disponible en pièce jointe.",
                reply_markup=reply_markup,
                media=media,
            )
            return
        await self._reply(
            update,
            "Раздел пока пуст.\nCette section est vide pour le moment.",
            reply_markup=reply_markup,
        )

    async def _send_schedule(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        content = self._get_content(context)
        await self._send_content_block(update, context, content.schedule)

    async def _send_about(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        content = self._get_content(context)
        await self._send_content_block(update, context, content.about)

    async def _send_teachers(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        content = self._get_content(context)
        await self._send_content_block(update, context, content.teachers)

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
    application.run_polling()


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

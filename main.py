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
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Union

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

try:  # pragma: no cover - import error path depends on the environment
    from telegram.ext import AIORateLimiter
except ImportError:  # pragma: no cover - see comment above
    AIORateLimiter = None  # type: ignore[assignment]


LOGGER = logging.getLogger(__name__)


ChatIdInput = Union[int, str]
AdminChatIdsInput = Union[ChatIdInput, Iterable[ChatIdInput], None]


@dataclass
class BotContent:
    """Mutable content blocks that administrators can edit at runtime."""

    schedule: str
    about: str
    teachers: str
    payment: str
    album: str
    contacts: str
    vocabulary: list[dict[str, str]]

    @classmethod
    def default(cls) -> "BotContent":
        return cls(
            schedule=(
                "🇫🇷 Voici nos horaires actuels :\n"
                "🇷🇺 Наше актуальное расписание:\n\n"
                "☀️ Matin / Утро : 10:00 – 12:00\n"
                "🌤 Après-midi / День : 14:00 – 16:00\n"
                "🌙 Soir / Вечер : 18:00 – 20:00"
            ),
            about=(
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
            ),
            teachers=(
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
            ),
            payment=(
                "🇫🇷 Veuillez envoyer une photo ou un reçu de paiement ici.\n"
                "🇷🇺 Пожалуйста, отправьте сюда фото или чек об оплате.\n\n"
                "📌 Après vérification, nous confirmerons votre inscription.\n"
                "📌 После проверки мы подтвердим вашу запись."
            ),
            album=(
                "🇫🇷 Regardez nos meilleurs moments 🎭\n"
                "🇷🇺 Посмотрите наши лучшие моменты 🎭\n\n"
                "👉 https://confetti.ru/album"
            ),
            contacts=(
                "📞 Téléphone : +7 (900) 000-00-00\n"
                "📧 Email : confetti@example.com\n"
                "🌐 Site / Сайт : https://confetti.ru\n"
                "📲 Telegram : @ConfettiAdmin"
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
            schedule=self.schedule,
            about=self.about,
            teachers=self.teachers,
            payment=self.payment,
            album=self.album,
            contacts=self.contacts,
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

    MAIN_MENU_BUTTON = "⬅️ Главное меню"
    REGISTRATION_BUTTON = "📝 Запись / Inscription"
    ADMIN_MENU_BUTTON = "🛠 Админ-панель"
    ADMIN_BACK_TO_USER_BUTTON = "⬅️ Пользовательское меню"
    ADMIN_BROADCAST_BUTTON = "📣 Рассылка"
    ADMIN_VIEW_APPLICATIONS_BUTTON = "📬 Заявки"
    ADMIN_EDIT_SCHEDULE_BUTTON = "🗓 Редактировать расписание"
    ADMIN_EDIT_ABOUT_BUTTON = "ℹ️ Редактировать информацию"
    ADMIN_EDIT_TEACHERS_BUTTON = "👩‍🏫 Редактировать преподавателей"
    ADMIN_EDIT_ALBUM_BUTTON = "📸 Редактировать фотоальбом"
    ADMIN_EDIT_CONTACTS_BUTTON = "📞 Редактировать контакты"
    ADMIN_EDIT_VOCABULARY_BUTTON = "📚 Редактировать словарь"
    ADMIN_CANCEL_BUTTON = "🚫 Отмена"

    MAIN_MENU_LAYOUT = (
        (REGISTRATION_BUTTON, "📅 Расписание / Horaires"),
        ("ℹ️ О студии / À propos de nous", "👩‍🏫 Преподаватели / Enseignants"),
        ("💳 Сообщить об оплате / Paiement", "📸 Фотоальбом / Album photo"),
        ("📞 Контакты / Contact", "📚 Полезные слова / Vocabulaire"),
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

    def build_application(self) -> Application:
        """Construct the PTB application."""

        builder = ApplicationBuilder().token(self.token)

        limiter = self._build_rate_limiter()
        if limiter is not None:
            builder = builder.rate_limiter(limiter)

        application = builder.build()
        self._register_handlers(application)
        return application

    def __post_init__(self) -> None:
        self.admin_chat_ids = _normalise_admin_chat_ids(self.admin_chat_ids)

    def build_profile(self, chat: Any) -> "UserProfile":
        """Return the appropriate profile for ``chat``."""

        chat_id = _coerce_chat_id_from_object(chat)
        if self.is_admin_chat(chat_id):
            return AdminProfile(chat_id=chat_id)
        return UserProfile(chat_id=chat_id)

    def is_admin_chat(self, chat: Any) -> bool:
        """Return ``True`` when ``chat`` belongs to an administrator."""

        try:
            chat_id = _coerce_chat_id_from_object(chat)
        except ValueError:
            return False
        return chat_id in self.admin_chat_ids

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

        application.add_handler(CommandHandler("start", self._start))
        application.add_handler(CommandHandler("menu", self._show_main_menu))
        application.add_handler(CommandHandler("admin", self._show_admin_menu))
        application.add_handler(conversation)
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self._handle_text))

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

    def _main_menu_markup_for(self, update: Update) -> ReplyKeyboardMarkup:
        return self._main_menu_markup(include_admin=self._is_admin_update(update))

    def _admin_menu_markup(self) -> ReplyKeyboardMarkup:
        keyboard = [
            [self.ADMIN_BACK_TO_USER_BUTTON, self.ADMIN_CANCEL_BUTTON],
            [self.ADMIN_BROADCAST_BUTTON, self.ADMIN_VIEW_APPLICATIONS_BUTTON],
            [self.ADMIN_EDIT_SCHEDULE_BUTTON],
            [self.ADMIN_EDIT_ABOUT_BUTTON],
            [self.ADMIN_EDIT_TEACHERS_BUTTON],
            [self.ADMIN_EDIT_ALBUM_BUTTON],
            [self.ADMIN_EDIT_CONTACTS_BUTTON],
            [self.ADMIN_EDIT_VOCABULARY_BUTTON],
        ]
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

    def _admin_cancel_markup(self) -> ReplyKeyboardMarkup:
        return ReplyKeyboardMarkup([[self.ADMIN_CANCEL_BUTTON]], resize_keyboard=True)

    def _is_admin_update(self, update: Update) -> bool:
        chat = update.effective_chat
        return bool(chat and self.is_admin_chat(chat))

    def _application_data(self, context: ContextTypes.DEFAULT_TYPE) -> dict[str, Any]:
        """Return application-level storage across PTB versions."""

        if hasattr(context, "application_data"):
            return context.application_data  # type: ignore[attr-defined]

        return context.application.bot_data

    def _remember_chat(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, data: dict[str, Any]
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
        }
        registrations = self._application_data(context).setdefault("registrations", [])
        if isinstance(registrations, list):
            registrations.append(record)
        else:
            self._application_data(context)["registrations"] = [record]

    async def _start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Send the greeting and display the main menu."""

        self._remember_chat(update, context)
        await self._send_greeting(update)

    async def _show_main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Show the menu without repeating the full greeting."""

        self._remember_chat(update, context)
        message = (
            "👉 Veuillez choisir une rubrique dans le menu ci-dessous.\n"
            "👉 Пожалуйста, выберите раздел в меню ниже."
        )
        if self._is_admin_update(update):
            message += (
                "\n\n🛠 Для управления ботом откройте «Админ-панель» в меню."
                "\n🛠 Pour administrer le bot, choisissez «Админ-панель»."
            )
        await self._reply(update, message, reply_markup=self._main_menu_markup_for(update))

    async def _show_admin_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._is_admin_update(update):
            await self._reply(
                update,
                "Эта панель доступна только администраторам.\n"
                "Ce panneau est réservé aux administrateurs.",
                reply_markup=self._main_menu_markup_for(update),
            )
            return
        self._remember_chat(update, context)
        message = (
            "Админ-панель открыта. Выберите действие ниже.\n"
            "Panneau d'administration ouvert — choisissez une action."
        )
        await self._reply(update, message, reply_markup=self._admin_menu_markup())

    async def _send_greeting(self, update: Update) -> None:
        greeting = (
            "🎉 🇫🇷 Bonjour et bienvenue dans la compagnie «Confetti» !\n"
            "🎉 🇷🇺 Здравствуйте и добро пожаловать в студию «Конфетти»!\n\n"
            "Nous adorons la France et le français — et nous sommes prêts à partager cet amour à chacun.\n\n"
            "Мы обожаем Францию и французский — и готовы делиться этой любовью с каждым.\n\n"
            "👉 Veuillez choisir une rubrique dans le menu ci-dessous.\n"
            "👉 Пожалуйста, выберите раздел в меню ниже."
        )
        if self._is_admin_update(update):
            greeting += (
                "\n\n🛠 У вас есть доступ к админ-панели — нажмите кнопку ниже, чтобы управлять контентом."
                "\n🛠 Vous pouvez gérer le contenu via le bouton «Админ-панель»."
            )
        await self._reply(update, greeting, reply_markup=self._main_menu_markup_for(update))

    async def _reply(
        self,
        update: Update,
        text: str,
        *,
        reply_markup: Optional[ReplyKeyboardMarkup | ReplyKeyboardRemove] = None,
    ) -> None:
        if update.message:
            await update.message.reply_text(text, reply_markup=reply_markup)
        elif update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.message.reply_text(text, reply_markup=reply_markup)

    # ------------------------------------------------------------------
    # Registration conversation

    async def _start_registration(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
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
        await self._send_registration_summary(update, context)
        await self._show_main_menu(update, context)
        return ConversationHandler.END

    async def _registration_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        context.user_data.pop("registration", None)
        await self._reply(
            update,
            "❌ Регистрация отменена.\n❌ L'inscription est annulée.",
            reply_markup=self._main_menu_markup_for(update),
        )
        return ConversationHandler.END

    async def _send_registration_summary(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        data = context.user_data.get("registration", {})
        summary = (
            "🇫🇷 Votre demande a été enregistrée !\n"
            "🇷🇺 Ваша заявка принята!\n\n"
            f"👦 Enfant : {data.get('child_name', '—')} ({data.get('class', '—')})\n"
            f"👤 Contact : {data.get('contact_person', '—')}\n"
            f"📱 Téléphone : {data.get('phone', '—')}\n"
            f"🕒 Heure : {data.get('time', '—')}\n"
            f"📚 Programme : {data.get('program', '—')}\n\n"
            "Nous vous contacterons prochainement.\n"
            "Мы свяжемся с вами в ближайшее время."
        )
        await self._reply(update, summary, reply_markup=self._main_menu_markup_for(update))
        self._store_registration(update, context, data)

    # ------------------------------------------------------------------
    # Menu handlers

    async def _handle_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.message:
            return

        self._remember_chat(update, context)

        text = (update.message.text or "").strip()
        if not text:
            return

        if text == self.MAIN_MENU_BUTTON:
            await self._show_main_menu(update, context)
            return

        profile = self.build_profile(update.effective_chat)
        pending = context.chat_data.get("pending_admin_action")

        if pending and profile.is_admin:
            if text == self.ADMIN_CANCEL_BUTTON:
                context.chat_data.pop("pending_admin_action", None)
                await self._reply(
                    update,
                    "Действие отменено.\nL'action est annulée.",
                    reply_markup=self._admin_menu_markup(),
                )
                return
            context.chat_data.pop("pending_admin_action", None)
            await self._dispatch_admin_action(update, context, pending, text)
            return

        if profile.is_admin:
            if text == self.ADMIN_MENU_BUTTON:
                await self._show_admin_menu(update, context)
                return
            if text == self.ADMIN_BACK_TO_USER_BUTTON:
                await self._show_main_menu(update, context)
                return
            if text == self.ADMIN_BROADCAST_BUTTON:
                context.chat_data["pending_admin_action"] = {"type": "broadcast"}
                await self._reply(
                    update,
                    "Введите текст для рассылки всем пользователям.\n"
                    "Envoyez le message à diffuser.",
                    reply_markup=self._admin_cancel_markup(),
                )
                return
            if text == self.ADMIN_VIEW_APPLICATIONS_BUTTON:
                await self._admin_show_registrations(update, context)
                return
            if text == self.ADMIN_EDIT_SCHEDULE_BUTTON:
                await self._prompt_admin_content_edit(
                    update,
                    context,
                    field="schedule",
                    instruction="Отправьте новый текст расписания (можно в несколько строк).",
                )
                return
            if text == self.ADMIN_EDIT_ABOUT_BUTTON:
                await self._prompt_admin_content_edit(
                    update,
                    context,
                    field="about",
                    instruction="Отправьте обновлённый текст раздела «О студии».",
                )
                return
            if text == self.ADMIN_EDIT_TEACHERS_BUTTON:
                await self._prompt_admin_content_edit(
                    update,
                    context,
                    field="teachers",
                    instruction="Вставьте полный текст раздела о преподавателях.",
                )
                return
            if text == self.ADMIN_EDIT_ALBUM_BUTTON:
                await self._prompt_admin_content_edit(
                    update,
                    context,
                    field="album",
                    instruction="Отправьте ссылку или описание фотоальбома.",
                )
                return
            if text == self.ADMIN_EDIT_CONTACTS_BUTTON:
                await self._prompt_admin_content_edit(
                    update,
                    context,
                    field="contacts",
                    instruction="Введите новый блок контактов.",
                )
                return
            if text == self.ADMIN_EDIT_VOCABULARY_BUTTON:
                await self._prompt_admin_vocabulary_edit(update, context)
                return

        await self._handle_menu_selection(update, context)

    async def _dispatch_admin_action(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        pending: Dict[str, Any],
        text: str,
    ) -> None:
        action_type = pending.get("type")
        if action_type == "broadcast":
            await self._admin_send_broadcast(update, context, text)
            return
        if action_type == "edit_content":
            field = pending.get("field")
            if isinstance(field, str):
                await self._admin_apply_content_update(update, context, field, text)
            else:
                await self._reply(
                    update,
                    "Не удалось определить редактируемый блок.\n"
                    "Impossible d'identifier la section à modifier.",
                    reply_markup=self._admin_menu_markup(),
                )
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
        current_value = getattr(content, field)
        message = (
            f"{instruction}\n"
            "\nТекущий текст:"
            f"\n{current_value}"
        )
        await self._reply(update, message, reply_markup=self._admin_cancel_markup())

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
        await self._reply(update, message, reply_markup=self._admin_cancel_markup())

    async def _admin_send_broadcast(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, message: str
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
                await context.bot.send_message(chat_id=chat_id, text=message)
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

    async def _admin_show_registrations(
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

        lines = ["Последние заявки (до 10):"]
        for index, record in enumerate(reversed(registrations[-10:]), start=1):
            child = record.get("child_name") or "—"
            klass = record.get("class") or "—"
            program = record.get("program") or "—"
            contact = record.get("contact_person") or "—"
            phone = record.get("phone") or "—"
            created = record.get("created_at") or "—"
            lines.append(
                f"{index}. {child} ({klass})\n"
                f"   Программа: {program}\n"
                f"   Контакт: {contact} | {phone}\n"
                f"   Время: {record.get('time') or '—'} | Добавлено: {created}"
            )
        message = "\n\n".join(lines)
        await self._reply(update, message, reply_markup=self._admin_menu_markup())

    async def _admin_apply_content_update(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, field: str, value: str
    ) -> None:
        content = self._get_content(context)
        if not hasattr(content, field):
            await self._reply(
                update,
                "Этот раздел нельзя редактировать.\nCette section ne peut pas être modifiée.",
                reply_markup=self._admin_menu_markup(),
            )
            return
        setattr(content, field, value)
        await self._reply(
            update,
            "Раздел обновлён!\nLa section a été mise à jour.",
            reply_markup=self._admin_menu_markup(),
        )

    async def _admin_apply_vocabulary_update(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE, payload: str
    ) -> bool:
        lines = [line.strip() for line in payload.splitlines() if line.strip()]
        if not lines:
            await self._reply(
                update,
                "Отправьте хотя бы одну строку с данными.\nVeuillez fournir au moins une entrée.",
                reply_markup=self._admin_cancel_markup(),
            )
            return False

        entries: list[dict[str, str]] = []
        for line in lines:
            parts = [part.strip() for part in line.split("|")]
            if len(parts) != 5:
                await self._reply(
                    update,
                    "Неверный формат. Используйте 5 частей через вертикальную черту.|\n"
                    "Format incorrect: 5 éléments séparés par |.",
                    reply_markup=self._admin_cancel_markup(),
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
            "💳 Сообщить об оплате / Paiement": self._send_payment_instructions,
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
                reply_markup=self._main_menu_markup_for(update),
            )
            return
        await handler(update, context)

    async def _send_schedule(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        content = self._get_content(context)
        await self._reply(update, content.schedule, reply_markup=self._main_menu_markup_for(update))

    async def _send_about(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        content = self._get_content(context)
        await self._reply(update, content.about, reply_markup=self._main_menu_markup_for(update))

    async def _send_teachers(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        content = self._get_content(context)
        await self._reply(update, content.teachers, reply_markup=self._main_menu_markup_for(update))

    async def _send_payment_instructions(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        content = self._get_content(context)
        await self._reply(update, content.payment, reply_markup=self._main_menu_markup_for(update))

    async def _send_album(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        content = self._get_content(context)
        await self._reply(update, content.album, reply_markup=self._main_menu_markup_for(update))

    async def _send_contacts(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        content = self._get_content(context)
        await self._reply(update, content.contacts, reply_markup=self._main_menu_markup_for(update))

    async def _send_vocabulary(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        content = self._get_content(context)
        if not content.vocabulary:
            await self._reply(
                update,
                "Список слов пока пуст. Добавьте варианты через админ-панель.\n"
                "La liste de vocabulaire est vide pour le moment.",
                reply_markup=self._main_menu_markup_for(update),
            )
            return
        entry = random.choice(content.vocabulary)
        text = (
            "🎁 Mot du jour / Слово дня :\n\n"
            f"🇫🇷 {entry.get('word', '—')} {entry.get('emoji', '')}\n"
            f"🇷🇺 {entry.get('translation', '—')}\n\n"
            f"💬 Exemple : {entry.get('example_fr', '—')} — {entry.get('example_ru', '—')}"
        )
        await self._reply(update, text, reply_markup=self._main_menu_markup_for(update))


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

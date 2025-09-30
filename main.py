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
import random
import re
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
class ConfettiTelegramBot:
    """Light-weight wrapper around the PTB application builder."""

    token: str
    admin_chat_ids: AdminChatIdsInput = ()

    REGISTRATION_PROGRAM = 1
    REGISTRATION_CHILD_NAME = 2
    REGISTRATION_CLASS = 3
    REGISTRATION_CONTACT_PERSON = 4
    REGISTRATION_PHONE = 5
    REGISTRATION_TIME = 6

    MAIN_MENU_BUTTON = "⬅️ Главное меню"
    REGISTRATION_BUTTON = "📝 Запись / Inscription"

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
        application.add_handler(conversation)
        application.add_handler(
            MessageHandler(
                filters.Regex(self._exact_match_regex(self.MAIN_MENU_BUTTON)),
                self._show_main_menu,
            )
        )
        application.add_handler(
            MessageHandler(filters.TEXT & ~filters.COMMAND, self._handle_menu_selection)
        )

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

    def _main_menu_markup(self) -> ReplyKeyboardMarkup:
        keyboard = [list(row) for row in self.MAIN_MENU_LAYOUT]
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

    async def _start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Send the greeting and display the main menu."""

        await self._send_greeting(update)

    async def _show_main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Show the menu without repeating the full greeting."""

        message = (
            "👉 Veuillez choisir une rubrique dans le menu ci-dessous.\n"
            "👉 Пожалуйста, выберите раздел в меню ниже."
        )
        await self._reply(update, message, reply_markup=self._main_menu_markup())

    async def _send_greeting(self, update: Update) -> None:
        greeting = (
            "🎉 🇫🇷 Bonjour et bienvenue dans la compagnie «Confetti» !\n"
            "🎉 🇷🇺 Здравствуйте и добро пожаловать в студию «Конфетти»!\n\n"
            "Nous adorons la France et le français — et nous sommes prêts à partager cet amour à chacun.\n\n"
            "Мы обожаем Францию и французский — и готовы делиться этой любовью с каждым.\n\n"
            "👉 Veuillez choisir une rubrique dans le menu ci-dessous.\n"
            "👉 Пожалуйста, выберите раздел в меню ниже."
        )
        await self._reply(update, greeting, reply_markup=self._main_menu_markup())

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
            reply_markup=self._main_menu_markup(),
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
        await self._reply(update, summary, reply_markup=self._main_menu_markup())

    # ------------------------------------------------------------------
    # Menu handlers

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
                reply_markup=self._main_menu_markup(),
            )
            return
        await handler(update, context)

    async def _send_schedule(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        text = (
            "🇫🇷 Voici nos horaires actuels :\n"
            "🇷🇺 Наше актуальное расписание:\n\n"
            "☀️ Matin / Утро : 10:00 – 12:00\n"
            "🌤 Après-midi / День : 14:00 – 16:00\n"
            "🌙 Soir / Вечер : 18:00 – 20:00"
        )
        await self._reply(update, text, reply_markup=self._main_menu_markup())

    async def _send_about(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        text = (
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
        await self._reply(update, text, reply_markup=self._main_menu_markup())

    async def _send_teachers(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        text = (
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
        await self._reply(update, text, reply_markup=self._main_menu_markup())

    async def _send_payment_instructions(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        text = (
            "🇫🇷 Veuillez envoyer une photo ou un reçu de paiement ici.\n"
            "🇷🇺 Пожалуйста, отправьте сюда фото или чек об оплате.\n\n"
            "📌 Après vérification, nous confirmerons votre inscription.\n"
            "📌 После проверки мы подтвердим вашу запись."
        )
        await self._reply(update, text, reply_markup=self._main_menu_markup())

    async def _send_album(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        text = (
            "🇫🇷 Regardez nos meilleurs moments 🎭\n"
            "🇷🇺 Посмотрите наши лучшие моменты 🎭\n\n"
            "👉 https://confetti.ru/album"
        )
        await self._reply(update, text, reply_markup=self._main_menu_markup())

    async def _send_contacts(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        text = (
            "📞 Téléphone : +7 (900) 000-00-00\n"
            "📧 Email : confetti@example.com\n"
            "🌐 Site / Сайт : https://confetti.ru\n"
            "📲 Telegram : @ConfettiAdmin"
        )
        await self._reply(update, text, reply_markup=self._main_menu_markup())

    async def _send_vocabulary(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        entry = random.choice(self.VOCABULARY)
        text = (
            "🎁 Mot du jour / Слово дня :\n\n"
            f"🇫🇷 {entry['word']} {entry['emoji']}\n"
            f"🇷🇺 {entry['translation']}\n\n"
            f"💬 Exemple : {entry['example_fr']} — {entry['example_ru']}"
        )
        await self._reply(update, text, reply_markup=self._main_menu_markup())


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
    bot = ConfettiTelegramBot(token="TOKEN_PLACEHOLDER")
    application = bot.build_application()
    # The original project keeps polling outside of the kata scope.  We expose
    # the configured application so that callers can decide how to run it.
    application.run_polling()


if __name__ == "__main__":  # pragma: no cover - module executable guard
    main()

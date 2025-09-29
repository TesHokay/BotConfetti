from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from telegram import Update
from telegram.ext import (
    AIORateLimiter,
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from bot_confetti.config import BotConfig
from bot_confetti.database import Database
from bot_confetti.keyboards import admin as admin_keyboard
from bot_confetti.keyboards import user as user_keyboard
from bot_confetti.messages import (
    ADMIN_PANEL_TITLE,
    BOOKING_CONFIRMATION,
    BOOKING_PROMPTS,
    BROADCAST_PROMPT,
    EDIT_PROMPT_TEMPLATE,
    MENU_LABELS,
    WELCOME_MESSAGE,
)
from bot_confetti.services.booking import BookingService
from bot_confetti.services.broadcast import BroadcastService
from bot_confetti.services.content import ContentService
from bot_confetti.utils.formatting import format_bookings

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


class ConfettiBot:
    def __init__(self, config: BotConfig) -> None:
        self.config = config
        self.database = Database(config.database_path)
        self.content_service = ContentService(self.database)
        self.booking_service = BookingService(self.database)
        self.broadcast_service = BroadcastService(self.database)
        self.application: Optional[Application] = None

    def build_application(self) -> Application:
        application = (
            ApplicationBuilder()
            .token(self.config.token)
            .rate_limiter(AIORateLimiter())
            .post_init(self._post_init)
            .build()
        )

        application.add_handler(CommandHandler("start", self.on_start))
        application.add_handler(CommandHandler("admin", self.show_admin_panel))
        application.add_handler(CommandHandler("cancel", self.cancel_flow))

        application.add_handler(CallbackQueryHandler(self.on_menu_callback, pattern=r"^menu:"))
        application.add_handler(CallbackQueryHandler(self.on_admin_callback, pattern=r"^admin:"))

        application.add_handler(MessageHandler(filters.PHOTO, self.on_photo_message))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.on_text_message))

        application.add_error_handler(self.on_error)

        self.application = application
        return application

    async def _post_init(self, application: Application) -> None:
        LOGGER.info("Confetti bot is ready. Registered handlers: %s", application.handlers)

    # Helpers -------------------------------------------------------------
    async def is_admin(self, update: Update) -> bool:
        user = update.effective_user
        return bool(user and user.id in self.config.admin_ids)

    def require_admin(self, update: Update) -> bool:
        if not update.effective_user or update.effective_user.id not in self.config.admin_ids:
            LOGGER.warning("Unauthorized access attempt: %s", update.effective_user)
            return False
        return True

    # Command handlers ----------------------------------------------------
    async def on_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.message:
            return
        user_id = await self.booking_service.ensure_user(update, context)
        context.user_data.clear()
        await update.message.reply_text(WELCOME_MESSAGE, reply_markup=user_keyboard.main_menu_keyboard())
        LOGGER.info("User %s opened the menu", user_id)

    async def show_admin_panel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.message or not self.require_admin(update):
            return
        await update.message.reply_text(
            ADMIN_PANEL_TITLE,
            reply_markup=admin_keyboard.admin_menu_keyboard(),
        )

    async def cancel_flow(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if update.message:
            await update.message.reply_text("Диалог отменён. / Conversation annulée.")
        context.user_data.clear()
        if update.effective_user:
            self.booking_service.cancel_booking(update.effective_user.id)

    # Callback handlers ---------------------------------------------------
    async def on_menu_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.callback_query:
            return
        query = update.callback_query
        await query.answer()

        _, action = query.data.split(":", maxsplit=1)
        if action == "book":
            await self.start_booking_flow(update, context)
            return

        text = self.content_service.get(action)
        await query.edit_message_text(text=text, reply_markup=user_keyboard.main_menu_keyboard())

    async def on_admin_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.callback_query or not self.require_admin(update):
            return
        query = update.callback_query
        await query.answer()

        data = query.data.split(":")
        action = data[1]

        if action == "broadcast":
            context.user_data["awaiting_broadcast"] = True
            await query.edit_message_text(BROADCAST_PROMPT, reply_markup=admin_keyboard.admin_menu_keyboard())
            return

        if action == "view_bookings":
            bookings = self.database.list_bookings()
            text = format_bookings(bookings)
            await query.edit_message_text(text, reply_markup=admin_keyboard.admin_menu_keyboard())
            return

        if action == "edit":
            section = data[2]
            context.user_data["awaiting_edit"] = section
            await query.edit_message_text(
                EDIT_PROMPT_TEMPLATE.format(section=MENU_LABELS.get(section, section)),
                reply_markup=admin_keyboard.admin_menu_keyboard(),
            )
            return

    # Booking flow --------------------------------------------------------
    async def start_booking_flow(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.callback_query:
            return
        await self.booking_service.ensure_user(update, context)
        user_id = update.effective_user.id if update.effective_user else None
        if user_id is None:
            return
        booking = self.booking_service.start_booking(user_id)
        context.user_data["booking_state"] = "full_name"
        await update.callback_query.edit_message_text(
            BOOKING_PROMPTS["full_name"], reply_markup=user_keyboard.main_menu_keyboard()
        )
        LOGGER.info("Started booking for user %s", booking.user_id)

    async def on_text_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.message:
            return
        user_id = update.effective_user.id if update.effective_user else None
        if user_id is None:
            return
        state = context.user_data.get("booking_state")
        if state:
            await self.handle_booking_state(state, update, context)
            return
        if context.user_data.get("awaiting_broadcast") and await self.is_admin(update):
            await self.handle_broadcast(update, context)
            return
        if (section := context.user_data.get("awaiting_edit")) and await self.is_admin(update):
            await self.handle_content_edit(section, update, context)
            return

    async def handle_booking_state(
        self, state: str, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        user_id = update.effective_user.id if update.effective_user else None
        if user_id is None:
            return
        booking = self.booking_service.get_booking(user_id)
        if not booking:
            context.user_data.pop("booking_state", None)
            await update.message.reply_text("Диалог не найден, начните заново через меню.")
            return

        text = update.message.text.strip()

        if state == "full_name":
            booking.full_name = text
            context.user_data["booking_state"] = "contact"
            await update.message.reply_text(BOOKING_PROMPTS["contact"])
            return

        if state == "contact":
            booking.contact = text
            context.user_data["booking_state"] = "preferred_date"
            await update.message.reply_text(BOOKING_PROMPTS["preferred_date"])
            return

        if state == "preferred_date":
            if not self._validate_date(text):
                await update.message.reply_text(
                    "Дата должна быть в формате ДД.ММ.ГГГГ. Попробуйте снова."
                )
                return
            booking.preferred_date = text
            context.user_data["booking_state"] = "notes"
            await update.message.reply_text(BOOKING_PROMPTS["notes"])
            return

        if state == "notes":
            booking.notes = None if text.lower() in {"-", "нет", "non", "no"} else text
            self.booking_service.finalise_booking(booking)
            context.user_data["booking_state"] = "payment"
            await update.message.reply_text(BOOKING_PROMPTS["payment"])
            return

        if state == "payment":
            await update.message.reply_text("Пожалуйста, отправьте скриншот оплаты в виде фото.")

    async def on_photo_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.message or not update.message.photo:
            return
        user_id = update.effective_user.id if update.effective_user else None
        if user_id is None:
            return
        if context.user_data.get("booking_state") != "payment":
            return
        booking = self.booking_service.get_booking(user_id)
        if not booking or booking.booking_id is None:
            await update.message.reply_text("Сначала заполните заявку, затем отправьте фото оплаты.")
            return
        photo = update.message.photo[-1]
        self.booking_service.save_payment(
            booking,
            file_id=photo.file_id,
            file_unique_id=getattr(photo, "file_unique_id", None),
        )
        self.booking_service.cancel_booking(user_id)
        context.user_data.clear()
        await update.message.reply_text(
            BOOKING_CONFIRMATION,
            reply_markup=user_keyboard.main_menu_keyboard(),
        )

    # Admin flows ---------------------------------------------------------
    async def handle_broadcast(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        context.user_data.pop("awaiting_broadcast", None)
        message = update.message
        sent = await self.broadcast_service.send_broadcast(message)
        await message.reply_text(f"Рассылка отправлена {sent} пользователям.")

    async def handle_content_edit(
        self, section: str, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        context.user_data.pop("awaiting_edit", None)
        text = update.message.text
        self.content_service.set(section, text)
        await update.message.reply_text("Раздел обновлён.")

    # Misc helpers --------------------------------------------------------
    def _validate_date(self, value: str) -> bool:
        try:
            datetime.strptime(value, "%d.%m.%Y")
            return True
        except ValueError:
            return False

    async def on_error(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        LOGGER.exception("Update %s caused error %s", update, context.error)


def main() -> None:
    config = BotConfig.load()
    bot = ConfettiBot(config)
    application = bot.build_application()
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        LOGGER.info("Bot stopped manually")

from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from bot_confetti import messages


def admin_menu_keyboard() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(messages.ADMIN_MENU_LABELS["broadcast"], callback_data="admin:broadcast")],
        [InlineKeyboardButton(messages.ADMIN_MENU_LABELS["view_bookings"], callback_data="admin:view_bookings")],
        [InlineKeyboardButton(messages.ADMIN_MENU_LABELS["edit_schedule"], callback_data="admin:edit:schedule")],
        [InlineKeyboardButton(messages.ADMIN_MENU_LABELS["edit_about"], callback_data="admin:edit:about")],
        [InlineKeyboardButton(messages.ADMIN_MENU_LABELS["edit_teachers"], callback_data="admin:edit:teachers")],
        [InlineKeyboardButton(messages.ADMIN_MENU_LABELS["edit_contacts"], callback_data="admin:edit:contacts")],
    ]
    return InlineKeyboardMarkup(buttons)

from __future__ import annotations

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from bot_confetti import messages


def main_menu_keyboard() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(messages.MENU_LABELS["about"], callback_data="menu:about")],
        [InlineKeyboardButton(messages.MENU_LABELS["teachers"], callback_data="menu:teachers")],
        [InlineKeyboardButton(messages.MENU_LABELS["schedule"], callback_data="menu:schedule")],
        [InlineKeyboardButton(messages.MENU_LABELS["book"], callback_data="menu:book")],
        [InlineKeyboardButton(messages.MENU_LABELS["contacts"], callback_data="menu:contacts")],
    ]
    return InlineKeyboardMarkup(buttons)

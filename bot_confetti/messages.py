from __future__ import annotations

WELCOME_MESSAGE = (
    "\U0001F388 Bonjour et здравствуйте!\n\n"
    "Bienvenue dans Confetti — студию французского языка.\n"
    "Nous sommes ravis de vous voir ici!\n\n"
    "Добро пожаловать в Confetti — студию французского языка.\n"
    "Мы рады вас видеть здесь!"
)

MENU_LABELS = {
    "about": "\U0001F4DA О студии / À propos",
    "teachers": "\U0001F9D1\u200d\U0001F3EB Преподаватели / Enseignants",
    "schedule": "\U0001F4C5 Расписание / Horaires",
    "book": "\U0001F58C\uFE0F Записаться / Réserver",
    "contacts": "\U0001F4E9 Контакты / Contacts",
}

ADMIN_PANEL_TITLE = "\U0001F9D1\u200d\U0001F3EB Панель администратора"

ADMIN_MENU_LABELS = {
    "broadcast": "\U0001F4E2 Рассылка",
    "view_bookings": "\U0001F4CB Заявки",
    "edit_schedule": "\U0001F4C5 Редактировать расписание",
    "edit_about": "\U0001F4DD Редактировать информацию",
    "edit_teachers": "\U0001F469\u200d\U0001F3EB Обновить преподавателей",
    "edit_contacts": "\U0001F4E9 Обновить контакты",
}

BOOKING_PROMPTS = {
    "full_name": "Напишите, пожалуйста, ваше полное имя / Indiquez votre nom complet",
    "contact": "Оставьте телефон или @username для связи / Donnez un téléphone ou un @username",
    "preferred_date": (
        "Укажите желаемую дату пробного урока (формат ДД.ММ.ГГГГ)\n"
        "Indiquez la date souhaitée (JJ.MM.AAAA)"
    ),
    "notes": (
        "Есть ли пожелания к уроку? Напишите их или отправьте — если нет.\n"
        "Avez-vous des souhaits? Écrivez-les ou envoyez — si non."
    ),
    "payment": (
        "Отправьте, пожалуйста, скриншот оплаты.\n"
        "Envoyez une capture d'écran du paiement, s'il vous plaît."
    ),
}

BOOKING_CONFIRMATION = (
    "\U0001F389 Merci!\n\n"
    "Мы получили вашу заявку и оплату.\n"
    "Администратор свяжется с вами в ближайшее время."
)

BROADCAST_PROMPT = (
    "Отправьте сообщение для рассылки всем студентам.\n"
    "Send the broadcast message (текст/фото/видео)."
)

EDIT_PROMPT_TEMPLATE = "Отправьте новый текст для раздела: {section}."

BOOKING_SUMMARY_ROW = "{date} — {name} — {contact} — {notes}"

DEFAULT_CONTENT = {
    "about": (
        "\U0001F388 Confetti — франкоязычная студия, где уроки проходят в формате праздника.\n"
        "\nNous proposons des programmes personnalisés для детей и взрослых."
    ),
    "teachers": (
        "\U0001F469\u200d\U0001F3EB Наша команда преподавателей — дипломированные специалисты,\n"
        "живущие во Франции и регулярно повышающие квалификацию."
    ),
    "schedule": (
        "\U0001F4C5 Актуальные группы:\n"
        "- Пн/Ср 19:00 — разговорный клуб B1\n"
        "- Сб 11:00 — детская группа A1\n"
        "\nPour réserver une place, laissez заявку."
    ),
    "contacts": (
        "\U0001F4DE +33 1 23 45 67 89\n"
        "Telegram: @confetti_fr\n"
        "Email: bonjour@confetti.fr"
    ),
}

__all__ = [
    "WELCOME_MESSAGE",
    "MENU_LABELS",
    "ADMIN_PANEL_TITLE",
    "ADMIN_MENU_LABELS",
    "BOOKING_PROMPTS",
    "BOOKING_CONFIRMATION",
    "BROADCAST_PROMPT",
    "EDIT_PROMPT_TEMPLATE",
    "BOOKING_SUMMARY_ROW",
    "DEFAULT_CONTENT",
]

from __future__ import annotations

from typing import Iterable

from bot_confetti.database import Booking
from bot_confetti.messages import BOOKING_SUMMARY_ROW


def format_bookings(bookings: Iterable[Booking]) -> str:
    rows = [
        BOOKING_SUMMARY_ROW.format(
            date=booking.preferred_date,
            name=booking.full_name,
            contact=booking.contact,
            notes=booking.additional_notes or "—",
        )
        for booking in bookings
    ]
    if not rows:
        return "Пока нет заявок / Pas encore de demandes."
    header = "\U0001F4C4 Заявки / Demandes:\n"
    return header + "\n".join(rows)


__all__ = ["format_bookings"]

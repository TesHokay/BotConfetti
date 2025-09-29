from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from telegram import Update
from telegram.ext import ContextTypes

from bot_confetti import messages
from bot_confetti.database import Database


@dataclass(slots=True)
class PendingBooking:
    user_id: int
    full_name: Optional[str] = None
    contact: Optional[str] = None
    preferred_date: Optional[str] = None
    notes: Optional[str] = None
    booking_id: Optional[int] = None


class BookingService:
    def __init__(self, database: Database) -> None:
        self.database = database
        self._pending: dict[int, PendingBooking] = {}

    def start_booking(self, user_id: int) -> PendingBooking:
        booking = PendingBooking(user_id=user_id)
        self._pending[user_id] = booking
        return booking

    def get_booking(self, user_id: int) -> Optional[PendingBooking]:
        return self._pending.get(user_id)

    def cancel_booking(self, user_id: int) -> None:
        self._pending.pop(user_id, None)

    async def ensure_user(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        assert update.effective_user is not None
        return self.database.upsert_user(
            telegram_id=update.effective_user.id,
            username=update.effective_user.username,
            first_name=update.effective_user.first_name,
            last_name=update.effective_user.last_name,
            language_code=update.effective_user.language_code,
        )

    def finalise_booking(self, booking: PendingBooking) -> int:
        assert booking.full_name and booking.contact and booking.preferred_date
        booking_id = self.database.create_booking(
            user_id=booking.user_id,
            full_name=booking.full_name,
            contact=booking.contact,
            preferred_date=booking.preferred_date,
            additional_notes=booking.notes,
        )
        booking.booking_id = booking_id
        return booking_id

    def save_payment(self, booking: PendingBooking, *, file_id: str, file_unique_id: Optional[str]) -> int:
        if booking.booking_id is None:
            raise ValueError("Booking must be finalised before attaching payment")
        return self.database.save_payment(
            booking.booking_id, file_id=file_id, file_unique_id=file_unique_id
        )


__all__ = ["BookingService", "PendingBooking"]

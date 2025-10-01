from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union


@dataclass(frozen=True)
class Chat:
    """Simplified representation of a Telegram chat."""

    id: int


@dataclass(frozen=True)
class User:
    """Simplified representation of a Telegram user."""

    id: int


@dataclass(frozen=True)
class Update:
    """Simplified representation of a Telegram update."""

    effective_chat: Optional[Chat]
    effective_user: Optional[User]


ChatLike = Union[Chat, User, int, str, None]


@dataclass(frozen=True)
class Booking:
    """Information about a single scheduled activity for a user."""

    phone: str
    child_name: str
    class_name: str
    activity: str
    time: datetime
    created_at: datetime


class ConfettiTelegramBot:
    """Minimal bot implementation with administrator helpers.

    The real project contains a substantially larger code-base, but for the
    purposes of these kata-style exercises we only need the pieces that deal
    with administrator resolution and menu selection.
    """

    def __init__(self, admin_ids: Iterable[ChatLike]):
        self._admin_ids: Set[int] = set()
        for identifier in admin_ids:
            coerced = self._coerce_chat_id_from_object(identifier)
            if coerced is not None:
                self._admin_ids.add(coerced)
        self._user_bookings: Dict[int, List[Booking]] = {}

    # ------------------------------------------------------------------
    # Identifier coercion utilities
    # ------------------------------------------------------------------
    def _coerce_chat_id_from_object(self, obj: ChatLike) -> Optional[int]:
        """Normalise chat and user like objects into an integer identifier."""

        if obj is None:
            return None

        # ``bool`` is an ``int`` subclass, make sure we do not silently coerce
        # the values ``True``/``False`` into ``1``/``0``.
        if isinstance(obj, bool):
            return None

        if isinstance(obj, int):
            return obj

        if isinstance(obj, str):
            value = obj.strip()
            if not value:
                return None
            try:
                return int(value)
            except ValueError:
                return None

        potential_identifier = getattr(obj, "id", None)
        if potential_identifier is None:
            return None

        return self._coerce_chat_id_from_object(potential_identifier)

    # ------------------------------------------------------------------
    # Administrator handling helpers
    # ------------------------------------------------------------------
    def is_admin_chat(self, entity: ChatLike) -> bool:
        """Return ``True`` when the supplied chat/user resolves to an admin.

        Historically this helper only accepted chat objects, but tests exercise
        the behaviour with both chats *and* users.  The method therefore
        performs the identifier coercion and checks the resulting integer
        against the stored admin identifiers.
        """

        coerced = self._coerce_chat_id_from_object(entity)
        if coerced is None:
            return False
        return coerced in self._admin_ids

    def _is_admin_update(self, update: Update) -> bool:
        """Determine whether an update originates from an administrator."""

        if self.is_admin_chat(update.effective_chat):
            return True
        return self.is_admin_chat(update.effective_user)

    # ------------------------------------------------------------------
    # Menu helpers
    # ------------------------------------------------------------------
    def _admin_keyboard(self) -> Sequence[str]:
        return ("admin", "dashboard", "broadcast")

    def _user_keyboard(self) -> Sequence[str]:
        return ("profile", "help")

    def _main_menu_markup_for(self, actor: ChatLike) -> Sequence[str]:
        if self.is_admin_chat(actor):
            return self._admin_keyboard()
        return self._user_keyboard()

    def build_profile(self, update: Update) -> dict:
        is_admin = self._is_admin_update(update)
        preferred_actor: ChatLike
        if self.is_admin_chat(update.effective_user):
            preferred_actor = update.effective_user
        else:
            preferred_actor = update.effective_chat

        return {
            "is_admin": is_admin,
            "keyboard": self._main_menu_markup_for(preferred_actor),
        }

    def _show_admin_menu(self, update: Update) -> Sequence[str]:
        if not self._is_admin_update(update):
            return ()
        if self.is_admin_chat(update.effective_user):
            return self._main_menu_markup_for(update.effective_user)
        return self._main_menu_markup_for(update.effective_chat)

    # ------------------------------------------------------------------
    # Broadcast helpers
    # ------------------------------------------------------------------
    def _broadcast_target_ids(self, *entities: ChatLike) -> Set[int]:
        targets: Set[int] = set()
        for entity in entities:
            if not entity:
                continue
            coerced = self._coerce_chat_id_from_object(entity)
            if coerced is None:
                continue
            if self.is_admin_chat(coerced):
                targets.add(coerced)
        return targets

    def broadcast_to_admins(self, update: Update) -> Set[int]:
        """Return the identifiers that would receive a broadcast."""

        return self._broadcast_target_ids(
            update.effective_user,
            update.effective_chat,
        )

    # ------------------------------------------------------------------
    # Booking management
    # ------------------------------------------------------------------
    def _resolve_user_id(self, user: User) -> Optional[int]:
        return self._coerce_chat_id_from_object(user)

    def _bookings_for_user(self, user: User) -> List[Booking]:
        user_id = self._resolve_user_id(user)
        if user_id is None:
            raise ValueError("Cannot manage bookings without a valid user identifier")
        return self._user_bookings.setdefault(user_id, [])

    def booking_time_prompt(self, user: User) -> str:
        """Return the prompt presented when choosing a booking time."""

        previous_time = self._last_booking_time(user)
        if previous_time is None:
            return "Пожалуйста, выберите время занятия."
        formatted = previous_time.strftime("%d.%m.%Y %H:%M")
        return (
            "Хотите записаться по тому же времени или другое? "
            f"Предыдущее время: {formatted}."
        )

    def _last_booking_time(self, user: User) -> Optional[datetime]:
        bookings = self._bookings_for_user(user)
        if not bookings:
            return None
        return bookings[-1].time

    def add_booking(
        self,
        user: User,
        *,
        phone: str,
        child_name: str,
        class_name: str,
        activity: str,
        time: Optional[datetime] = None,
        reuse_previous_time: bool = False,
        created_at: Optional[datetime] = None,
    ) -> Booking:
        """Record a booking for the user, optionally reusing the previous time."""

        bookings = self._bookings_for_user(user)
        if reuse_previous_time:
            time_to_use = self._last_booking_time(user)
            if time_to_use is None:
                raise ValueError("No previous booking time available to reuse")
        else:
            if time is None:
                raise ValueError("Booking time must be provided when not reusing")
            time_to_use = time

        now = created_at or datetime.now(UTC)
        booking = Booking(
            phone=phone,
            child_name=child_name,
            class_name=class_name,
            activity=activity,
            time=time_to_use,
            created_at=now,
        )
        bookings.append(booking)
        return booking

    def list_bookings(self, user: User) -> Tuple[Booking, ...]:
        """Return a tuple of the user's current bookings."""

        bookings = tuple(self._bookings_for_user(user))
        return bookings

    def cancel_booking(self, user: User, index: int) -> Booking:
        """Remove a booking by its index for the given user."""

        bookings = self._bookings_for_user(user)
        if index < 0 or index >= len(bookings):
            raise IndexError("Invalid booking selection")
        return bookings.pop(index)

    def cleanup_expired_bookings(self, *, now: Optional[datetime] = None) -> None:
        """Remove bookings that are older than seven days across all users."""

        reference_time = now or datetime.now(UTC)
        cutoff = reference_time - timedelta(days=7)
        for user_id, bookings in list(self._user_bookings.items()):
            remaining = [booking for booking in bookings if booking.created_at >= cutoff]
            if remaining:
                self._user_bookings[user_id] = remaining
            else:
                self._user_bookings.pop(user_id, None)

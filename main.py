"""Entrypoint for the Confetti Telegram bot.

This module contains only a very small portion of the original project that is
required for the automated tests that accompany this kata.  The real project
uses :class:`telegram.ext.AIORateLimiter` which is part of an optional extra
(`python-telegram-bot[rate-limiter]`).  The optional dependency is not
available in the execution environment of the kata and therefore the original
implementation crashed on start-up.

To make the bot runnable everywhere we attempt to instantiate
``AIORateLimiter`` inside :func:`ConfettiTelegramBot._build_rate_limiter`.  If
the optional extra is missing the class is still importable but its
constructor raises a :class:`RuntimeError`.  We catch the exception and log a
warning which allows the bot to start without the rate limiter.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, Optional, Union

from telegram.ext import Application, ApplicationBuilder

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

    def build_application(self) -> Application:
        """Construct the PTB application.

        The original project attaches a rate limiter to the application
        builder.  Because the optional dependency that implements the rate
        limiter is not always available, we configure it only when it can be
        instantiated successfully.  This mirrors the behaviour of the real
        project closely enough for the exercises that accompany this kata.
        """

        builder = ApplicationBuilder().token(self.token)

        limiter = self._build_rate_limiter()
        if limiter is not None:
            builder = builder.rate_limiter(limiter)

        return builder.build()

    def __post_init__(self) -> None:
        self.admin_chat_ids = _normalise_admin_chat_ids(self.admin_chat_ids)

    def build_profile(self, chat: Any) -> "UserProfile":
        """Return the appropriate profile for ``chat``.

        A chat is considered administrative when its ID is listed in
        :attr:`admin_chat_ids`.  The helper accepts either chat objects that
        expose an ``id`` attribute (like :class:`telegram.Chat`) or raw chat
        identifiers.
        """

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
        """Return an ``AIORateLimiter`` instance when possible.

        ``AIORateLimiter`` can be imported without the optional dependencies,
        but trying to instantiate it raises a ``RuntimeError``.  Catch the
        exception and continue without a limiter so that the application still
        starts.
        """

        if AIORateLimiter is None:
            LOGGER.warning(
                "python-telegram-bot was installed without the optional rate "
                "limiter extras. The bot will run without a rate limiter."
            )
            return None

        try:
            return AIORateLimiter()
        except RuntimeError as exc:  # pragma: no cover - depends on installation
            LOGGER.warning(
                "Failed to initialise the AIORateLimiter: %s. Running without "
                "a rate limiter.",
                exc,
            )
            return None


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
    bot.build_application()


if __name__ == "__main__":  # pragma: no cover - module executable guard
    main()


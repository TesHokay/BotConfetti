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
from dataclasses import dataclass
from typing import Optional

from telegram.ext import Application, ApplicationBuilder

try:  # pragma: no cover - import error path depends on the environment
    from telegram.ext import AIORateLimiter
except ImportError:  # pragma: no cover - see comment above
    AIORateLimiter = None  # type: ignore[assignment]


LOGGER = logging.getLogger(__name__)


@dataclass
class ConfettiTelegramBot:
    """Light-weight wrapper around the PTB application builder."""

    token: str

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


def main() -> None:  # pragma: no cover - thin wrapper
    """Entry point used by the console script in the original project."""

    logging.basicConfig(level=logging.INFO)
    bot = ConfettiTelegramBot(token="TOKEN_PLACEHOLDER")
    bot.build_application()


if __name__ == "__main__":  # pragma: no cover - module executable guard
    main()


from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

from dotenv import load_dotenv


@dataclass(slots=True)
class BotConfig:
    """Configuration container for the Confetti tutoring bot."""

    token: str
    admin_ids: List[int] = field(default_factory=list)
    database_path: Path = field(default=Path("data/confetti.sqlite"))

    @classmethod
    def load(cls, env_path: str | os.PathLike[str] | None = ".env") -> "BotConfig":
        """Load configuration from environment variables."""
        if env_path is not None:
            load_dotenv(env_path)

        raw_admins = os.getenv("BOT_ADMIN_IDS", "")
        admin_ids = [
            int(value)
            for chunk in raw_admins.split(",")
            if (value := chunk.strip()).isdigit()
        ]

        token = os.getenv("BOT_TOKEN")
        if not token:
            raise RuntimeError(
                "BOT_TOKEN is not defined. Please add it to your .env file before running the bot."
            )

        database_path = Path(os.getenv("BOT_DATABASE", "data/confetti.sqlite")).expanduser()
        return cls(token=token, admin_ids=admin_ids, database_path=database_path)


__all__ = ["BotConfig"]

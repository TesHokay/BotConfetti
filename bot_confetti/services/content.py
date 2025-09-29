from __future__ import annotations

from bot_confetti.database import Database
from bot_confetti.messages import DEFAULT_CONTENT


class ContentService:
    def __init__(self, database: Database) -> None:
        self.database = database
        self._ensure_defaults()

    def _ensure_defaults(self) -> None:
        for key, value in DEFAULT_CONTENT.items():
            if not self.database.get_content(key):
                self.database.set_content(key, value)

    def get(self, key: str) -> str:
        return self.database.get_content(key, DEFAULT_CONTENT.get(key, ""))

    def set(self, key: str, value: str) -> None:
        self.database.set_content(key, value)


__all__ = ["ContentService"]

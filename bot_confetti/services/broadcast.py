from __future__ import annotations

from telegram import Message

from bot_confetti.database import Database


class BroadcastService:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def send_broadcast(self, message: Message) -> int:
        """Send a broadcast by copying the original message to all contacts."""
        sent = 0
        for telegram_id in self.database.get_user_ids():
            try:
                await message.copy(chat_id=telegram_id)
                sent += 1
            except Exception:  # pragma: no cover - logging handled by application
                continue
        return sent


__all__ = ["BroadcastService"]

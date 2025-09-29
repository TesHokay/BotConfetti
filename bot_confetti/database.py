from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Iterator, Optional


@dataclass(slots=True)
class Booking:
    booking_id: int
    user_id: int
    full_name: str
    contact: str
    preferred_date: str
    additional_notes: Optional[str]
    created_at: datetime


@dataclass(slots=True)
class Payment:
    payment_id: int
    booking_id: int
    file_id: str
    file_unique_id: Optional[str]
    created_at: datetime


class Database:
    """Simple SQLite wrapper to keep the project modular."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialise()

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _initialise(self) -> None:
        with self._connection() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    telegram_id INTEGER UNIQUE NOT NULL,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    language_code TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS bookings (
                    booking_id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                    full_name TEXT NOT NULL,
                    contact TEXT NOT NULL,
                    preferred_date TEXT NOT NULL,
                    additional_notes TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS payments (
                    payment_id INTEGER PRIMARY KEY,
                    booking_id INTEGER NOT NULL REFERENCES bookings(booking_id) ON DELETE CASCADE,
                    file_id TEXT NOT NULL,
                    file_unique_id TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS content (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )

    # User helpers ---------------------------------------------------------
    def upsert_user(
        self,
        telegram_id: int,
        username: Optional[str],
        first_name: Optional[str],
        last_name: Optional[str],
        language_code: Optional[str],
    ) -> int:
        with self._connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO users (telegram_id, username, first_name, last_name, language_code)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(telegram_id) DO UPDATE SET
                    username = excluded.username,
                    first_name = excluded.first_name,
                    last_name = excluded.last_name,
                    language_code = excluded.language_code
                RETURNING user_id
                """,
                (telegram_id, username, first_name, last_name, language_code),
            )
            row = cursor.fetchone()
            return int(row[0])

    def get_user_ids(self) -> Iterable[int]:
        with self._connection() as conn:
            rows = conn.execute("SELECT telegram_id FROM users").fetchall()
            return [int(row[0]) for row in rows]

    # Content helpers -----------------------------------------------------
    def get_content(self, key: str, default: str = "") -> str:
        with self._connection() as conn:
            row = conn.execute("SELECT value FROM content WHERE key = ?", (key,)).fetchone()
            if row:
                return str(row[0])
            return default

    def set_content(self, key: str, value: str) -> None:
        with self._connection() as conn:
            conn.execute(
                "INSERT INTO content(key, value) VALUES(?, ?)\n                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

    # Booking helpers -----------------------------------------------------
    def create_booking(
        self,
        user_id: int,
        full_name: str,
        contact: str,
        preferred_date: str,
        additional_notes: Optional[str],
    ) -> int:
        with self._connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO bookings (user_id, full_name, contact, preferred_date, additional_notes)
                VALUES (?, ?, ?, ?, ?)
                RETURNING booking_id
                """,
                (user_id, full_name, contact, preferred_date, additional_notes),
            )
            row = cursor.fetchone()
            return int(row[0])

    def save_payment(
        self, booking_id: int, *, file_id: str, file_unique_id: Optional[str]
    ) -> int:
        with self._connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO payments (booking_id, file_id, file_unique_id)
                VALUES (?, ?, ?)
                RETURNING payment_id
                """,
                (booking_id, file_id, file_unique_id),
            )
            row = cursor.fetchone()
            return int(row[0])

    def list_bookings(self) -> Iterable[Booking]:
        with self._connection() as conn:
            rows = conn.execute(
                """
                SELECT booking_id, user_id, full_name, contact, preferred_date, additional_notes, created_at
                FROM bookings
                ORDER BY created_at DESC
                """
            ).fetchall()

        return [
            Booking(
                booking_id=int(row["booking_id"]),
                user_id=int(row["user_id"]),
                full_name=str(row["full_name"]),
                contact=str(row["contact"]),
                preferred_date=str(row["preferred_date"]),
                additional_notes=row["additional_notes"],
                created_at=datetime.fromisoformat(row["created_at"]),
            )
            for row in rows
        ]

    def list_payments(self) -> Iterable[Payment]:
        with self._connection() as conn:
            rows = conn.execute(
                """
                SELECT payment_id, booking_id, file_id, file_unique_id, created_at
                FROM payments
                ORDER BY created_at DESC
                """
            ).fetchall()

        return [
            Payment(
                payment_id=int(row["payment_id"]),
                booking_id=int(row["booking_id"]),
                file_id=str(row["file_id"]),
                file_unique_id=row["file_unique_id"],
                created_at=datetime.fromisoformat(row["created_at"]),
            )
            for row in rows
        ]


__all__ = ["Database", "Booking", "Payment"]

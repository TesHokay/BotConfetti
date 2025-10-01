from datetime import datetime, timedelta
from pathlib import Path
import importlib.util
import sys


def load_main_module():
    module_path = Path(__file__).resolve().parent.parent / "main.py"
    spec = importlib.util.spec_from_file_location("main", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


main = load_main_module()
Chat = main.Chat
ConfettiTelegramBot = main.ConfettiTelegramBot
Update = main.Update
User = main.User
Booking = main.Booking


def test_bookings_are_stored_per_user():
    bot = ConfettiTelegramBot(admin_ids=())
    user_one = User(id=1)
    user_two = User(id=2)
    when = datetime(2024, 1, 1, 10, 0)

    bot.add_booking(
        user_one,
        phone="+1",
        child_name="Alice",
        class_name="3A",
        activity="Drawing",
        time=when,
    )
    bot.add_booking(
        user_two,
        phone="+2",
        child_name="Bob",
        class_name="4B",
        activity="Math",
        time=when,
    )

    bookings_one = bot.list_bookings(user_one)
    bookings_two = bot.list_bookings(user_two)

    assert len(bookings_one) == 1
    assert len(bookings_two) == 1
    assert bookings_one[0].phone == "+1"
    assert bookings_two[0].phone == "+2"


def test_booking_time_prompt_changes_after_first_booking():
    bot = ConfettiTelegramBot(admin_ids=())
    user = User(id=1)
    prompt = bot.booking_time_prompt(user)
    assert prompt == "Пожалуйста, выберите время занятия."

    when = datetime(2024, 1, 1, 10, 0)
    bot.add_booking(
        user,
        phone="+1",
        child_name="Alice",
        class_name="3A",
        activity="Drawing",
        time=when,
    )

    prompt = bot.booking_time_prompt(user)
    assert "Хотите записаться по тому же времени или другое?" in prompt
    assert "01.01.2024 10:00" in prompt


def test_can_reuse_previous_booking_time():
    bot = ConfettiTelegramBot(admin_ids=())
    user = User(id=1)
    initial_time = datetime(2024, 1, 1, 10, 0)
    reused_time = datetime(2024, 1, 2, 12, 0)

    bot.add_booking(
        user,
        phone="+1",
        child_name="Alice",
        class_name="3A",
        activity="Drawing",
        time=initial_time,
    )

    booking = bot.add_booking(
        user,
        phone="+1",
        child_name="Alice",
        class_name="3A",
        activity="Painting",
        time=reused_time,
        reuse_previous_time=True,
    )

    assert booking.time == initial_time


def test_cancel_booking_by_index():
    bot = ConfettiTelegramBot(admin_ids=())
    user = User(id=1)
    times = [
        datetime(2024, 1, 1, 10, 0),
        datetime(2024, 1, 2, 11, 0),
    ]
    for idx, when in enumerate(times):
        bot.add_booking(
            user,
            phone=f"+{idx}",
            child_name="Alice",
            class_name="3A",
            activity=f"Activity {idx}",
            time=when,
        )

    removed = bot.cancel_booking(user, 0)
    remaining = bot.list_bookings(user)

    assert removed.activity == "Activity 0"
    assert len(remaining) == 1
    assert remaining[0].activity == "Activity 1"


def test_cleanup_removes_old_bookings():
    bot = ConfettiTelegramBot(admin_ids=())
    user = User(id=1)
    now = datetime(2024, 1, 8, 12, 0)
    recent_booking_time = datetime(2024, 1, 7, 9, 0)

    bot.add_booking(
        user,
        phone="+1",
        child_name="Alice",
        class_name="3A",
        activity="Recent",
        time=recent_booking_time,
        created_at=now - timedelta(days=2),
    )

    bot.add_booking(
        user,
        phone="+2",
        child_name="Alice",
        class_name="3A",
        activity="Old",
        time=datetime(2023, 12, 20, 9, 0),
        created_at=now - timedelta(days=8),
    )

    bot.cleanup_expired_bookings(now=now)
    remaining = bot.list_bookings(user)

    assert len(remaining) == 1
    assert remaining[0].activity == "Recent"

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


def make_bot(admin_ids):
    return ConfettiTelegramBot(admin_ids)


def test_admin_keyboard_available_for_admin_user_in_regular_chat():
    bot = make_bot({1})
    chat = Chat(id=999)
    user = User(id=1)
    update = Update(effective_chat=chat, effective_user=user)

    profile = bot.build_profile(update)

    assert profile["is_admin"] is True
    assert profile["keyboard"] == bot._admin_keyboard()


def test_regular_user_does_not_receive_admin_keyboard():
    bot = make_bot({1})
    chat = Chat(id=999)
    user = User(id=2)
    update = Update(effective_chat=chat, effective_user=user)

    profile = bot.build_profile(update)

    assert profile["is_admin"] is False
    assert profile["keyboard"] == bot._user_keyboard()


def test_broadcast_targets_include_admin_user_even_in_regular_chat():
    bot = make_bot({1})
    chat = Chat(id=999)
    user = User(id=1)
    update = Update(effective_chat=chat, effective_user=user)

    targets = bot.broadcast_to_admins(update)

    assert targets == {1}


def test_is_admin_update_detects_admin_user_even_if_chat_not_admin():
    bot = make_bot({1})
    chat = Chat(id=999)
    user = User(id=1)
    update = Update(effective_chat=chat, effective_user=user)

    assert bot._is_admin_update(update) is True


def test_is_admin_update_handles_missing_user():
    bot = make_bot({1})
    chat = Chat(id=1)
    update = Update(effective_chat=chat, effective_user=None)

    assert bot._is_admin_update(update) is True

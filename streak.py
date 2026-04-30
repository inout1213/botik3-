"""
streak.py — система стриков и наград BZH Academy
Хранит данные в JSON-файле (простое решение без базы данных).
Режим: пользователь пишет отчёт текстом → админ подтверждает.
"""

import json
import os
from datetime import date, timedelta

STREAK_FILE = "streaks.json"
STREAK_GOAL = 5  # дней для получения награды

# Состояния ожидания отчёта
WAITING_REPORT = {}  # user_id: task_text


def _load() -> dict:
    if not os.path.exists(STREAK_FILE):
        return {}
    with open(STREAK_FILE, "r") as f:
        return json.load(f)


def _save(data: dict):
    with open(STREAK_FILE, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_user(user_id: int) -> dict:
    data = _load()
    uid = str(user_id)
    if uid not in data:
        data[uid] = {
            "streak": 0,
            "last_date": None,
            "total_completed": 0,
            "rewards_claimed": 0,
            "pending_approval": False,
        }
        _save(data)
    return data[uid]


def set_pending(user_id: int):
    """Отмечает что отчёт отправлен и ждёт подтверждения."""
    data = _load()
    uid = str(user_id)
    if uid in data:
        data[uid]["pending_approval"] = True
        _save(data)


def approve_checkin(user_id: int) -> dict:
    """Подтверждает стрик после проверки отчёта админом."""
    data = _load()
    uid = str(user_id)
    today = str(date.today())
    yesterday = str(date.today() - timedelta(days=1))

    user = data.get(uid, {
        "streak": 0,
        "last_date": None,
        "total_completed": 0,
        "rewards_claimed": 0,
        "pending_approval": False,
    })

    if user["last_date"] == today:
        return {"already_done": True, "streak": user["streak"], "reward": False}

    streak_broken = False
    if user["last_date"] == yesterday:
        user["streak"] += 1
    else:
        if user["streak"] > 0:
            streak_broken = True
        user["streak"] = 1

    user["last_date"] = today
    user["total_completed"] += 1
    user["pending_approval"] = False

    reward = False
    if user["streak"] % STREAK_GOAL == 0:
        reward = True
        user["rewards_claimed"] += 1

    data[uid] = user
    _save(data)

    return {
        "already_done": False,
        "streak": user["streak"],
        "reward": reward,
        "streak_broken": streak_broken,
    }


def reject_checkin(user_id: int):
    """Отклоняет отчёт — стрик не засчитывается."""
    data = _load()
    uid = str(user_id)
    if uid in data:
        data[uid]["pending_approval"] = False
        _save(data)


def get_streak(user_id: int) -> int:
    return get_user(user_id)["streak"]


def progress_bar(streak: int, goal: int = STREAK_GOAL) -> str:
    filled = streak % goal if streak % goal != 0 else (goal if streak > 0 else 0)
    empty = goal - filled
    return "🔥" * filled + "⬜" * empty


def _day_ru(n: int) -> str:
    if n == 1:
        return "день"
    elif 2 <= n <= 4:
        return "дня"
    return "дней"


def _day_uk(n: int) -> str:
    if n == 1:
        return "день"
    elif 2 <= n <= 4:
        return "дні"
    return "днів"


def streak_status(user_id: int, lang: str = "ru") -> str:
    user = get_user(user_id)
    streak = user["streak"]
    goal = STREAK_GOAL
    remaining = goal - (streak % goal) if streak % goal != 0 else 0
    bar = progress_bar(streak)
    rewards = user["rewards_claimed"]
    pending = user.get("pending_approval", False)

    if lang == "uk":
        if pending:
            return (
                f"🔥 *Твій стрік: {streak} {_day_uk(streak)}*\n\n"
                f"{bar}\n\n"
                f"⏳ _Звіт надіслано — очікуй підтвердження._"
            )

        if streak == 0:
            return (
                f"🔥 *Твій стрік: 0 днів*\n\n"
                f"{bar}\n\n"
                f"Виконай сьогоднішнє завдання і надішли звіт!\n"
                f"_{goal} днів поспіль = безкоштовний воркбук на вибір_ 🎁"
            )

        if remaining == 0:
            return (
                f"🔥 *Твій стрік: {streak} {_day_uk(streak)}*\n\n"
                f"{'🔥' * goal}\n\n"
                f"🎁 Ти заробив нагороду! Обери безкоштовний воркбук.\n"
                f"_Всього нагород отримано: {rewards}_"
            )

        return (
            f"🔥 *Твій стрік: {streak} {_day_uk(streak)}*\n\n"
            f"{bar}\n\n"
            f"До безкоштовного воркбуку: *{remaining} {_day_uk(remaining)}* 🎁\n"
            f"_Не переривай стрік — повертайся завтра!_"
        )

    # Русский
    if pending:
        return (
            f"🔥 *Твой стрик: {streak} {_day_ru(streak)}*\n\n"
            f"{bar}\n\n"
            f"⏳ _Отчёт отправлен — ожидай подтверждения._"
        )

    if streak == 0:
        return (
            f"🔥 *Твой стрик: 0 дней*\n\n"
            f"{bar}\n\n"
            f"Выполни сегодняшнее задание и отправь отчёт!\n"
            f"_{goal} дней подряд = бесплатный воркбук на выбор_ 🎁"
        )

    if remaining == 0:
        return (
            f"🔥 *Твой стрик: {streak} {_day_ru(streak)}*\n\n"
            f"{'🔥' * goal}\n\n"
            f"🎁 Ты заработал награду! Выбери бесплатный воркбук.\n"
            f"_Всего наград получено: {rewards}_"
        )

    return (
        f"🔥 *Твой стрик: {streak} {_day_ru(streak)}*\n\n"
        f"{bar}\n\n"
        f"До бесплатного воркбука: *{remaining} {_day_ru(remaining)}* 🎁\n"
        f"_Не прерывай стрик — возвращайся завтра!_"
    )

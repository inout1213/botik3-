"""
database.py — работа с PostgreSQL для BZH Academy
"""

import asyncpg
import os
from datetime import date, timedelta, datetime

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:OzdENPEKrIhiVmialxSSPZKWFfcjhJfr@postgres.railway.internal:5432/railway")

STREAK_GOAL = 5

_pool = None


async def get_pool():
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL)
    return _pool


async def init_db():
    """Создаёт таблицы если их нет."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                lang TEXT DEFAULT 'ru',
                referred_by BIGINT DEFAULT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            ALTER TABLE users ADD COLUMN IF NOT EXISTS referred_by BIGINT DEFAULT NULL
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS purchases (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                username TEXT,
                full_name TEXT,
                product_key TEXT,
                product_title TEXT,
                amount INTEGER,
                original_amount INTEGER,
                discount_percent INTEGER DEFAULT 0,
                purchased_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            ALTER TABLE purchases ADD COLUMN IF NOT EXISTS original_amount INTEGER DEFAULT NULL
        """)
        await conn.execute("""
            ALTER TABLE purchases ADD COLUMN IF NOT EXISTS discount_percent INTEGER DEFAULT 0
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS streaks (
                user_id BIGINT PRIMARY KEY,
                streak INTEGER DEFAULT 0,
                last_date DATE,
                total_completed INTEGER DEFAULT 0,
                rewards_claimed INTEGER DEFAULT 0,
                pending_approval BOOLEAN DEFAULT FALSE,
                last_report TEXT,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS reports (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                username TEXT,
                full_name TEXT,
                task TEXT,
                report_text TEXT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS referrals (
                id SERIAL PRIMARY KEY,
                referrer_id BIGINT,
                referred_id BIGINT,
                purchased BOOLEAN DEFAULT FALSE,
                purchased_at TIMESTAMP DEFAULT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS discounts (
                user_id BIGINT PRIMARY KEY,
                discount_percent INTEGER DEFAULT 0,
                used BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
                streak INTEGER DEFAULT 0,
                last_date DATE,
                total_completed INTEGER DEFAULT 0,
                rewards_claimed INTEGER DEFAULT 0,
                pending_approval BOOLEAN DEFAULT FALSE,
                last_report TEXT,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS reports (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                username TEXT,
                full_name TEXT,
                task TEXT,
                report_text TEXT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)


# ─── Пользователи ───────────────────────────────────────────────────────────────

async def upsert_user(user_id: int, username: str, full_name: str, lang: str = "ru"):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (user_id, username, full_name, lang)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (user_id) DO UPDATE
            SET username = EXCLUDED.username, full_name = EXCLUDED.full_name
        """, user_id, username or "", full_name or "", lang)


async def set_user_lang(user_id: int, lang: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO users (user_id, lang) VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET lang = $2
        """, user_id, lang)


async def get_user_lang(user_id: int) -> str:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT lang FROM users WHERE user_id = $1", user_id)
        return row["lang"] if row else "ru"


async def get_all_users():
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch("SELECT user_id, username, full_name FROM users")


async def get_users_count() -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT COUNT(*) FROM users")


# ─── Покупки ────────────────────────────────────────────────────────────────────

async def save_purchase(user_id: int, username: str, full_name: str, product_key: str, product_title: str, amount: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO purchases (user_id, username, full_name, product_key, product_title, amount)
            VALUES ($1, $2, $3, $4, $5, $6)
        """, user_id, username or "", full_name or "", product_key, product_title, amount)


async def get_purchases_count() -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT COUNT(*) FROM purchases")


async def get_purchases_today() -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT COUNT(*) FROM purchases WHERE purchased_at::date = CURRENT_DATE"
        )


async def get_top_products():
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT product_title, COUNT(*) as cnt
            FROM purchases
            GROUP BY product_title
            ORDER BY cnt DESC
            LIMIT 5
        """)


async def get_recent_purchases(limit: int = 10):
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT user_id, username, full_name, product_title, amount, purchased_at
            FROM purchases
            ORDER BY purchased_at DESC
            LIMIT $1
        """, limit)


# ─── Стрики ─────────────────────────────────────────────────────────────────────

async def get_user_streak(user_id: int) -> dict:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM streaks WHERE user_id = $1", user_id)
        if not row:
            await conn.execute("""
                INSERT INTO streaks (user_id) VALUES ($1)
                ON CONFLICT DO NOTHING
            """, user_id)
            return {
                "streak": 0, "last_date": None, "total_completed": 0,
                "rewards_claimed": 0, "pending_approval": False, "last_report": None
            }
        return dict(row)


async def set_pending(user_id: int, report_text: str = None):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO streaks (user_id, pending_approval, last_report)
            VALUES ($1, TRUE, $2)
            ON CONFLICT (user_id) DO UPDATE
            SET pending_approval = TRUE, last_report = $2, updated_at = NOW()
        """, user_id, report_text)


async def save_report(user_id: int, username: str, full_name: str, task: str, report_text: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO reports (user_id, username, full_name, task, report_text)
            VALUES ($1, $2, $3, $4, $5)
        """, user_id, username or "", full_name or "", task, report_text)


async def approve_checkin(user_id: int) -> dict:
    pool = await get_pool()
    today = date.today()
    yesterday = today - timedelta(days=1)

    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM streaks WHERE user_id = $1", user_id)
        if not row:
            user = {"streak": 0, "last_date": None, "total_completed": 0, "rewards_claimed": 0}
        else:
            user = dict(row)

        if user["last_date"] == today:
            return {"already_done": True, "streak": user["streak"], "reward": False}

        streak_broken = False
        if user["last_date"] == yesterday:
            new_streak = user["streak"] + 1
        else:
            if user["streak"] > 0:
                streak_broken = True
            new_streak = 1

        new_total = user["total_completed"] + 1
        reward = False
        new_rewards = user["rewards_claimed"]

        if new_streak % STREAK_GOAL == 0:
            reward = True
            new_rewards += 1

        await conn.execute("""
            INSERT INTO streaks (user_id, streak, last_date, total_completed, rewards_claimed, pending_approval, updated_at)
            VALUES ($1, $2, $3, $4, $5, FALSE, NOW())
            ON CONFLICT (user_id) DO UPDATE
            SET streak = $2, last_date = $3, total_completed = $4,
                rewards_claimed = $5, pending_approval = FALSE, updated_at = NOW()
        """, user_id, new_streak, today, new_total, new_rewards)

        return {
            "already_done": False,
            "streak": new_streak,
            "reward": reward,
            "streak_broken": streak_broken,
        }


async def reject_checkin(user_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE streaks SET pending_approval = FALSE, updated_at = NOW()
            WHERE user_id = $1
        """, user_id)


async def get_active_streaks():
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT s.user_id, s.streak, s.last_date, u.username, u.full_name
            FROM streaks s
            LEFT JOIN users u ON s.user_id = u.user_id
            WHERE s.streak > 0
            ORDER BY s.streak DESC
            LIMIT 10
        """)


# ─── Статистика ─────────────────────────────────────────────────────────────────

async def get_stats() -> dict:
    pool = await get_pool()
    async with pool.acquire() as conn:
        users = await conn.fetchval("SELECT COUNT(*) FROM users")
        purchases_total = await conn.fetchval("SELECT COUNT(*) FROM purchases")
        purchases_today = await conn.fetchval(
            "SELECT COUNT(*) FROM purchases WHERE purchased_at::date = CURRENT_DATE"
        )
        revenue_total = await conn.fetchval("SELECT COALESCE(SUM(amount), 0) FROM purchases")
        revenue_today = await conn.fetchval(
            "SELECT COALESCE(SUM(amount), 0) FROM purchases WHERE purchased_at::date = CURRENT_DATE"
        )
        active_streaks = await conn.fetchval("SELECT COUNT(*) FROM streaks WHERE streak > 0")
        top = await conn.fetch("""
            SELECT product_title, COUNT(*) as cnt
            FROM purchases GROUP BY product_title
            ORDER BY cnt DESC LIMIT 3
        """)
        return {
            "users": users,
            "purchases_total": purchases_total,
            "purchases_today": purchases_today,
            "revenue_total": revenue_total,
            "revenue_today": revenue_today,
            "active_streaks": active_streaks,
            "top_products": top,
        }


# ─── Рефералы ───────────────────────────────────────────────────────────────────

REFERRAL_DISCOUNTS = {1: 15, 2: 30, 3: 50}  # купивших → скидка %


async def register_referral(referrer_id: int, referred_id: int):
    """Регистрирует что referred_id пришёл по ссылке referrer_id."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Проверяем что такой реферал ещё не зарегистрирован
        existing = await conn.fetchval(
            "SELECT id FROM referrals WHERE referred_id = $1", referred_id
        )
        if not existing and referrer_id != referred_id:
            await conn.execute("""
                INSERT INTO referrals (referrer_id, referred_id)
                VALUES ($1, $2)
                ON CONFLICT DO NOTHING
            """, referrer_id, referred_id)
            # Сохраняем кто пригласил в таблице users
            await conn.execute("""
                UPDATE users SET referred_by = $1 WHERE user_id = $2 AND referred_by IS NULL
            """, referrer_id, referred_id)


async def on_referral_purchase(user_id: int):
    """Вызывается когда реферал совершил покупку — обновляет счётчик и скидку реферера."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Отмечаем что реферал купил
        await conn.execute("""
            UPDATE referrals SET purchased = TRUE, purchased_at = NOW()
            WHERE referred_id = $1 AND purchased = FALSE
        """, user_id)

        # Находим реферера
        row = await conn.fetchrow(
            "SELECT referrer_id FROM referrals WHERE referred_id = $1 AND purchased = TRUE",
            user_id
        )
        if not row:
            return None

        referrer_id = row["referrer_id"]

        # Считаем сколько купивших рефералов у реферера
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM referrals WHERE referrer_id = $1 AND purchased = TRUE",
            referrer_id
        )

        # Определяем скидку
        if count >= 3:
            discount = 50
        elif count == 2:
            discount = 30
        elif count == 1:
            discount = 15
        else:
            return None

        # Сохраняем скидку рефереру
        await conn.execute("""
            INSERT INTO discounts (user_id, discount_percent)
            VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE
            SET discount_percent = GREATEST(discounts.discount_percent, $2), used = FALSE
        """, referrer_id, discount)

        return {"referrer_id": referrer_id, "discount": discount, "referrals_count": count}


async def get_discount(user_id: int) -> int:
    """Возвращает актуальную скидку пользователя (0 если нет)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT discount_percent FROM discounts WHERE user_id = $1 AND used = FALSE",
            user_id
        )
        return row["discount_percent"] if row else 0


async def use_discount(user_id: int):
    """Помечает скидку как использованную после покупки."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE discounts SET used = TRUE WHERE user_id = $1", user_id
        )


async def get_referral_stats(user_id: int) -> dict:
    """Статистика рефералов пользователя."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        total = await conn.fetchval(
            "SELECT COUNT(*) FROM referrals WHERE referrer_id = $1", user_id
        )
        purchased = await conn.fetchval(
            "SELECT COUNT(*) FROM referrals WHERE referrer_id = $1 AND purchased = TRUE", user_id
        )
        discount = await get_discount(user_id)
        return {"total": total, "purchased": purchased, "discount": discount}


async def get_top_referrers(limit: int = 10):
    """Топ рефереров для админ-панели."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT r.referrer_id, u.username, u.full_name,
                   COUNT(*) as total,
                   SUM(CASE WHEN r.purchased THEN 1 ELSE 0 END) as purchased
            FROM referrals r
            LEFT JOIN users u ON r.referrer_id = u.user_id
            GROUP BY r.referrer_id, u.username, u.full_name
            ORDER BY purchased DESC
            LIMIT $1
        """, limit)


# ─── Дневник ────────────────────────────────────────────────────────────────────

async def init_diary_table():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS diary (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                mood TEXT,
                text TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)


async def save_diary_entry(user_id: int, mood: str, text: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO diary (user_id, mood, text) VALUES ($1, $2, $3)
        """, user_id, mood, text)


async def get_diary_entries(user_id: int, limit: int = 5):
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT id, mood, text, created_at
            FROM diary WHERE user_id = $1
            ORDER BY created_at DESC LIMIT $2
        """, user_id, limit)


async def delete_diary_entry(entry_id: int, user_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            DELETE FROM diary WHERE id = $1 AND user_id = $2
        """, entry_id, user_id)


async def get_mood_week(user_id: int):
    """Настроение за последние 7 дней."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT DATE(created_at) as day, mood, COUNT(*) as cnt
            FROM diary
            WHERE user_id = $1
              AND created_at >= NOW() - INTERVAL '7 days'
            GROUP BY DATE(created_at), mood
            ORDER BY day ASC
        """, user_id)


# ─── Уведомления ────────────────────────────────────────────────────────────────

async def init_notifications_table():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS notifications (
                user_id BIGINT PRIMARY KEY,
                diary_push BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)


async def set_diary_push(user_id: int, enabled: bool):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO notifications (user_id, diary_push)
            VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET diary_push = $2
        """, user_id, enabled)


async def get_diary_push(user_id: int) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT diary_push FROM notifications WHERE user_id = $1", user_id
        )
        return row["diary_push"] if row else False


async def get_diary_push_users():
    """Все пользователи у которых включены пуши дневника."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT n.user_id, u.lang
            FROM notifications n
            LEFT JOIN users u ON n.user_id = u.user_id
            WHERE n.diary_push = TRUE
        """)


async def get_streak_at_risk_users():
    """Пользователи у которых стрик > 0 и они не заходили 23+ часов."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT s.user_id, s.streak, u.lang
            FROM streaks s
            LEFT JOIN users u ON s.user_id = u.user_id
            WHERE s.streak > 0
              AND s.last_date = CURRENT_DATE - 1
              AND s.pending_approval = FALSE
              AND s.updated_at <= NOW() - INTERVAL '23 hours'
        """)

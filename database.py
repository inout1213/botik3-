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
                created_at TIMESTAMP DEFAULT NOW()
            )
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
                purchased_at TIMESTAMP DEFAULT NOW()
            )
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

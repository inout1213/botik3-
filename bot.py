"""
BZH Academy — Telegram Sales Bot
Оплата: Telegram Stars | Доставка: PDF сразу после оплаты
"""

import asyncio
import logging
import random
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery, LabeledPrice,
    PreCheckoutQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    BufferedInputFile
)
from aiogram.filters import CommandStart, Command
from aiogram.fsm.storage.memory import MemoryStorage
from config import BOT_TOKEN, ADMIN_ID, CATALOG, SUBSCRIPTION_2M, SUBSCRIPTION_YEAR, CATEGORIES, CATEGORY_MAP, POPULAR, PROBLEM_SEARCH
from streak import progress_bar, STREAK_GOAL, WAITING_REPORT
from database import (
    init_db, upsert_user, set_user_lang, get_user_lang,
    get_all_users, get_stats, get_active_streaks, get_recent_purchases,
    save_purchase, save_report, set_pending, approve_checkin, reject_checkin,
    get_user_streak
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

USER_LANG = {}  # кэш языков


async def get_lang(user_id: int) -> str:
    if user_id in USER_LANG:
        return USER_LANG[user_id]
    lang = await get_user_lang(user_id)
    USER_LANG[user_id] = lang
    return lang


WELCOME_TEXTS = {
    "ru": [
        (
            "Тревога, прокрастинация, выгорание — это не твои черты характера.\n"
            "Это паттерны. И они меняются.\n\n"
            "_BZH Academy — протоколы для тех, кто хочет разобраться._"
        ),
        (
            "Ты читал, слушал, пробовал. Что-то шло не так.\n"
            "Потому что понимать — это не то же самое, что иметь инструмент.\n\n"
            "_BZH Academy — протоколы которые реально работают._"
        ),
        (
            "Большинство людей живут на автопилоте и не знают почему.\n"
            "BZH Academy — протоколы чтобы наконец разобраться.\n\n"
            "_Практично. Коротко. Без воды._"
        ),
        (
            "Никто не учил работать с тревогой, мотивацией, выгоранием.\n"
            "Мы сделали протоколы — коротко, практично, без воды.\n\n"
            "_BZH Academy — начни прямо сейчас._"
        ),
        (
            "Ты не сломан. Мозг просто работает не так, как нас учили думать.\n\n"
            "_BZH Academy — протоколы которые реально работают._"
        ),
        (
            "Сила воли здесь ни при чём. Есть инструменты — просто никто их не дал.\n\n"
            "_BZH Academy — практические протоколы для работы с собой._"
        ),
        (
            "Ты уже пробовал разобраться. Не сработало — потому что метод был не тот.\n\n"
            "_BZH Academy — протоколы на основе психологии, не мотивации._"
        ),
    ],
    "uk": [
        (
            "Тривога, прокрастинація, вигорання — це не твої риси характеру.\n"
            "Це патерни. І вони змінюються.\n\n"
            "_BZH Academy — протоколи для тих, хто хоче розібратися._"
        ),
        (
            "Ти читав, слухав, пробував. Щось йшло не так.\n"
            "Тому що розуміти — це не те саме, що мати інструмент.\n\n"
            "_BZH Academy — протоколи які реально працюють._"
        ),
        (
            "Більшість людей живуть на автопілоті і не знають чому.\n"
            "BZH Academy — протоколи щоб нарешті розібратися.\n\n"
            "_Практично. Коротко. Без води._"
        ),
        (
            "Ніхто не вчив працювати з тривогою, мотивацією, вигоранням.\n"
            "Ми зробили протоколи — коротко, практично, без води.\n\n"
            "_BZH Academy — почни прямо зараз._"
        ),
        (
            "Ти не зламаний. Мозок просто працює не так, як нас вчили думати.\n\n"
            "_BZH Academy — протоколи які реально працюють._"
        ),
        (
            "Сила волі тут ні до чого. Є інструменти — просто ніхто їх не дав.\n\n"
            "_BZH Academy — практичні протоколи для роботи з собою._"
        ),
        (
            "Ти вже пробував розібратися. Не спрацювало — бо метод був не той.\n\n"
            "_BZH Academy — протоколи на основі психології, не мотивації._"
        ),
    ],
}

T = {
    "ru": {
        "what_now": "Что мешает прямо сейчас?",
        "cant_start": "😶‍🌫️ Не могу начать",
        "anxiety": "😰 Тревога и страх",
        "burnout_btn": "🔥 Выгорел, нет сил",
        "insecure": "🫤 Не уверен в себе",
        "conflicts": "⚡️ Конфликты и люди",
        "all_protocols": "📖 Все протоколы",
        "my_streak": "🔥 Мой стрик",
        "language": "🌐 Мова / Язык",
        "back": "◀️ Назад",
        "back_main": "🏠 Главное меню",
        "back_catalog": "◀️ Назад в каталог",
        "popular": "🔥 Популярное",
        "by_category": "🧠 По категориям",
        "search_problem": "🔍 Поиск по проблеме",
        "all_workbooks": "📋 Все воркбуки",
        "library": "🔑 Библиотека — все сразу",
        "catalog_title": "📖 *Воркбуки BZH Academy*\n\nКак хочешь найти воркбук?",
        "choose_category": "🧠 *Выбери категорию:*",
        "what_bothers_q": "🔍 *Что тебя беспокоит?*\n\n_Выбери — подберём воркбук._",
        "here_helps": "Вот что поможет:\n\n",
        "not_found": "Ничего не найдено.",
        "all_title": "📋 *Все воркбуки BZH Academy*\n\n",
        "popular_title": "🔥 *Популярные воркбуки*\n\n",
        "about_text": (
            "💡 *О BZH Academy*\n\n"
            "Мы делаем воркбуки — не курсы, не лекции.\n"
            "Только практика. Только инструменты.\n\n"
            "Каждый воркбук — это:\n"
            "· 8–10 блоков с упражнениями\n"
            "· КПТ-инструменты и чек-листы\n"
            "· Директивы для действия\n"
            "· PDF сразу после оплаты\n\n"
            "_Открываешь — и сразу работаешь._"
        ),
        "view_workbooks": "📖 Смотреть воркбуки",
        "sub_text": (
            "🔑 *Библиотека BZH Academy*\n\n"
            "Все воркбуки сразу + все новые за период.\n\n"
            "┌ 📦 *2 месяца* — 1 500 ⭐\n"
            "└ 🏆 *Год* — 2 500 ⭐ · выгоднее в 3×\n\n"
            "_Оплатил — получил всё мгновенно._"
        ),
        "sub_2m": "📦 2 месяца — 1 500 ⭐",
        "sub_year": "🏆 Год — 2 500 ⭐ · лучший выбор",
        "sub_2m_label": "2 месяца",
        "sub_year_label": "год",
        "payment_ok": "✅ *Оплата прошла!*\n\nДержи свой воркбук 👇\n_Открывай и приступай прямо сейчас._",
        "pdf_unavailable": "PDF временно недоступен. Мы свяжемся с тобой в течение нескольких минут.",
        "workbook_unavailable": "Воркбук временно недоступен. Напишем тебе лично.",
        "sub_ok": "✅ *Библиотека BZH Academy — {label}*\n\nВсе воркбуки уже здесь 👇\n_Сохрани их — и возвращайся когда нужно._",
        "send_report": "✍️ Отправить отчёт",
        "report_prompt": (
            "✍️ *Напиши свой отчёт*\n\n"
            "_Расскажи как выполнил задание — что сделал, что почувствовал, что понял._\n\n"
            "Просто отправь текст в этот чат 👇"
        ),
        "cancel": "◀️ Отмена",
        "report_sent": "⏳ *Отчёт отправлен на проверку.*\n\n_Как только подтвердят — стрик засчитается_ 👍",
        "done_today": "✅ *Сегодня уже засчитано!*\n_Возвращайся завтра._",
        "pending": "⏳ *Отчёт на проверке.*\n_Ожидай подтверждения._",
        "task_today": "📌 *Задание на сегодня:*\n_{task}_",
        "reward_msg": "🎁 *Твоя награда — {title}*\n\n_Заслужил. Держи!_",
        "reward_pdf_unavail": "PDF временно недоступен. Напишем тебе лично.",
        "choose_lang": "🌐 Выбери язык / Обери мову:",
        "lang_set": "Язык изменён на русский 🇷🇺",
    },
    "uk": {
        "what_now": "Що заважає прямо зараз?",
        "cant_start": "😶‍🌫️ Не можу почати",
        "anxiety": "😰 Тривога і страх",
        "burnout_btn": "🔥 Вигорів, немає сил",
        "insecure": "🫤 Не впевнений у собі",
        "conflicts": "⚡️ Конфлікти і люди",
        "all_protocols": "📖 Всі протоколи",
        "my_streak": "🔥 Мій стрік",
        "language": "🌐 Мова / Язык",
        "back": "◀️ Назад",
        "back_main": "🏠 Головне меню",
        "back_catalog": "◀️ Назад до каталогу",
        "popular": "🔥 Популярне",
        "by_category": "🧠 За категоріями",
        "search_problem": "🔍 Пошук за проблемою",
        "all_workbooks": "📋 Всі воркбуки",
        "library": "🔑 Бібліотека — все одразу",
        "catalog_title": "📖 *Воркбуки BZH Academy*\n\nЯк хочеш знайти воркбук?",
        "choose_category": "🧠 *Обери категорію:*",
        "what_bothers_q": "🔍 *Що тебе турбує?*\n\n_Обери — підберемо воркбук._",
        "here_helps": "Ось що допоможе:\n\n",
        "not_found": "Нічого не знайдено.",
        "all_title": "📋 *Всі воркбуки BZH Academy*\n\n",
        "popular_title": "🔥 *Популярні воркбуки*\n\n",
        "about_text": (
            "💡 *Про BZH Academy*\n\n"
            "Ми робимо воркбуки — не курси, не лекції.\n"
            "Тільки практика. Тільки інструменти.\n\n"
            "Кожен воркбук — це:\n"
            "· 8–10 блоків з вправами\n"
            "· КПТ-інструменти і чек-листи\n"
            "· Директиви для дії\n"
            "· PDF одразу після оплати\n\n"
            "_Відкриваєш — і одразу працюєш._"
        ),
        "view_workbooks": "📖 Дивитись воркбуки",
        "sub_text": (
            "🔑 *Бібліотека BZH Academy*\n\n"
            "Всі воркбуки одразу + всі нові за період.\n\n"
            "┌ 📦 *2 місяці* — 1 500 ⭐\n"
            "└ 🏆 *Рік* — 2 500 ⭐ · вигідніше в 3×\n\n"
            "_Оплатив — отримав все миттєво._"
        ),
        "sub_2m": "📦 2 місяці — 1 500 ⭐",
        "sub_year": "🏆 Рік — 2 500 ⭐ · найкращий вибір",
        "sub_2m_label": "2 місяці",
        "sub_year_label": "рік",
        "payment_ok": "✅ *Оплата пройшла!*\n\nТримай свій воркбук 👇\n_Відкривай і починай прямо зараз._",
        "pdf_unavailable": "PDF тимчасово недоступний. Ми зв'яжемося з тобою протягом кількох хвилин.",
        "workbook_unavailable": "Воркбук тимчасово недоступний. Напишемо тобі особисто.",
        "sub_ok": "✅ *Бібліотека BZH Academy — {label}*\n\nВсі воркбуки вже тут 👇\n_Збережи їх — і повертайся коли потрібно._",
        "send_report": "✍️ Надіслати звіт",
        "report_prompt": (
            "✍️ *Напиши свій звіт*\n\n"
            "_Розкажи як виконав завдання — що зробив, що відчув, що зрозумів._\n\n"
            "Просто надішли текст у цей чат 👇"
        ),
        "cancel": "◀️ Скасування",
        "report_sent": "⏳ *Звіт надіслано на перевірку.*\n\n_Як тільки підтвердять — стрік зарахується_ 👍",
        "done_today": "✅ *Сьогодні вже зараховано!*\n_Повертайся завтра._",
        "pending": "⏳ *Звіт на перевірці.*\n_Очікуй підтвердження._",
        "task_today": "📌 *Завдання на сьогодні:*\n_{task}_",
        "reward_msg": "🎁 *Твоя нагорода — {title}*\n\n_Заслужив. Тримай!_",
        "reward_pdf_unavail": "PDF тимчасово недоступний. Напишемо тобі особисто.",
        "choose_lang": "🌐 Виберіть мову / Выберите язык:",
        "lang_set": "Мову змінено на українську 🇺🇦",
    },
}

DAILY_TASKS = {
    "ru": [
        "Запиши одну ситуацию, где ты избегал действия из-за тревоги. Что ты сделал вместо?",
        "Выдели 10 минут и проведи декатастрофизацию своей главной тревоги прямо сейчас.",
        "Сделай один маленький шаг к тому, что откладывал. Любой. Запиши результат.",
        "Определи свою главную задачу на сегодня по правилу 1-3-5. Выполни хотя бы «1».",
        "Проведи 15 минут без телефона. Только бумага и мысли. Запиши что пришло в голову.",
        "Вспомни ситуацию из прошлого, которая началась с неопределённости и привела к росту.",
        "Сделай дыхание 4-7-8 три раза подряд. Запиши как изменилось состояние.",
    ],
    "uk": [
        "Запиши одну ситуацію, де ти уникав дії через тривогу. Що ти зробив натомість?",
        "Виділи 10 хвилин і проведи декатастрофізацію своєї головної тривоги прямо зараз.",
        "Зроби один маленький крок до того, що відкладав. Будь-який. Запиши результат.",
        "Визнач своє головне завдання на сьогодні за правилом 1-3-5. Виконай хоча б «1».",
        "Проведи 15 хвилин без телефону. Тільки папір і думки. Запиши що прийшло в голову.",
        "Згадай ситуацію з минулого, яка почалася з невизначеності і призвела до зростання.",
        "Зроби дихання 4-7-8 три рази поспіль. Запиши як змінився стан.",
    ],
}

PROBLEM_SEARCH_UK = {
    "не можу почати": ["procrastination", "motivation", "self_doubt"],
    "тривога": ["uncertainty", "self_doubt", "burnout"],
    "втомився": ["burnout", "productivity", "motivation"],
    "конфлікт": ["conflict", "toxic_relationships"],
    "не впевнений": ["self_doubt", "imposter", "motivation"],
    "самотньо": ["loneliness", "toxic_relationships"],
    "не можу вирішити": ["decisions", "uncertainty"],
    "вигорів": ["burnout", "motivation", "productivity"],
    "токсичні": ["toxic_relationships", "conflict"],
}


# ─── Клавиатуры ────────────────────────────────────────────────────────────────

def main_menu(lang: str = "ru") -> InlineKeyboardMarkup:
    t = T[lang]
    if lang == "uk":
        buttons = [
            [InlineKeyboardButton(text=t["cant_start"], callback_data="uk_не можу почати")],
            [InlineKeyboardButton(text=t["anxiety"], callback_data="uk_тривога")],
            [InlineKeyboardButton(text=t["burnout_btn"], callback_data="uk_вигорів")],
            [InlineKeyboardButton(text=t["insecure"], callback_data="uk_не впевнений")],
            [InlineKeyboardButton(text=t["conflicts"], callback_data="uk_конфлікт")],
        ]
    else:
        buttons = [
            [InlineKeyboardButton(text=t["cant_start"], callback_data="search_не могу начать")],
            [InlineKeyboardButton(text=t["anxiety"], callback_data="search_тревога")],
            [InlineKeyboardButton(text=t["burnout_btn"], callback_data="search_выгорел")],
            [InlineKeyboardButton(text=t["insecure"], callback_data="search_не уверен")],
            [InlineKeyboardButton(text=t["conflicts"], callback_data="search_конфликт")],
        ]
    buttons.append([
        InlineKeyboardButton(text=t["all_protocols"], callback_data="catalog"),
        InlineKeyboardButton(text=t["my_streak"], callback_data="streak"),
    ])
    buttons.append([InlineKeyboardButton(text=t["language"], callback_data="choose_lang")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def catalog_keyboard(lang: str = "ru") -> InlineKeyboardMarkup:
    t = T[lang]
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t["popular"], callback_data="cat_popular")],
        [InlineKeyboardButton(text=t["by_category"], callback_data="cat_categories")],
        [InlineKeyboardButton(text=t["search_problem"], callback_data="cat_search")],
        [InlineKeyboardButton(text=t["all_workbooks"], callback_data="cat_all")],
        [InlineKeyboardButton(text=t["library"], callback_data="subscription")],
        [InlineKeyboardButton(text=t["back"], callback_data="back_main")],
    ])


def workbook_list_keyboard(keys: list, back: str = "catalog", lang: str = "ru") -> InlineKeyboardMarkup:
    t = T[lang]
    buttons = []
    for key in keys:
        item = CATALOG.get(key)
        if item:
            title = item.get("title_uk", item["title"]) if lang == "uk" else item["title"]
            buttons.append([InlineKeyboardButton(
                text=f"{item['emoji']} {title} — {item['price']} ⭐",
                callback_data=f"buy_{key}"
            )])
    buttons.append([InlineKeyboardButton(text=t["back"], callback_data=back)])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def categories_keyboard(lang: str = "ru") -> InlineKeyboardMarkup:
    t = T[lang]
    buttons = []
    for key, cat in CATEGORIES.items():
        title = cat.get("title_uk", cat["title"]) if lang == "uk" else cat["title"]
        buttons.append([InlineKeyboardButton(
            text=f"{cat['emoji']} {title}",
            callback_data=f"category_{key}"
        )])
    buttons.append([InlineKeyboardButton(text=t["back"], callback_data="catalog")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def streak_keyboard(lang: str = "ru", pending: bool = False) -> InlineKeyboardMarkup:
    t = T[lang]
    buttons = []
    if not pending:
        buttons.append([InlineKeyboardButton(text=t["send_report"], callback_data="send_report")])
    buttons.append([InlineKeyboardButton(text=t["back"], callback_data="back_main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def admin_approve_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Засчитать", callback_data=f"approve_{user_id}"),
        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_{user_id}"),
    ]])


def reward_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{item['emoji']} {item['title']}", callback_data=f"reward_{key}")]
        for key, item in CATALOG.items()
    ])


def lang_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🇺🇦 Українська", callback_data="set_lang_uk")],
        [InlineKeyboardButton(text="🇷🇺 Русский", callback_data="set_lang_ru")],
    ])


# ─── Старт ─────────────────────────────────────────────────────────────────────

@dp.message(CommandStart())
async def cmd_start(message: Message):
    user = message.from_user
    await upsert_user(user.id, user.username, user.full_name)
    lang = await get_lang(user.id)
    phrase = random.choice(WELCOME_TEXTS[lang])
    t = T[lang]
    await message.answer(
        f"{phrase}\n\n{t['what_now']}",
        parse_mode="Markdown",
        reply_markup=main_menu(lang)
    )


@dp.message(Command("admin"))
async def cmd_admin(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    await message.answer(
        "🔧 Панель администратора BZH Academy",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
            [InlineKeyboardButton(text="🔥 Активные стрики", callback_data="admin_streaks")],
            [InlineKeyboardButton(text="💰 Последние покупки", callback_data="admin_purchases")],
            [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
            [InlineKeyboardButton(text="🎁 Выдать воркбук", callback_data="admin_send_wb")],
        ])
    )


@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    stats = await get_stats()
    top = "\n".join([f"  {i+1}. {r['product_title']} — {r['cnt']} шт" for i, r in enumerate(stats["top_products"])])
    text = (
        f"📊 Статистика BZH Academy\n\n"
        f"👥 Пользователей: {stats['users']}\n"
        f"💰 Покупок всего: {stats['purchases_total']}\n"
        f"💰 Покупок сегодня: {stats['purchases_today']}\n"
        f"⭐ Звёзд всего: {stats['revenue_total']}\n"
        f"⭐ Звёзд сегодня: {stats['revenue_today']}\n"
        f"🔥 Активных стриков: {stats['active_streaks']}\n\n"
        f"🏆 Топ воркбуки:\n{top or '  Нет данных'}"
    )
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")]
        ])
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_streaks")
async def admin_streaks(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    rows = await get_active_streaks()
    if not rows:
        text = "🔥 Активных стриков нет"
    else:
        lines = []
        for r in rows:
            name = r["full_name"] or r["username"] or str(r["user_id"])
            lines.append(f"🔥 {r['streak']} дн — {name} (@{r['username'] or '—'})")
        text = "🔥 Активные стрики (топ 10):\n\n" + "\n".join(lines)
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")]
        ])
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_purchases")
async def admin_purchases(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    rows = await get_recent_purchases(10)
    if not rows:
        text = "💰 Покупок пока нет"
    else:
        lines = []
        for r in rows:
            dt = r["purchased_at"].strftime("%d.%m %H:%M")
            name = r["full_name"] or r["username"] or str(r["user_id"])
            lines.append(f"• {dt} — {name}\n  {r['product_title']} — {r['amount']} ⭐")
        text = "💰 Последние 10 покупок:\n\n" + "\n\n".join(lines)
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")]
        ])
    )
    await callback.answer()


BROADCAST_PENDING = {}


@dp.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_start(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    BROADCAST_PENDING[callback.from_user.id] = True
    await callback.message.edit_text(
        "📢 Напиши текст рассылки — отправлю всем пользователям.\n\nПросто напиши сообщение в чат:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена", callback_data="admin_back")]
        ])
    )
    await callback.answer()


SEND_WB_PENDING = {}


@dp.callback_query(F.data == "admin_send_wb")
async def admin_send_wb_start(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    SEND_WB_PENDING[callback.from_user.id] = True
    await callback.message.edit_text(
        "🎁 Напиши в формате:\n\nUSER_ID ключ_воркбука\n\nНапример:\n123456789 burnout\n\nКлючи: " + ", ".join(CATALOG.keys()),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена", callback_data="admin_back")]
        ])
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_back")
async def admin_back(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    BROADCAST_PENDING.pop(callback.from_user.id, None)
    SEND_WB_PENDING.pop(callback.from_user.id, None)
    await callback.message.edit_text(
        "🔧 Панель администратора BZH Academy",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
            [InlineKeyboardButton(text="🔥 Активные стрики", callback_data="admin_streaks")],
            [InlineKeyboardButton(text="💰 Последние покупки", callback_data="admin_purchases")],
            [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
            [InlineKeyboardButton(text="🎁 Выдать воркбук", callback_data="admin_send_wb")],
        ])
    )
    await callback.answer()


# ─── Язык ───────────────────────────────────────────────────────────────────────

@dp.callback_query(F.data == "choose_lang")
async def choose_lang(callback: CallbackQuery):
    lang = await get_lang(callback.from_user.id)
    await callback.message.edit_text(
        T[lang]["choose_lang"],
        reply_markup=lang_keyboard()
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("set_lang_"))
async def set_lang(callback: CallbackQuery):
    lang = callback.data.replace("set_lang_", "")
    USER_LANG[callback.from_user.id] = lang
    await set_user_lang(callback.from_user.id, lang)
    t = T[lang]
    phrase = random.choice(WELCOME_TEXTS[lang])
    await callback.message.edit_text(
        f"{t['lang_set']}\n\n{phrase}\n\n{t['what_now']}",
        parse_mode="Markdown",
        reply_markup=main_menu(lang)
    )
    await callback.answer()


# ─── Навигация ──────────────────────────────────────────────────────────────────

@dp.callback_query(F.data == "back_main")
async def back_main(callback: CallbackQuery):
    lang = await get_lang(callback.from_user.id)
    phrase = random.choice(WELCOME_TEXTS[lang])
    t = T[lang]
    await callback.message.edit_text(
        f"{phrase}\n\n{t['what_now']}",
        parse_mode="Markdown",
        reply_markup=main_menu(lang)
    )
    await callback.answer()


@dp.callback_query(F.data == "catalog")
async def show_catalog(callback: CallbackQuery):
    lang = await get_lang(callback.from_user.id)
    await callback.message.edit_text(
        T[lang]["catalog_title"],
        parse_mode="Markdown",
        reply_markup=catalog_keyboard(lang)
    )
    await callback.answer()


@dp.callback_query(F.data == "cat_popular")
async def show_popular(callback: CallbackQuery):
    lang = await get_lang(callback.from_user.id)
    t = T[lang]
    text = t["popular_title"]
    for key in POPULAR:
        item = CATALOG.get(key)
        if item:
            title = item.get("title_uk", item["title"]) if lang == "uk" else item["title"]
            desc = item.get("description_uk", item["description"]) if lang == "uk" else item["description"]
            text += f"{item['emoji']} *{title}*\n_{desc}_\n💳 {item['price']} ⭐\n\n"
    await callback.message.edit_text(
        text, parse_mode="Markdown",
        reply_markup=workbook_list_keyboard(POPULAR, back="catalog", lang=lang)
    )
    await callback.answer()


@dp.callback_query(F.data == "cat_categories")
async def show_categories(callback: CallbackQuery):
    lang = await get_lang(callback.from_user.id)
    await callback.message.edit_text(
        T[lang]["choose_category"],
        parse_mode="Markdown",
        reply_markup=categories_keyboard(lang)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("category_"))
async def show_category(callback: CallbackQuery):
    lang = await get_lang(callback.from_user.id)
    key = callback.data.replace("category_", "")
    cat = CATEGORIES.get(key)
    keys = CATEGORY_MAP.get(key, [])
    if not cat or not keys:
        await callback.answer(T[lang]["not_found"], show_alert=True)
        return
    cat_title = cat.get("title_uk", cat["title"]) if lang == "uk" else cat["title"]
    text = f"{cat['emoji']} *{cat_title}*\n\n"
    for k in keys:
        item = CATALOG.get(k)
        if item:
            title = item.get("title_uk", item["title"]) if lang == "uk" else item["title"]
            desc = item.get("description_uk", item["description"]) if lang == "uk" else item["description"]
            text += f"{item['emoji']} *{title}*\n_{desc}_\n💳 {item['price']} ⭐\n\n"
    await callback.message.edit_text(
        text, parse_mode="Markdown",
        reply_markup=workbook_list_keyboard(keys, back="cat_categories", lang=lang)
    )
    await callback.answer()


@dp.callback_query(F.data == "cat_search")
async def show_search(callback: CallbackQuery):
    lang = await get_lang(callback.from_user.id)
    t = T[lang]
    if lang == "uk":
        problems = list(PROBLEM_SEARCH_UK.keys())
        prefix = "uk_"
    else:
        problems = list(PROBLEM_SEARCH.keys())
        prefix = "search_"
    buttons = [[InlineKeyboardButton(text=f"🔍 {p}", callback_data=f"{prefix}{p}")] for p in problems]
    buttons.append([InlineKeyboardButton(text=t["back"], callback_data="catalog")])
    await callback.message.edit_text(
        t["what_bothers_q"],
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("search_"))
async def show_search_result_ru(callback: CallbackQuery):
    lang = await get_lang(callback.from_user.id)
    t = T[lang]
    problem = callback.data.replace("search_", "")
    keys = PROBLEM_SEARCH.get(problem, [])
    if not keys:
        await callback.answer(t["not_found"], show_alert=True)
        return
    text = f"🔍 *«{problem}»*\n\n{t['here_helps']}"
    for k in keys:
        item = CATALOG.get(k)
        if item:
            title = item.get("title_uk", item["title"]) if lang == "uk" else item["title"]
            desc = item.get("description_uk", item["description"]) if lang == "uk" else item["description"]
            text += f"{item['emoji']} *{title}*\n_{desc}_\n💳 {item['price']} ⭐\n\n"
    await callback.message.edit_text(
        text, parse_mode="Markdown",
        reply_markup=workbook_list_keyboard(keys, back="back_main", lang=lang)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("uk_"))
async def show_search_result_uk(callback: CallbackQuery):
    lang = "uk"
    t = T[lang]
    problem = callback.data.replace("uk_", "")
    keys = PROBLEM_SEARCH_UK.get(problem, [])
    if not keys:
        await callback.answer(t["not_found"], show_alert=True)
        return
    text = f"🔍 *«{problem}»*\n\n{t['here_helps']}"
    for k in keys:
        item = CATALOG.get(k)
        if item:
            title = item.get("title_uk", item["title"])
            desc = item.get("description_uk", item["description"])
            text += f"{item['emoji']} *{title}*\n_{desc}_\n💳 {item['price']} ⭐\n\n"
    await callback.message.edit_text(
        text, parse_mode="Markdown",
        reply_markup=workbook_list_keyboard(keys, back="back_main", lang=lang)
    )
    await callback.answer()


@dp.callback_query(F.data == "cat_all")
async def show_all(callback: CallbackQuery):
    lang = await get_lang(callback.from_user.id)
    t = T[lang]
    text = t["all_title"]
    for item in CATALOG.values():
        title = item.get("title_uk", item["title"]) if lang == "uk" else item["title"]
        desc = item.get("description_uk", item["description"]) if lang == "uk" else item["description"]
        text += f"{item['emoji']} *{title}*\n_{desc}_\n💳 {item['price']} ⭐\n\n"
    await callback.message.edit_text(
        text, parse_mode="Markdown",
        reply_markup=workbook_list_keyboard(list(CATALOG.keys()), back="catalog", lang=lang)
    )
    await callback.answer()


@dp.callback_query(F.data == "about")
async def show_about(callback: CallbackQuery):
    lang = await get_lang(callback.from_user.id)
    t = T[lang]
    await callback.message.edit_text(
        t["about_text"],
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=t["view_workbooks"], callback_data="catalog")],
            [InlineKeyboardButton(text=t["back"], callback_data="back_main")],
        ])
    )
    await callback.answer()


# ─── Покупка ────────────────────────────────────────────────────────────────────

@dp.callback_query(F.data.startswith("buy_"))
async def buy_workbook(callback: CallbackQuery):
    lang = await get_lang(callback.from_user.id)
    key = callback.data.replace("buy_", "")
    item = CATALOG.get(key)
    if not item:
        await callback.answer(T[lang]["not_found"], show_alert=True)
        return
    title = item.get("title_uk", item["title"]) if lang == "uk" else item["title"]
    desc = item.get("description_uk", item["description"]) if lang == "uk" else item["description"]
    await callback.answer()
    await bot.send_invoice(
        chat_id=callback.from_user.id,
        title=title,
        description=desc,
        payload=f"workbook_{key}",
        currency="XTR",
        prices=[LabeledPrice(label=title, amount=item["price"])],
        provider_token="",
    )


@dp.callback_query(F.data == "subscription")
async def show_subscription(callback: CallbackQuery):
    lang = await get_lang(callback.from_user.id)
    t = T[lang]
    await callback.message.edit_text(
        t["sub_text"],
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=t["sub_2m"], callback_data="buy_sub_2m")],
            [InlineKeyboardButton(text=t["sub_year"], callback_data="buy_sub_year")],
            [InlineKeyboardButton(text=t["back"], callback_data="catalog")],
        ])
    )
    await callback.answer()


@dp.callback_query(F.data == "buy_sub_2m")
async def buy_sub_2m(callback: CallbackQuery):
    sub = SUBSCRIPTION_2M
    await callback.answer()
    await bot.send_invoice(
        chat_id=callback.from_user.id,
        title=sub["title"],
        description=sub["description"],
        payload="subscription_2m",
        currency="XTR",
        prices=[LabeledPrice(label=sub["title"], amount=sub["price"])],
        provider_token="",
    )


@dp.callback_query(F.data == "buy_sub_year")
async def buy_sub_year(callback: CallbackQuery):
    sub = SUBSCRIPTION_YEAR
    await callback.answer()
    await bot.send_invoice(
        chat_id=callback.from_user.id,
        title=sub["title"],
        description=sub["description"],
        payload="subscription_year",
        currency="XTR",
        prices=[LabeledPrice(label=sub["title"], amount=sub["price"])],
        provider_token="",
    )


# ─── Оплата ─────────────────────────────────────────────────────────────────────

@dp.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    await query.answer(ok=True)


@dp.message(F.successful_payment)
async def successful_payment(message: Message):
    import datetime
    payload = message.successful_payment.invoice_payload
    user = message.from_user
    lang = await get_lang(user.id)
    t = T[lang]
    amount = message.successful_payment.total_amount

    logger.info(f"Оплата: user={user.id} ({user.username}), payload={payload}")

    await bot.send_message(
        ADMIN_ID,
        f"💰 Новая оплата!\n\n"
        f"Пользователь: {user.full_name} (@{user.username})\n"
        f"ID: {user.id}\n"
        f"Товар: {payload}\n"
        f"Сумма: {amount} ⭐\n"
        f"Время: {datetime.datetime.now().strftime('%d.%m.%Y %H:%M')}"
    )

    if payload.startswith("workbook_"):
        key = payload.replace("workbook_", "")
        item = CATALOG.get(key)
        if item and item.get("pdf_path"):
            title = item.get("title_uk", item["title"]) if lang == "uk" else item["title"]
            await save_purchase(user.id, user.username, user.full_name, key, title, amount)
            await message.answer(t["payment_ok"], parse_mode="Markdown")
            try:
                with open(item["pdf_path"], "rb") as pdf:
                    await bot.send_document(
                        message.chat.id,
                        document=BufferedInputFile(pdf.read(), filename=f"{title}.pdf"),
                        caption=f"📖 {title} | BZH Academy"
                    )
            except FileNotFoundError:
                logger.error(f"PDF не найден: {item['pdf_path']}")
                await message.answer(t["pdf_unavailable"])
        else:
            await message.answer(t["workbook_unavailable"])

    elif payload in ("subscription_2m", "subscription_year"):
        label = t["sub_2m_label"] if payload == "subscription_2m" else t["sub_year_label"]
        await save_purchase(user.id, user.username, user.full_name, payload, f"Библиотека — {label}", amount)
        await message.answer(t["sub_ok"].format(label=label), parse_mode="Markdown")
        for item in CATALOG.values():
            if item.get("pdf_path"):
                try:
                    with open(item["pdf_path"], "rb") as pdf:
                        await bot.send_document(
                            message.chat.id,
                            document=BufferedInputFile(pdf.read(), filename=f"{item['title']}.pdf"),
                            caption=f"📖 {item['title']} | BZH Academy"
                        )
                except FileNotFoundError:
                    logger.warning(f"PDF не найден: {item['pdf_path']}")


# ─── Стрик ──────────────────────────────────────────────────────────────────────

@dp.callback_query(F.data == "streak")
async def show_streak(callback: CallbackQuery):
    import datetime
    lang = await get_lang(callback.from_user.id)
    t = T[lang]
    user_id = callback.from_user.id
    user = await get_user_streak(user_id)
    today_str = str(datetime.date.today())
    done_today = str(user["last_date"]) == today_str if user["last_date"] else False
    pending = user.get("pending_approval", False)

    streak = user["streak"]
    goal = STREAK_GOAL
    bar = progress_bar(streak)

    if lang == "uk":
        from streak import _day_uk
        day_word = _day_uk
    else:
        from streak import _day_ru
        day_word = _day_ru

    if pending:
        if lang == "uk":
            status = f"🔥 *Твій стрік: {streak} {day_word(streak)}*\n\n{bar}\n\n⏳ _Звіт надіслано — очікуй підтвердження._"
        else:
            status = f"🔥 *Твой стрик: {streak} {day_word(streak)}*\n\n{bar}\n\n⏳ _Отчёт отправлен — ожидай подтверждения._"
    elif streak == 0:
        if lang == "uk":
            status = f"🔥 *Твій стрік: 0 днів*\n\n{bar}\n\nВиконай сьогоднішнє завдання і надішли звіт!\n_{goal} днів поспіль = безкоштовний воркбук_ 🎁"
        else:
            status = f"🔥 *Твой стрик: 0 дней*\n\n{bar}\n\nВыполни задание и отправь отчёт!\n_{goal} дней подряд = бесплатный воркбук_ 🎁"
    else:
        remaining = goal - (streak % goal) if streak % goal != 0 else 0
        if lang == "uk":
            status = f"🔥 *Твій стрік: {streak} {day_word(streak)}*\n\n{bar}\n\nДо безкоштовного воркбуку: *{remaining} {day_word(remaining)}* 🎁\n_Не переривай стрік — повертайся завтра!_"
        else:
            status = f"🔥 *Твой стрик: {streak} {day_word(streak)}*\n\n{bar}\n\nДо бесплатного воркбука: *{remaining} {day_word(remaining)}* 🎁\n_Не прерывай стрик — возвращайся завтра!_"

    task = random.choice(DAILY_TASKS[lang])
    text = status + "\n\n"
    if done_today:
        text += t["done_today"]
    elif pending:
        text += t["pending"]
    else:
        text += t["task_today"].format(task=task)
        WAITING_REPORT[user_id] = task

    await callback.message.edit_text(
        text, parse_mode="Markdown",
        reply_markup=streak_keyboard(lang=lang, pending=pending or done_today)
    )
    await callback.answer()


@dp.callback_query(F.data == "send_report")
async def ask_for_report(callback: CallbackQuery):
    lang = await get_lang(callback.from_user.id)
    t = T[lang]
    user_id = callback.from_user.id
    await callback.message.edit_text(
        t["report_prompt"],
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=t["cancel"], callback_data="streak")]
        ])
    )
    if user_id not in WAITING_REPORT:
        WAITING_REPORT[user_id] = "задание дня"
    await callback.answer()


@dp.message(F.text & ~F.text.startswith("/"))
async def handle_report(message: Message):
    user_id = message.from_user.id

    # Рассылка
    if user_id == ADMIN_ID and BROADCAST_PENDING.get(user_id):
        BROADCAST_PENDING.pop(user_id)
        users = await get_all_users()
        sent = 0
        failed = 0
        for u in users:
            try:
                await bot.send_message(u["user_id"], message.text)
                sent += 1
            except Exception:
                failed += 1
        await message.answer(f"📢 Рассылка завершена!\nОтправлено: {sent}\nОшибок: {failed}")
        return

    # Выдача воркбука вручную
    if user_id == ADMIN_ID and SEND_WB_PENDING.get(user_id):
        SEND_WB_PENDING.pop(user_id)
        try:
            parts = message.text.strip().split()
            target_id = int(parts[0])
            key = parts[1]
            item = CATALOG.get(key)
            if not item:
                await message.answer(f"Воркбук '{key}' не найден. Ключи: {', '.join(CATALOG.keys())}")
                return
            with open(item["pdf_path"], "rb") as pdf:
                await bot.send_document(
                    target_id,
                    document=BufferedInputFile(pdf.read(), filename=f"{item['title']}.pdf"),
                    caption=f"🎁 {item['title']} | BZH Academy"
                )
            await message.answer(f"✅ Воркбук {item['title']} отправлен пользователю {target_id}")
        except Exception as e:
            await message.answer(f"❌ Ошибка: {e}")
        return

    # Отчёт по стрику
    if user_id not in WAITING_REPORT:
        return
    lang = await get_lang(user_id)
    t = T[lang]
    task = WAITING_REPORT.pop(user_id)
    user = message.from_user

    await set_pending(user_id, message.text)
    await save_report(user_id, user.username, user.full_name, task, message.text)
    await upsert_user(user_id, user.username, user.full_name)

    await message.answer(t["report_sent"], parse_mode="Markdown")
    await bot.send_message(
        ADMIN_ID,
        f"📋 Новый отчёт\n\n"
        f"👤 {user.full_name} (@{user.username})\n"
        f"ID: {user_id}\n\n"
        f"📌 Задание:\n{task}\n\n"
        f"✍️ Отчёт:\n{message.text}",
        reply_markup=admin_approve_keyboard(user_id)
    )


@dp.callback_query(F.data.startswith("approve_"))
async def approve_report(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет доступа.", show_alert=True)
        return
    user_id = int(callback.data.replace("approve_", ""))
    result = await approve_checkin(user_id)
    streak = result["streak"]
    from streak import _day_ru

    if result["reward"]:
        await bot.send_message(
            user_id,
            f"🎉 {streak} дней подряд — ты сделал это!\n\n"
            f"{'🔥' * STREAK_GOAL}\n\n"
            f"Выбери воркбук, который получишь бесплатно 👇",
            reply_markup=reward_keyboard()
        )
    else:
        bar = progress_bar(streak)
        remaining = STREAK_GOAL - (streak % STREAK_GOAL)
        await bot.send_message(
            user_id,
            f"✅ Стрик засчитан! День {streak}\n\n{bar}\n\nДо бесплатного воркбука: {remaining} {_day_ru(remaining)}"
        )

    await callback.message.edit_text(callback.message.text + "\n\n✅ Засчитано")
    await callback.answer("Стрик засчитан!")


@dp.callback_query(F.data.startswith("reject_"))
async def reject_report(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет доступа.", show_alert=True)
        return
    user_id = int(callback.data.replace("reject_", ""))
    lang = await get_lang(user_id)
    await reject_checkin(user_id)

    await bot.send_message(
        user_id,
        "❌ Отчёт не принят\n\nПопробуй выполнить задание глубже и отправь новый отчёт.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=T[lang]["my_streak"], callback_data="streak")]
        ])
    )
    await callback.message.edit_text(callback.message.text + "\n\n❌ Отклонено")
    await callback.answer("Отчёт отклонён.")


@dp.callback_query(F.data.startswith("reward_"))
async def send_reward(callback: CallbackQuery):
    lang = await get_lang(callback.from_user.id)
    t = T[lang]
    key = callback.data.replace("reward_", "")
    item = CATALOG.get(key)
    user = callback.from_user

    if not item:
        await callback.answer(t["not_found"], show_alert=True)
        return

    await callback.message.edit_text(
        t["reward_msg"].format(title=item["title"]),
        parse_mode="Markdown"
    )

    try:
        with open(item["pdf_path"], "rb") as pdf:
            await bot.send_document(
                callback.from_user.id,
                document=BufferedInputFile(pdf.read(), filename=f"{item['title']}.pdf"),
                caption=f"🎁 {item['title']} | BZH Academy"
            )
    except FileNotFoundError:
        await callback.message.answer(t["reward_pdf_unavail"])

    await bot.send_message(
        ADMIN_ID,
        f"🎁 *Награда выдана*\n\n"
        f"Пользователь: {user.full_name} (@{user.username})\n"
        f"ID: `{user.id}`\n"
        f"Воркбук: {item['title']}",
        parse_mode="Markdown"
    )
    await callback.answer()


# ─── Запуск ─────────────────────────────────────────────────────────────────────

async def main():
    logger.info("BZH Academy Bot запущен")
    await init_db()
    logger.info("База данных инициализирована")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

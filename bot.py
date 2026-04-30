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
    get_user_streak, register_referral, on_referral_purchase,
    get_discount, use_discount, get_referral_stats, get_top_referrers,
    init_diary_table, save_diary_entry, get_diary_entries,
    delete_diary_entry, get_mood_week,
    init_notifications_table, set_diary_push, get_diary_push,
    get_diary_push_users, get_streak_at_risk_users
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

# ─── Квиз ───────────────────────────────────────────────────────────────────────

QUIZ_ANSWERS = {}  # user_id: [ответы]
DIARY_MOOD = {}   # user_id: выбранное настроение
DIARY_WRITING = set()  # user_id: ожидает текст дневника

QUIZ_QUESTIONS = {
    "ru": [
        {
            "text": "Что сейчас мешает больше всего?",
            "options": [
                ("😶‍🌫️ Не могу начать / откладываю", "procrastination"),
                ("😰 Тревога и страх", "anxiety"),
                ("🔥 Нет сил / выгорание", "burnout"),
                ("🫤 Не уверен в себе", "self_doubt"),
                ("⚡️ Конфликты и отношения", "relations"),
            ]
        },
        {
            "text": "Как давно это происходит?",
            "options": [
                ("🌱 Недавно началось", "recent"),
                ("📅 Несколько месяцев", "months"),
                ("🔄 Уже давно, хронически", "chronic"),
            ]
        },
        {
            "text": "Как это влияет на жизнь?",
            "options": [
                ("💼 Мешает работе", "work"),
                ("❤️ Мешает отношениям", "relations_impact"),
                ("🌀 Мешает всему сразу", "everything"),
                ("🔍 Просто хочу разобраться в себе", "self"),
            ]
        },
        {
            "text": "Пробовал ли уже что-то?",
            "options": [
                ("🆕 Нет, это первый шаг", "first"),
                ("📚 Читал книги / смотрел видео", "books"),
                ("🛋 Был у психолога", "therapy"),
                ("😔 Пробовал многое — не помогло", "tried"),
            ]
        },
        {
            "text": "Что хочешь получить?",
            "options": [
                ("🔍 Понять причину", "understand"),
                ("📋 Конкретный план действий", "plan"),
                ("⚡️ Быстрый результат", "fast"),
                ("🌱 Долгосрочное изменение", "longterm"),
            ]
        },
    ],
    "uk": [
        {
            "text": "Що зараз заважає найбільше?",
            "options": [
                ("😶‍🌫️ Не можу почати / відкладаю", "procrastination"),
                ("😰 Тривога і страх", "anxiety"),
                ("🔥 Немає сил / вигорання", "burnout"),
                ("🫤 Не впевнений у собі", "self_doubt"),
                ("⚡️ Конфлікти і стосунки", "relations"),
            ]
        },
        {
            "text": "Як давно це відбувається?",
            "options": [
                ("🌱 Нещодавно почалося", "recent"),
                ("📅 Кілька місяців", "months"),
                ("🔄 Вже давно, хронічно", "chronic"),
            ]
        },
        {
            "text": "Як це впливає на життя?",
            "options": [
                ("💼 Заважає роботі", "work"),
                ("❤️ Заважає стосункам", "relations_impact"),
                ("🌀 Заважає всьому одразу", "everything"),
                ("🔍 Просто хочу розібратися в собі", "self"),
            ]
        },
        {
            "text": "Пробував вже щось?",
            "options": [
                ("🆕 Ні, це перший крок", "first"),
                ("📚 Читав книги / дивився відео", "books"),
                ("🛋 Був у психолога", "therapy"),
                ("😔 Пробував багато — не допомогло", "tried"),
            ]
        },
        {
            "text": "Що хочеш отримати?",
            "options": [
                ("🔍 Зрозуміти причину", "understand"),
                ("📋 Конкретний план дій", "plan"),
                ("⚡️ Швидкий результат", "fast"),
                ("🌱 Довгострокову зміну", "longterm"),
            ]
        },
    ]
}

# Логика подбора протоколов по ответам
def get_quiz_result(answers: list) -> list:
    scores = {}

    # Q1 — основная проблема
    q1 = answers[0] if len(answers) > 0 else ""
    q1_map = {
        "procrastination": ["procrastination", "motivation", "self_doubt"],
        "anxiety": ["uncertainty", "self_doubt", "burnout"],
        "burnout": ["burnout", "motivation", "productivity"],
        "self_doubt": ["self_doubt", "imposter", "motivation"],
        "relations": ["conflict", "toxic_relationships", "loneliness"],
    }
    for key in q1_map.get(q1, []):
        scores[key] = scores.get(key, 0) + 3

    # Q2 — давность
    q2 = answers[1] if len(answers) > 1 else ""
    if q2 == "chronic":
        for key in ["burnout", "self_doubt", "loneliness"]:
            scores[key] = scores.get(key, 0) + 1
    elif q2 == "recent":
        for key in ["uncertainty", "decisions"]:
            scores[key] = scores.get(key, 0) + 1

    # Q3 — влияние
    q3 = answers[2] if len(answers) > 2 else ""
    if q3 == "work":
        for key in ["productivity", "procrastination", "burnout"]:
            scores[key] = scores.get(key, 0) + 2
    elif q3 == "relations_impact":
        for key in ["conflict", "toxic_relationships", "loneliness"]:
            scores[key] = scores.get(key, 0) + 2
    elif q3 == "everything":
        for key in ["burnout", "motivation", "uncertainty"]:
            scores[key] = scores.get(key, 0) + 2
    elif q3 == "self":
        for key in ["self_doubt", "imposter", "decisions"]:
            scores[key] = scores.get(key, 0) + 2

    # Q4 — опыт
    q4 = answers[3] if len(answers) > 3 else ""
    if q4 == "tried":
        for key in ["burnout", "self_doubt", "uncertainty"]:
            scores[key] = scores.get(key, 0) + 1
    elif q4 == "first":
        for key in ["motivation", "procrastination"]:
            scores[key] = scores.get(key, 0) + 1

    # Q5 — цель
    q5 = answers[4] if len(answers) > 4 else ""
    if q5 == "plan":
        for key in ["productivity", "decisions", "procrastination"]:
            scores[key] = scores.get(key, 0) + 1
    elif q5 == "understand":
        for key in ["self_doubt", "imposter", "uncertainty"]:
            scores[key] = scores.get(key, 0) + 1
    elif q5 == "longterm":
        for key in ["burnout", "motivation", "self_doubt"]:
            scores[key] = scores.get(key, 0) + 1
    elif q5 == "fast":
        for key in ["procrastination", "motivation", "productivity"]:
            scores[key] = scores.get(key, 0) + 1

    # Топ 3 протокола
    sorted_keys = sorted(scores, key=lambda k: scores[k], reverse=True)
    return sorted_keys[:3] if sorted_keys else ["uncertainty", "motivation", "self_doubt"]


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
    buttons.append([
        InlineKeyboardButton(text="📔 Дневник" if lang == "ru" else "📔 Щоденник", callback_data="diary"),
        InlineKeyboardButton(text="🤝 Реферальная" if lang == "ru" else "🤝 Реферальна", callback_data="referral"),
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

    # Обработка реферальной ссылки
    args = message.text.split()
    if len(args) > 1 and args[1].startswith("ref_"):
        try:
            referrer_id = int(args[1].replace("ref_", ""))
            await register_referral(referrer_id, user.id)
        except ValueError:
            pass

    lang = await get_lang(user.id)
    phrase = random.choice(WELCOME_TEXTS[lang])

    if lang == "uk":
        text = f"{phrase}\n\nЯк хочеш продовжити?"
        buttons = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📖 Головне меню", callback_data="back_main")],
            [InlineKeyboardButton(text="🎯 Пройти квіз — підберемо протокол", callback_data="quiz_start")],
        ])
    else:
        text = f"{phrase}\n\nКак хочешь продолжить?"
        buttons = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📖 Главное меню", callback_data="back_main")],
            [InlineKeyboardButton(text="🎯 Пройти квиз — подберём протокол", callback_data="quiz_start")],
        ])

    await message.answer(text, parse_mode="Markdown", reply_markup=buttons)


@dp.callback_query(F.data == "diary")
async def show_diary(callback: CallbackQuery):
    lang = await get_lang(callback.from_user.id)
    user_id = callback.from_user.id
    entries = await get_diary_entries(user_id, 3)
    push_enabled = await get_diary_push(user_id)

    if lang == "uk":
        text = "📔 *Твій щоденник*\n\nЗаписуй думки, відстежуй настрій."
        btn_new = "✏️ Нова запис"
        btn_week = "📊 Настрій за тиждень"
        btn_all = "📋 Всі записи"
        btn_push = "🔔 Нагадування 20:00 — Вкл" if push_enabled else "🔕 Нагадування 20:00 — Вимк"
        btn_back = "◀️ Назад"
        last_label = "*Останні записи:*\n"
    else:
        text = "📔 *Твой дневник*\n\nЗаписывай мысли, отслеживай настроение."
        btn_new = "✏️ Новая запись"
        btn_week = "📊 Настроение за неделю"
        btn_all = "📋 Все записи"
        btn_push = "🔔 Напоминание 20:00 — Вкл" if push_enabled else "🔕 Напоминание 20:00 — Выкл"
        btn_back = "◀️ Назад"
        last_label = "*Последние записи:*\n"

    if entries:
        text += f"\n\n{last_label}"
        for e in entries:
            dt = e["created_at"].strftime("%d.%m %H:%M")
            short = e["text"][:60] + "..." if len(e["text"]) > 60 else e["text"]
            text += f"\n{e['mood']} {dt}\n_{short}_\n"

    await callback.message.edit_text(
        text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=btn_new, callback_data="diary_new")],
            [InlineKeyboardButton(text=btn_week, callback_data="diary_week"),
             InlineKeyboardButton(text=btn_all, callback_data="diary_all")],
            [InlineKeyboardButton(text=btn_push, callback_data="diary_push_toggle")],
            [InlineKeyboardButton(text=btn_back, callback_data="back_main")],
        ])
    )
    await callback.answer()


@dp.callback_query(F.data == "diary_push_toggle")
async def diary_push_toggle(callback: CallbackQuery):
    lang = await get_lang(callback.from_user.id)
    user_id = callback.from_user.id
    current = await get_diary_push(user_id)
    await set_diary_push(user_id, not current)

    if not current:
        text = "🔔 Напоминание включено — буду писать каждый день в 20:00" if lang == "ru" else "🔔 Нагадування увімкнено — буду писати щодня о 20:00"
    else:
        text = "🔕 Напоминание выключено" if lang == "ru" else "🔕 Нагадування вимкнено"

    await callback.answer(text, show_alert=True)
    await show_diary(callback)


@dp.callback_query(F.data == "diary_new")
async def diary_new(callback: CallbackQuery):
    lang = await get_lang(callback.from_user.id)
    if lang == "uk":
        text = "Як ти зараз себе почуваєш?"
    else:
        text = "Как ты сейчас себя чувствуешь?"

    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="😊", callback_data="diary_mood_😊"),
                InlineKeyboardButton(text="😐", callback_data="diary_mood_😐"),
                InlineKeyboardButton(text="😔", callback_data="diary_mood_😔"),
                InlineKeyboardButton(text="😤", callback_data="diary_mood_😤"),
                InlineKeyboardButton(text="😰", callback_data="diary_mood_😰"),
            ],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="diary")],
        ])
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("diary_mood_"))
async def diary_mood_selected(callback: CallbackQuery):
    lang = await get_lang(callback.from_user.id)
    user_id = callback.from_user.id
    mood = callback.data.replace("diary_mood_", "")
    DIARY_MOOD[user_id] = mood
    DIARY_WRITING.add(user_id)

    if lang == "uk":
        text = f"Настрій: {mood}\n\nНапиши що у тебе на думці — будь-який текст 👇"
        cancel = "◀️ Скасування"
    else:
        text = f"Настроение: {mood}\n\nНапиши что у тебя на уме — любой текст 👇"
        cancel = "◀️ Отмена"

    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=cancel, callback_data="diary")]
        ])
    )
    await callback.answer()


@dp.callback_query(F.data == "diary_week")
async def diary_week(callback: CallbackQuery):
    lang = await get_lang(callback.from_user.id)
    rows = await get_mood_week(callback.from_user.id)

    if not rows:
        text = "За последние 7 дней записей нет." if lang == "ru" else "За останні 7 днів записів немає."
    else:
        MOOD_LABELS = {"😊": "Хорошо", "😐": "Нейтрально", "😔": "Грустно", "😤": "Раздражение", "😰": "Тревога"}
        MOOD_LABELS_UK = {"😊": "Добре", "😐": "Нейтрально", "😔": "Сумно", "😤": "Роздратування", "😰": "Тривога"}
        labels = MOOD_LABELS_UK if lang == "uk" else MOOD_LABELS

        title = "📊 *Настрій за тиждень:*\n\n" if lang == "uk" else "📊 *Настроение за неделю:*\n\n"
        text = title

        # Группируем по дням
        days = {}
        for r in rows:
            day = r["day"].strftime("%d.%m")
            if day not in days:
                days[day] = []
            days[day].append(r["mood"])

        for day, moods in days.items():
            mood_str = " ".join(moods)
            text += f"*{day}* — {mood_str}\n"

    await callback.message.edit_text(
        text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад" if lang == "ru" else "◀️ Назад", callback_data="diary")]
        ])
    )
    await callback.answer()


@dp.callback_query(F.data == "diary_all")
async def diary_all(callback: CallbackQuery):
    lang = await get_lang(callback.from_user.id)
    entries = await get_diary_entries(callback.from_user.id, 10)

    if not entries:
        text = "Записей пока нет." if lang == "ru" else "Записів поки немає."
        buttons = [[InlineKeyboardButton(text="◀️ Назад", callback_data="diary")]]
    else:
        text = "📋 *Твои записи:*\n\n" if lang == "ru" else "📋 *Твої записи:*\n\n"
        buttons = []
        for e in entries:
            dt = e["created_at"].strftime("%d.%m %H:%M")
            short = e["text"][:50] + "..." if len(e["text"]) > 50 else e["text"]
            text += f"{e['mood']} *{dt}*\n{short}\n\n"
            buttons.append([InlineKeyboardButton(
                text=f"🗑 Удалить {dt}" if lang == "ru" else f"🗑 Видалити {dt}",
                callback_data=f"diary_del_{e['id']}"
            )])
        buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="diary")])

    await callback.message.edit_text(
        text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("diary_del_"))
async def diary_delete(callback: CallbackQuery):
    lang = await get_lang(callback.from_user.id)
    entry_id = int(callback.data.replace("diary_del_", ""))
    await delete_diary_entry(entry_id, callback.from_user.id)
    await callback.answer("Удалено" if lang == "ru" else "Видалено")
    # Обновляем список
    await diary_all(callback)


@dp.callback_query(F.data == "quiz_start")
async def quiz_start(callback: CallbackQuery):
    lang = await get_lang(callback.from_user.id)
    QUIZ_ANSWERS[callback.from_user.id] = []
    await show_quiz_question(callback.message, callback.from_user.id, lang, 0, edit=True)
    await callback.answer()


async def show_quiz_question(message, user_id: int, lang: str, q_index: int, edit: bool = False):
    questions = QUIZ_QUESTIONS[lang]
    if q_index >= len(questions):
        return

    q = questions[q_index]
    total = len(questions)
    progress = "●" * (q_index + 1) + "○" * (total - q_index - 1)

    text = f"_{progress}_ {q_index + 1}/{total}\n\n*{q['text']}*"
    buttons = []
    for label, value in q["options"]:
        buttons.append([InlineKeyboardButton(
            text=label,
            callback_data=f"quiz_ans_{q_index}_{value}"
        )])

    markup = InlineKeyboardMarkup(inline_keyboard=buttons)
    if edit:
        await message.edit_text(text, parse_mode="Markdown", reply_markup=markup)
    else:
        await message.answer(text, parse_mode="Markdown", reply_markup=markup)


@dp.callback_query(F.data.startswith("quiz_ans_"))
async def quiz_answer(callback: CallbackQuery):
    lang = await get_lang(callback.from_user.id)
    user_id = callback.from_user.id
    parts = callback.data.replace("quiz_ans_", "").split("_", 1)
    q_index = int(parts[0])
    value = parts[1]

    if user_id not in QUIZ_ANSWERS:
        QUIZ_ANSWERS[user_id] = []

    # Сохраняем ответ
    answers = QUIZ_ANSWERS[user_id]
    if len(answers) <= q_index:
        answers.append(value)
    else:
        answers[q_index] = value

    next_index = q_index + 1

    if next_index < len(QUIZ_QUESTIONS[lang]):
        await show_quiz_question(callback.message, user_id, lang, next_index, edit=True)
    else:
        # Квиз завершён — показываем результат
        result_keys = get_quiz_result(answers)
        QUIZ_ANSWERS.pop(user_id, None)

        if lang == "uk":
            text = "🎯 *Ось твої протоколи — підібрано спеціально для тебе:*\n\n"
        else:
            text = "🎯 *Вот твои протоколы — подобраны специально для тебя:*\n\n"

        for key in result_keys:
            item = CATALOG.get(key)
            if item:
                title = item.get("title_uk", item["title"]) if lang == "uk" else item["title"]
                desc = item.get("description_uk", item["description"]) if lang == "uk" else item["description"]
                text += f"{item['emoji']} *{title}*\n_{desc}_\n💳 {item['price']} ⭐\n\n"

        await callback.message.edit_text(
            text,
            parse_mode="Markdown",
            reply_markup=workbook_list_keyboard(result_keys, back="back_main", lang=lang)
        )

    await callback.answer()


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
            [InlineKeyboardButton(text="🤝 Топ рефереров", callback_data="admin_referrers")],
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
            [InlineKeyboardButton(text="🤝 Топ рефереров", callback_data="admin_referrers")],
            [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
            [InlineKeyboardButton(text="🎁 Выдать воркбук", callback_data="admin_send_wb")],
        ])
    )
    await callback.answer()


@dp.callback_query(F.data == "admin_referrers")
async def admin_referrers(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    rows = await get_top_referrers(10)
    if not rows:
        text = "🤝 Рефералов пока нет"
    else:
        lines = []
        for r in rows:
            name = r["full_name"] or r["username"] or str(r["referrer_id"])
            lines.append(f"• {name} (@{r['username'] or '—'}) — приглашено: {r['total']}, купили: {r['purchased']}")
        text = "🤝 Топ рефереров:\n\n" + "\n".join(lines)
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")]
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

@dp.callback_query(F.data == "referral")
async def show_referral(callback: CallbackQuery):
    lang = await get_lang(callback.from_user.id)
    user_id = callback.from_user.id
    stats = await get_referral_stats(user_id)

    bot_info = await bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start=ref_{user_id}"

    purchased = stats["purchased"]
    total = stats["total"]
    discount = stats["discount"]

    if lang == "uk":
        if discount > 0:
            discount_text = f"🎁 *Твоя поточна знижка: {discount}%*\n_Застосується автоматично при наступній покупці._\n\n"
        else:
            discount_text = ""
        text = (
            f"🤝 *Реферальна програма*\n\n"
            f"Запрошуй друзів — отримуй знижки на воркбуки.\n\n"
            f"За кожного хто *купив*:\n"
            f"· 1 покупець → знижка *15%*\n"
            f"· 2 покупці → знижка *30%*\n"
            f"· 3+ покупці → знижка *50%*\n\n"
            f"{discount_text}"
            f"👥 Запрошено: *{total}*\n"
            f"💰 Купили: *{purchased}*\n\n"
            f"Твоє посилання:\n`{ref_link}`"
        )
    else:
        if discount > 0:
            discount_text = f"🎁 *Твоя текущая скидка: {discount}%*\n_Применится автоматически при следующей покупке._\n\n"
        else:
            discount_text = ""
        text = (
            f"🤝 *Реферальная программа*\n\n"
            f"Приглашай друзей — получай скидки на воркбуки.\n\n"
            f"За каждого кто *купил*:\n"
            f"· 1 покупатель → скидка *15%*\n"
            f"· 2 покупателя → скидка *30%*\n"
            f"· 3+ покупателей → скидка *50%*\n\n"
            f"{discount_text}"
            f"👥 Приглашено: *{total}*\n"
            f"💰 Купили: *{purchased}*\n\n"
            f"Твоя ссылка:\n`{ref_link}`"
        )

    await callback.message.edit_text(
        text,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад" if lang == "ru" else "◀️ Назад", callback_data="back_main")]
        ])
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("buy_"))
async def buy_workbook(callback: CallbackQuery):
    lang = await get_lang(callback.from_user.id)
    user_id = callback.from_user.id
    key = callback.data.replace("buy_", "")
    item = CATALOG.get(key)
    if not item:
        await callback.answer(T[lang]["not_found"], show_alert=True)
        return

    title = item.get("title_uk", item["title"]) if lang == "uk" else item["title"]
    desc = item.get("description_uk", item["description"]) if lang == "uk" else item["description"]
    original_price = item["price"]

    # Проверяем скидку
    discount = await get_discount(user_id)
    if discount > 0 and original_price > 1:
        final_price = max(1, int(original_price * (1 - discount / 100)))
        title_with_discount = f"{title} (скидка {discount}%)" if lang == "ru" else f"{title} (знижка {discount}%)"
    else:
        final_price = original_price
        title_with_discount = title

    await callback.answer()
    await bot.send_invoice(
        chat_id=user_id,
        title=title_with_discount,
        description=desc,
        payload=f"workbook_{key}",
        currency="XTR",
        prices=[LabeledPrice(label=title_with_discount, amount=final_price)],
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

            # Используем скидку если была
            discount = await get_discount(user.id)
            if discount > 0 and item["price"] > 1:
                await use_discount(user.id)

            # Начисляем бонус рефереру
            ref_result = await on_referral_purchase(user.id)
            if ref_result:
                referrer_id = ref_result["referrer_id"]
                new_discount = ref_result["discount"]
                count = ref_result["referrals_count"]
                try:
                    await bot.send_message(
                        referrer_id,
                        f"🎉 По твоей реферальной ссылке купили воркбук!\n\n"
                        f"💰 Купивших: {count}\n"
                        f"🎁 Твоя скидка на следующую покупку: *{new_discount}%*\n\n"
                        f"_Скидка применится автоматически._",
                        parse_mode="Markdown"
                    )
                except Exception:
                    pass

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
    lang = await get_lang(user_id)

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

    # Запись в дневник
    if user_id in DIARY_WRITING:
        DIARY_WRITING.discard(user_id)
        mood = DIARY_MOOD.pop(user_id, "😐")
        await save_diary_entry(user_id, mood, message.text)
        if lang == "uk":
            await message.answer(
                f"📔 Записано {mood}\n\n_Запис збережено._",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="📔 Відкрити щоденник", callback_data="diary")],
                    [InlineKeyboardButton(text="🏠 Головне меню", callback_data="back_main")],
                ])
            )
        else:
            await message.answer(
                f"📔 Записано {mood}\n\n_Запись сохранена._",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="📔 Открыть дневник", callback_data="diary")],
                    [InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_main")],
                ])
            )
        return

    # Отчёт по стрику
    if user_id not in WAITING_REPORT:
        return
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

DIARY_PROMPTS = {
    "ru": [
        "📔 Как прошёл день? Запиши мысли — это займёт 2 минуты.",
        "📔 Вечер — хорошее время остановиться и записать как ты себя чувствуешь.",
        "📔 Что сегодня было важным? Запиши в дневник.",
        "📔 Один момент из сегодняшнего дня — опиши его в дневнике.",
        "📔 Как твоё настроение сейчас? Открой дневник и запиши.",
    ],
    "uk": [
        "📔 Як пройшов день? Запиши думки — це займе 2 хвилини.",
        "📔 Вечір — гарний час зупинитися і записати як ти себе почуваєш.",
        "📔 Що сьогодні було важливим? Запиши у щоденник.",
        "📔 Один момент із сьогоднішнього дня — опиши його у щоденнику.",
        "📔 Який твій настрій зараз? Відкрий щоденник і запиши.",
    ],
}

STREAK_WARNINGS = {
    "ru": [
        "⚠️ Твой стрик под угрозой! Ты не заходил уже почти сутки.\n\nОсталось совсем немного — не дай стрику оборваться 🔥",
        "🔥 Стрик горит! Зайди и отправь отчёт — не теряй то, что заработал.",
        "⏰ Почти 24 часа без отчёта. Твой стрик ещё жив — успей сохранить!",
    ],
    "uk": [
        "⚠️ Твій стрік під загрозою! Ти не заходив вже майже добу.\n\nЗалишилося зовсім небагато — не дай стріку перерватися 🔥",
        "🔥 Стрік горить! Зайди і надішли звіт — не втрачай те, що заробив.",
        "⏰ Майже 24 години без звіту. Твій стрік ще живий — встигни зберегти!",
    ],
}


async def scheduler():
    """Фоновая задача — проверяет время и отправляет уведомления."""
    import datetime
    while True:
        now = datetime.datetime.now()

        # Пуш дневника в 20:00
        if now.hour == 20 and now.minute == 0:
            users = await get_diary_push_users()
            for u in users:
                try:
                    lang = u["lang"] or "ru"
                    prompt = random.choice(DIARY_PROMPTS[lang])
                    await bot.send_message(
                        u["user_id"],
                        prompt,
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(
                                text="📔 Открыть дневник" if lang == "ru" else "📔 Відкрити щоденник",
                                callback_data="diary"
                            )]
                        ])
                    )
                except Exception:
                    pass
            await asyncio.sleep(60)  # Ждём минуту чтобы не отправить дважды

        # Проверка стриков под угрозой — каждый час
        if now.minute == 0:
            users = await get_streak_at_risk_users()
            for u in users:
                try:
                    lang = u["lang"] or "ru"
                    warning = random.choice(STREAK_WARNINGS[lang])
                    await bot.send_message(
                        u["user_id"],
                        warning,
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(
                                text="🔥 Отправить отчёт" if lang == "ru" else "🔥 Надіслати звіт",
                                callback_data="streak"
                            )]
                        ])
                    )
                except Exception:
                    pass

        await asyncio.sleep(60)  # Проверяем каждую минуту


async def main():
    logger.info("BZH Academy Bot запущен")
    await init_db()
    await init_diary_table()
    await init_notifications_table()
    logger.info("База данных инициализирована")
    asyncio.create_task(scheduler())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

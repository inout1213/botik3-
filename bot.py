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
from streak import approve_checkin, reject_checkin, streak_status, progress_bar, STREAK_GOAL, WAITING_REPORT, set_pending, get_user

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

WELCOME_TEXTS = [
    "Ты не сломан. Тебя просто никто не учил работать с собой.",
    "Все инструменты уже существуют. Просто никто не дал их тебе.",
    "Проблема не в силе воли. Проблема в том, что мозг работает не так, как ты думаешь.",
    "Ты уже пробовал. Не сработало. Потому что метод был не тот.",
    "Это не слабость. Это просто отсутствие нужных инструментов.",
    "Большинство проблем в голове решаемы. Просто никто не объяснил как.",
    "Не мотивация. Не сила воли. Просто правильная система.",
]


# ─── Клавиатуры ────────────────────────────────────────────────────────────────

def main_menu() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="Не могу начать", callback_data="search_не могу начать")],
        [InlineKeyboardButton(text="Тревога и страх", callback_data="search_тревога")],
        [InlineKeyboardButton(text="Выгорел, нет сил", callback_data="search_выгорел")],
        [InlineKeyboardButton(text="Не уверен в себе", callback_data="search_не уверен")],
        [InlineKeyboardButton(text="Конфликты и люди", callback_data="search_конфликт")],
        [InlineKeyboardButton(text="Все протоколы →", callback_data="catalog")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def catalog_keyboard() -> InlineKeyboardMarkup:
    """Главный экран каталога — выбор режима."""
    buttons = [
        [InlineKeyboardButton(text="🔥 Популярное", callback_data="cat_popular")],
        [InlineKeyboardButton(text="🧠 По категориям", callback_data="cat_categories")],
        [InlineKeyboardButton(text="🔍 Поиск по проблеме", callback_data="cat_search")],
        [InlineKeyboardButton(text="📋 Все воркбуки", callback_data="cat_all")],
        [InlineKeyboardButton(text="🔑 Библиотека — все сразу", callback_data="subscription")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_main")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def workbook_list_keyboard(keys: list, back: str = "catalog") -> InlineKeyboardMarkup:
    """Список конкретных воркбуков."""
    buttons = []
    for key in keys:
        item = CATALOG.get(key)
        if item:
            buttons.append([InlineKeyboardButton(
                text=f"{item['emoji']} {item['title']} — {item['price']} ⭐",
                callback_data=f"buy_{key}"
            )])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=back)])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def categories_keyboard() -> InlineKeyboardMarkup:
    buttons = []
    for key, cat in CATEGORIES.items():
        buttons.append([InlineKeyboardButton(
            text=f"{cat['emoji']} {cat['title']}",
            callback_data=f"category_{key}"
        )])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="catalog")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def search_keyboard() -> InlineKeyboardMarkup:
    problems = list(PROBLEM_SEARCH.keys())
    buttons = []
    for p in problems:
        buttons.append([InlineKeyboardButton(text=f"🔍 {p}", callback_data=f"search_{p}")])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="catalog")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def back_button() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад в каталог", callback_data="catalog")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_main")],
    ])


# ─── Старт ─────────────────────────────────────────────────────────────────────

@dp.message(CommandStart())
async def cmd_start(message: Message):
    phrase = random.choice(WELCOME_TEXTS)
    await message.answer(
        f"{phrase}\n\n"
        f"_BZH Academy — протоколы для тех, кто хочет разобраться._\n\n"
        f"Что мешает прямо сейчас?",
        parse_mode="Markdown",
        reply_markup=main_menu()
    )


@dp.message(Command("admin"))
async def cmd_admin(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    await message.answer(
        "🔧 *Панель администратора*\n\n"
        "Команды:\n"
        "/stats — статистика продаж\n"
        "/broadcast — рассылка (в разработке)",
        parse_mode="Markdown"
    )


# ─── Навигация ──────────────────────────────────────────────────────────────────

@dp.callback_query(F.data == "back_main")
async def back_main(callback: CallbackQuery):
    phrase = random.choice(WELCOME_TEXTS)
    await callback.message.edit_text(
        f"{phrase}\n\n"
        f"_BZH Academy — протоколы для тех, кто хочет разобраться._\n\n"
        f"Что мешает прямо сейчас?",
        parse_mode="Markdown",
        reply_markup=main_menu()
    )
    await callback.answer()


@dp.callback_query(F.data == "catalog")
async def show_catalog(callback: CallbackQuery):
    await callback.message.edit_text(
        "📖 *Воркбуки BZH Academy*\n\n"
        "Как хочешь найти воркбук?",
        parse_mode="Markdown",
        reply_markup=catalog_keyboard()
    )
    await callback.answer()


@dp.callback_query(F.data == "cat_popular")
async def show_popular(callback: CallbackQuery):
    text = "🔥 *Популярные воркбуки*\n\n"
    for key in POPULAR:
        item = CATALOG.get(key)
        if item:
            text += f"{item['emoji']} *{item['title']}*\n_{item['description']}_\n💳 {item['price']} ⭐\n\n"
    await callback.message.edit_text(
        text, parse_mode="Markdown",
        reply_markup=workbook_list_keyboard(POPULAR, back="catalog")
    )
    await callback.answer()


@dp.callback_query(F.data == "cat_categories")
async def show_categories(callback: CallbackQuery):
    await callback.message.edit_text(
        "🧠 *Выбери категорию:*",
        parse_mode="Markdown",
        reply_markup=categories_keyboard()
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("category_"))
async def show_category(callback: CallbackQuery):
    key = callback.data.replace("category_", "")
    cat = CATEGORIES.get(key)
    keys = CATEGORY_MAP.get(key, [])
    if not cat or not keys:
        await callback.answer("Категория не найдена.", show_alert=True)
        return
    text = f"{cat['emoji']} *{cat['title']}*\n\n"
    for k in keys:
        item = CATALOG.get(k)
        if item:
            text += f"{item['emoji']} *{item['title']}*\n_{item['description']}_\n💳 {item['price']} ⭐\n\n"
    await callback.message.edit_text(
        text, parse_mode="Markdown",
        reply_markup=workbook_list_keyboard(keys, back="cat_categories")
    )
    await callback.answer()


@dp.callback_query(F.data == "cat_search")
async def show_search(callback: CallbackQuery):
    await callback.message.edit_text(
        "🔍 *Что тебя беспокоит?*\n\n_Выбери — подберём воркбук._",
        parse_mode="Markdown",
        reply_markup=search_keyboard()
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("search_"))
async def show_search_result(callback: CallbackQuery):
    problem = callback.data.replace("search_", "")
    keys = PROBLEM_SEARCH.get(problem, [])
    if not keys:
        await callback.answer("Ничего не найдено.", show_alert=True)
        return
    text = f"🔍 *«{problem}»*\n\nВот что поможет:\n\n"
    for k in keys:
        item = CATALOG.get(k)
        if item:
            text += f"{item['emoji']} *{item['title']}*\n_{item['description']}_\n💳 {item['price']} ⭐\n\n"
    await callback.message.edit_text(
        text, parse_mode="Markdown",
        reply_markup=workbook_list_keyboard(keys, back="cat_search")
    )
    await callback.answer()


@dp.callback_query(F.data == "cat_all")
async def show_all(callback: CallbackQuery):
    text = "📋 *Все воркбуки BZH Academy*\n\n"
    for item in CATALOG.values():
        text += f"{item['emoji']} *{item['title']}*\n_{item['description']}_\n💳 {item['price']} ⭐\n\n"
    await callback.message.edit_text(
        text, parse_mode="Markdown",
        reply_markup=workbook_list_keyboard(list(CATALOG.keys()), back="catalog")
    )
    await callback.answer()


@dp.callback_query(F.data == "about")
async def show_about(callback: CallbackQuery):
    await callback.message.edit_text(
        "💡 *О BZH Academy*\n\n"
        "Мы делаем воркбуки — не курсы, не лекции.\n"
        "Только практика. Только инструменты.\n\n"
        "Каждый воркбук — это:\n"
        "· 8–10 блоков с упражнениями\n"
        "· КПТ-инструменты и чек-листы\n"
        "· Директивы для действия\n"
        "· PDF сразу после оплаты\n\n"
        "_Открываешь — и сразу работаешь._",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📖 Смотреть воркбуки", callback_data="catalog")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="back_main")],
        ])
    )
    await callback.answer()


# ─── Покупка воркбука ───────────────────────────────────────────────────────────

@dp.callback_query(F.data.startswith("buy_"))
async def buy_workbook(callback: CallbackQuery):
    key = callback.data.replace("buy_", "")
    item = CATALOG.get(key)
    if not item:
        await callback.answer("Воркбук не найден.", show_alert=True)
        return

    await callback.answer()
    await bot.send_invoice(
        chat_id=callback.from_user.id,
        title=item["title"],
        description=item["description"],
        payload=f"workbook_{key}",
        currency="XTR",  # Telegram Stars
        prices=[LabeledPrice(label=item["title"], amount=item["price"])],
        provider_token="",  # пустой для Stars
    )


# ─── Подписка ───────────────────────────────────────────────────────────────────

@dp.callback_query(F.data == "subscription")
async def show_subscription(callback: CallbackQuery):
    await callback.message.edit_text(
        "🔑 *Библиотека BZH Academy*\n\n"
        "Все воркбуки сразу + все новые за период.\n\n"
        "┌ 📦 *2 месяца* — 1 500 ⭐\n"
        "└ 🏆 *Год* — 2 500 ⭐ · выгоднее в 3×\n\n"
        "_Оплатил — получил всё мгновенно._",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📦 2 месяца — 1 500 ⭐", callback_data="buy_sub_2m")],
            [InlineKeyboardButton(text="🏆 Год — 2 500 ⭐ · лучший выбор", callback_data="buy_sub_year")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="catalog")],
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


# ─── Обработка оплаты ───────────────────────────────────────────────────────────

@dp.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    # Всегда подтверждаем — Stars не требует верификации
    await query.answer(ok=True)


@dp.message(F.successful_payment)
async def successful_payment(message: Message):
    payload = message.successful_payment.invoice_payload
    user = message.from_user

    logger.info(f"Оплата: user={user.id} ({user.username}), payload={payload}")

    # Уведомление администратору
    await bot.send_message(
        ADMIN_ID,
        f"💰 *Новая оплата!*\n\n"
        f"Пользователь: {user.full_name} (@{user.username})\n"
        f"ID: `{user.id}`\n"
        f"Товар: `{payload}`\n"
        f"Сумма: {message.successful_payment.total_amount} ⭐",
        parse_mode="Markdown"
    )

    # Доставка PDF
    if payload.startswith("workbook_"):
        key = payload.replace("workbook_", "")
        item = CATALOG.get(key)
        if item and item.get("pdf_path"):
            await message.answer(
                f"✅ *Оплата прошла!*\n\n"
                f"Держи свой воркбук 👇\n"
                f"_Открывай и приступай прямо сейчас._",
                parse_mode="Markdown"
            )
            try:
                with open(item["pdf_path"], "rb") as pdf:
                    await bot.send_document(
                        message.chat.id,
                        document=BufferedInputFile(pdf.read(), filename=f"{item['title']}.pdf"),
                        caption=f"📖 {item['title']} | BZH Academy"
                    )
            except FileNotFoundError:
                logger.error(f"PDF не найден: {item['pdf_path']}")
                await message.answer(
                    "PDF временно недоступен. Мы свяжемся с тобой в течение нескольких минут."
                )
        else:
            await message.answer("Воркбук временно недоступен. Напишем тебе лично.")

    elif payload in ("subscription_2m", "subscription_year"):
        label = "2 месяца" if payload == "subscription_2m" else "год"
        await message.answer(
            f"✅ *Библиотека BZH Academy — {label}*\n\n"
            f"Все воркбуки уже здесь 👇\n"
            f"_Сохрани их — и возвращайся когда нужно._",
            parse_mode="Markdown"
        )
        # Отправляем все воркбуки
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

DAILY_TASKS = [
    "Запиши одну ситуацию, где ты избегал действия из-за тревоги. Что ты сделал вместо?",
    "Выдели 10 минут и проведи декатастрофизацию своей главной тревоги прямо сейчас.",
    "Сделай один маленький шаг к тому, что откладывал. Любой. Запиши результат.",
    "Определи свою главную задачу на сегодня по правилу 1-3-5. Выполни хотя бы «1».",
    "Проведи 15 минут без телефона. Только бумага и мысли. Запиши что пришло в голову.",
    "Вспомни ситуацию из прошлого, которая началась с неопределённости и привела к росту.",
    "Сделай дыхание 4-7-8 три раза подряд. Запиши как изменилось состояние.",
]


def streak_keyboard(pending: bool = False) -> InlineKeyboardMarkup:
    buttons = []
    if not pending:
        buttons.append([InlineKeyboardButton(text="✍️ Отправить отчёт", callback_data="send_report")])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="back_main")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def admin_approve_keyboard(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Засчитать", callback_data=f"approve_{user_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_{user_id}"),
        ]
    ])


def reward_keyboard() -> InlineKeyboardMarkup:
    buttons = []
    for key, item in CATALOG.items():
        buttons.append([InlineKeyboardButton(
            text=f"{item['emoji']} {item['title']}",
            callback_data=f"reward_{key}"
        )])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@dp.callback_query(F.data == "streak")
async def show_streak(callback: CallbackQuery):
    import random
    user_id = callback.from_user.id
    user = get_user(user_id)
    today_str = str(__import__("datetime").date.today())
    done_today = user["last_date"] == today_str
    pending = user.get("pending_approval", False)

    status = streak_status(user_id)
    task = random.choice(DAILY_TASKS)

    text = status + "\n\n"
    if done_today:
        text += "✅ *Сегодня уже засчитано!*\n_Возвращайся завтра._"
    elif pending:
        text += "⏳ *Отчёт на проверке.*\n_Ожидай подтверждения._"
    else:
        text += f"📌 *Задание на сегодня:*\n_{task}_"
        WAITING_REPORT[user_id] = task

    await callback.message.edit_text(
        text,
        parse_mode="Markdown",
        reply_markup=streak_keyboard(pending=pending or done_today)
    )
    await callback.answer()


@dp.callback_query(F.data == "send_report")
async def ask_for_report(callback: CallbackQuery):
    user_id = callback.from_user.id
    await callback.message.edit_text(
        "✍️ *Напиши свой отчёт*\n\n"
        "_Расскажи как выполнил задание — что сделал, что почувствовал, что понял._\n\n"
        "Просто отправь текст в этот чат 👇",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена", callback_data="streak")]
        ])
    )
    if user_id not in WAITING_REPORT:
        WAITING_REPORT[user_id] = "задание дня"
    await callback.answer()


@dp.message(F.text & ~F.text.startswith("/"))
async def handle_report(message: Message):
    user_id = message.from_user.id
    if user_id not in WAITING_REPORT:
        return

    task = WAITING_REPORT.pop(user_id)
    user = message.from_user
    set_pending(user_id)

    await message.answer(
        "⏳ *Отчёт отправлен на проверку.*\n\n"
        "_Как только подтвердят — стрик засчитается_ 👍",
        parse_mode="Markdown"
    )

    await bot.send_message(
        ADMIN_ID,
        f"📋 *Новый отчёт*\n\n"
        f"👤 {user.full_name} (@{user.username})\n"
        f"ID: `{user_id}`\n\n"
        f"📌 *Задание:*\n_{task}_\n\n"
        f"✍️ *Отчёт:*\n{message.text}",
        parse_mode="Markdown",
        reply_markup=admin_approve_keyboard(user_id)
    )


@dp.callback_query(F.data.startswith("approve_"))
async def approve_report(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет доступа.", show_alert=True)
        return

    user_id = int(callback.data.replace("approve_", ""))
    result = approve_checkin(user_id)
    streak = result["streak"]

    if result["reward"]:
        await bot.send_message(
            user_id,
            f"🎉 *{streak} дней подряд — ты сделал это!*\n\n"
            f"{'🔥' * STREAK_GOAL}\n\n"
            f"Выбери воркбук, который получишь бесплатно 👇",
            parse_mode="Markdown",
            reply_markup=reward_keyboard()
        )
    else:
        bar = progress_bar(streak)
        remaining = STREAK_GOAL - (streak % STREAK_GOAL)
        await bot.send_message(
            user_id,
            f"✅ *Стрик засчитан! День {streak}*\n\n"
            f"{bar}\n\n"
            f"_До бесплатного воркбука: {remaining} {'день' if remaining == 1 else 'дня' if 2 <= remaining <= 4 else 'дней'}_",
            parse_mode="Markdown"
        )

    await callback.message.edit_text(
        callback.message.text + "\n\n✅ *Засчитано*",
        parse_mode="Markdown"
    )
    await callback.answer("Стрик засчитан!")


@dp.callback_query(F.data.startswith("reject_"))
async def reject_report(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет доступа.", show_alert=True)
        return

    user_id = int(callback.data.replace("reject_", ""))
    reject_checkin(user_id)

    await bot.send_message(
        user_id,
        "❌ *Отчёт не принят*\n\n"
        "_Попробуй выполнить задание глубже и отправь новый отчёт._",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔥 Мой стрик", callback_data="streak")]
        ])
    )

    await callback.message.edit_text(
        callback.message.text + "\n\n❌ *Отклонено*",
        parse_mode="Markdown"
    )
    await callback.answer("Отчёт отклонён.")


@dp.callback_query(F.data.startswith("reward_"))
async def send_reward(callback: CallbackQuery):
    key = callback.data.replace("reward_", "")
    item = CATALOG.get(key)
    user = callback.from_user

    if not item:
        await callback.answer("Воркбук не найден.", show_alert=True)
        return

    await callback.message.edit_text(
        f"🎁 *Твоя награда — {item['title']}*\n\n_Заслужил. Держи!_",
        parse_mode="Markdown"
    )

    try:
        with open(item["pdf_path"], "rb") as pdf:
            await bot.send_document(
                callback.from_user.id,
                document=BufferedInputFile(pdf.read(), filename=f"{item['title']}.pdf"),
                caption=f"🎁 Награда за стрик · {item['title']} | BZH Academy"
            )
    except FileNotFoundError:
        await callback.message.answer("PDF временно недоступен. Напишем тебе лично.")

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
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

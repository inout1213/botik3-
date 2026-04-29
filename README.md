# BZH Academy Bot — Инструкция по запуску

## Что нужно для старта

- Python 3.10+
- Аккаунт в Telegram
- Сервер или компьютер (можно запустить локально для теста)

---

## Шаг 1 — Создай бота

1. Напиши @BotFather в Telegram
2. Отправь `/newbot`
3. Придумай имя и username бота
4. Скопируй токен вида `7123456789:AAF...`
5. Вставь токен в `config.py` → `BOT_TOKEN`

---

## Шаг 2 — Включи Telegram Stars

1. В @BotFather напиши `/mybots`
2. Выбери своего бота
3. `Bot Settings` → `Payments`
4. Выбери **Telegram Stars** — он встроен, ничего подключать не нужно

---

## Шаг 3 — Узнай свой Telegram ID

1. Напиши @userinfobot
2. Скопируй свой `id` (число)
3. Вставь в `config.py` → `ADMIN_ID`

---

## Шаг 4 — Добавь PDF-файлы

1. Создай папку `pdfs/` рядом с `bot.py`
2. Положи туда свои PDF-воркбуки
3. Пропиши путь в `config.py` → `pdf_path`

Структура папок:
```
bzh_bot/
├── bot.py
├── config.py
├── requirements.txt
└── pdfs/
    └── workbook_uncertainty.pdf
```

---

## Шаг 5 — Установи зависимости

```bash
pip install aiogram==3.13.0
```

Или создай `requirements.txt`:
```
aiogram==3.13.0
```
и установи:
```bash
pip install -r requirements.txt
```

---

## Шаг 6 — Запусти бота

```bash
python bot.py
```

Бот запустится и начнёт принимать сообщения.
В терминале увидишь: `BZH Academy Bot запущен`

---

## Как добавить новый воркбук

В `config.py` добавь в словарь `CATALOG`:

```python
"anxiety": {
    "emoji": "🧘",
    "title": "Протокол: Тревога",
    "description": "Инструменты для работы с тревогой.",
    "price": 250,
    "pdf_path": "pdfs/workbook_anxiety.pdf",
},
```

---

## Цены в Telegram Stars

| Stars | Примерно в EUR |
|-------|----------------|
| 100 ⭐ | ~1.3 €         |
| 250 ⭐ | ~3.3 €         |
| 500 ⭐ | ~6.5 €         |
| 600 ⭐ | ~7.8 €         |

Курс может меняться. Проверяй в @PremiumBot.

---

## Где держать бота постоянно

Для постоянной работы нужен сервер. Дешёвые варианты:
- **Railway.app** — бесплатный тариф для старта
- **Render.com** — бесплатный тариф
- **VPS** (Hetzner, DigitalOcean) — ~5€/месяц

---

## Вопросы

Если что-то не работает — напиши мне, разберёмся по шагам.

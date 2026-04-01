import os
from dotenv import load_dotenv
from datetime import datetime

from pymongo import MongoClient
import telebot

load_dotenv()


def _clean_str(value):
    return str(value or '').strip()


def _clean_telegram_chat_ids(value):
    if not isinstance(value, list):
        return []
    out = []
    for item in value:
        cleaned = _clean_str(item)
        if cleaned and cleaned not in out:
            out.append(cleaned)
    return out


TELEGRAM_BOT_TOKEN = _clean_str(os.environ.get('TELEGRAM_BOT_TOKEN'))
if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError('TELEGRAM_BOT_TOKEN is not configured')

MONGODB_URI = _clean_str(os.environ.get('MONGODB_URI')) or 'mongodb+srv://tsbgalcontract:mymongodb26@cluster0.kppkt.mongodb.net/test'
MONGODB_DB_NAME = _clean_str(os.environ.get('MONGODB_DB_NAME')) or 'memoria_test'

mongo_client = MongoClient(MONGODB_URI)
db = mongo_client[MONGODB_DB_NAME]
churches_collection = db['churches']

bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)
awaiting_code_chat_ids = set()


def _ask_for_code(message):
    awaiting_code_chat_ids.add(int(message.chat.id))
    bot.reply_to(message, 'Надішліть код для авторизації в боті.')


@bot.message_handler(commands=['start', 'edit'])
def handle_start_or_edit(message):
    _ask_for_code(message)


@bot.message_handler(content_types=['text'])
def handle_text(message):
    chat_id_num = int(message.chat.id)
    text = _clean_str(message.text)

    if text.startswith('/') and text.lower() not in {'/start', '/edit'}:
        return

    if chat_id_num not in awaiting_code_chat_ids:
        bot.reply_to(message, 'Для підключення церкви спочатку надішліть /start або /edit.')
        return

    code = text.upper()
    if not code:
        bot.reply_to(message, 'Код порожній. Надішліть дійсний код для авторизації.')
        return

    church = churches_collection.find_one({'botCode': code})
    if not church:
        bot.reply_to(message, 'Код не знайдено. Перевірте код і спробуйте ще раз.')
        return

    chat_id = str(chat_id_num)
    telegram_chat_ids = _clean_telegram_chat_ids(church.get('telegramChatIds'))
    legacy_chat_id = _clean_str(church.get('telegramChatId'))
    if legacy_chat_id and legacy_chat_id not in telegram_chat_ids:
        telegram_chat_ids.append(legacy_chat_id)
    if chat_id not in telegram_chat_ids:
        telegram_chat_ids.append(chat_id)

    primary_chat_id = telegram_chat_ids[0] if telegram_chat_ids else chat_id

    churches_collection.update_one(
        {'_id': church.get('_id')},
        {'$set': {
            'telegramChatIds': telegram_chat_ids,
            'telegramChatId': primary_chat_id,
            'updatedAt': datetime.utcnow(),
        }}
    )

    awaiting_code_chat_ids.discard(chat_id_num)
    church_name = _clean_str(church.get('name')) or 'церкву'
    bot.reply_to(message, f'Готово! Telegram успішно підключено до {church_name}.')


if __name__ == '__main__':
    bot.infinity_polling(skip_pending=True)

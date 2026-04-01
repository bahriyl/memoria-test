import os
from dotenv import load_dotenv
from datetime import datetime
from telebot.apihelper import ApiTelegramException

from pymongo import MongoClient
import telebot

from application import _admin_note_payments_confirm_by_token

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


def _parse_payment_confirmation_callback(data):
    raw = _clean_str(data)
    parts = raw.split(':')
    if len(parts) != 3 or parts[0] != 'payconf':
        raise ValueError('Невірний формат підтвердження.')
    token = _clean_str(parts[1])
    answer = _clean_str(parts[2]).lower()
    if not token:
        raise ValueError('Токен підтвердження відсутній.')
    if answer not in {'yes', 'no'}:
        raise ValueError('Невірна відповідь підтвердження.')
    return token, answer


def _answer_text(answer):
    return 'Підтверджено.' if answer == 'yes' else 'Відхилено.'


@bot.callback_query_handler(func=lambda call: _clean_str(getattr(call, 'data', '')).startswith('payconf:'))
def handle_payment_confirmation_callback(call):
    callback_id = getattr(call, 'id', '')
    data = getattr(call, 'data', '')
    message = getattr(call, 'message', None)
    try:
        token, answer = _parse_payment_confirmation_callback(data)
        result = _admin_note_payments_confirm_by_token(token, answer)
        bot.answer_callback_query(callback_id, text=_answer_text(result.get('answer')), show_alert=False)
        if message is not None:
            bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=message.message_id,
                text=result.get('message_text') or '',
                reply_markup=None,
            )
    except ValueError as exc:
        bot.answer_callback_query(callback_id, text=str(exc), show_alert=False)
    except LookupError as exc:
        bot.answer_callback_query(callback_id, text=str(exc), show_alert=False)
    except ApiTelegramException:
        # Message could already be edited or unavailable; callback has already been answered.
        pass
    except Exception:
        bot.answer_callback_query(callback_id, text='Не вдалося обробити підтвердження.', show_alert=False)


if __name__ == '__main__':
    bot.infinity_polling(skip_pending=True)

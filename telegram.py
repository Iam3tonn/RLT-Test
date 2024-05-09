import logging
from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from main import aggregate_payments

API_TOKEN = '6999901168:AAFZe8KMBwFkIVJus5_QIerv1RPH-Ghve9A'

# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Инициализация бота и диспетчера
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)
dp.middleware.setup(LoggingMiddleware())

@dp.message_handler(commands=['start', 'help'])
async def send_welcome(message: types.Message):
    await message.answer("Привет! Отправьте мне данные в формате JSON для агрегации. Пример:\n"
                         "{\n"
                         "   \"dt_from\": \"2022-09-01T00:00:00\",\n"
                         "   \"dt_upto\": \"2022-12-31T23:59:00\",\n"
                         "   \"group_type\": \"month\"\n"
                         "}")

@dp.message_handler()
async def process_json(message: types.Message):
    try:
        # Принимаем и обрабатываем JSON данные
        data = eval(message.text)
        dt_from = data['dt_from']
        dt_upto = data['dt_upto']
        group_type = data['group_type']
        
        # Вызов функции агрегации
        result = aggregate_payments(dt_from, dt_upto, group_type)
        await message.answer(f"Результаты агрегации: {result}")
    except (SyntaxError, KeyError, ValueError) as e:
        await message.reply(f"Ошибка в данных: {e}\n"
                            f"Убедитесь, что вы отправили правильный JSON формат.")
    except Exception as e:
        await message.reply(f"Неизвестная ошибка: {e}")

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)

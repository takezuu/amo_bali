from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram import Bot, Dispatcher, executor, types
from pw import TELEGRAM_TK
from paths import my_f

API_TOKEN = TELEGRAM_TK

# создаем бота
storage = MemoryStorage()
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot, storage=storage)

@dp.message_handler(commands=['Report'])
async def send_report(message: types.Message):

    file_errors = open(my_f + 'result_yar.txt', 'r', encoding='utf-8')
    report = file_errors.readlines()

    await bot.send_message(chat_id=message.chat.id, text=f"Ярик говорит кол-во ошибок на {report[-1]}",
                            protect_content=True)
    file_errors.close()


if __name__ == '__main__':
    # держит бота в онлайне
    executor.start_polling(dp, skip_updates=True)
import logging
import os
import threading
from datetime import date

import psycopg2 as pg
import re
import requests as req

from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.dispatcher import FSMContext

from aiogram.types import ReplyKeyboardRemove, \
    ReplyKeyboardMarkup, KeyboardButton, \
    BotCommand, BotCommandScopeDefault,BotCommandScopeChat

import requests
import json

conn=pg.connect (user='postgres', password='postgres', host='localhost', port='8888', database='RGR')
cursor=conn.cursor()

bot_token=os.getenv('TELEGRAM_BOT_TOKEN')
bot = Bot(token=bot_token)
dp=Dispatcher(bot, storage=MemoryStorage())


api_key = "FX4QCTKHVNOCPT4H"
url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=AAPL&apikey={api_key}"
response = requests.get(url)
data = json.loads(response.text)
#print(data)

commands = [
    types.BotCommand(command='/start', description='start'),
    types.BotCommand(command='/add_stock', description='Добавить ценную бумагу к портфелю'),
    types.BotCommand(command='/show_indicators', description='Результаты')
]

button_add = KeyboardButton('/add_stock')
button_show = KeyboardButton('/show_indicators')

buttons = ReplyKeyboardMarkup(resize_keyboard=True).add(button_add, button_show)

stocks = []
symbols = []

class Form(StatesGroup):
    add_stock = State()
    save_rater = State()
    save_stock = State()
    res_stock = State()

@dp.message_handler(commands=['start'])
async def start_command(message: types.Message):
    await bot.set_my_commands(commands, scope=BotCommandScopeDefault())
    await message.reply("Привет! Я бот для поиска оптимального момента покупки акций, чтобы добавить акцию /add_stock, чтобы посмотреть"
                        "результаты /show_indicators.", reply_markup=buttons)

# добавление акции для пользователя 
@dp.message_handler(commands=['add_stock'])
async def cmd_add_stock(message: types.Message):
    await message.reply ("Введите имя ценной бумаги:")
    await Form.add_stock.set()

def load_user_stocks(user):
    cursor.execute("""Select stock_name from user_stocks where user_id=%s""",(user,))
    rows = cursor.fetchall()
    return [row[0] for row in rows]

def save_user_stock(user_id, stock_name):
    cursor.execute("""INSERT INTO user_stocks (user_id, stock_name) 
                      VALUES (%s, REPLACE(REPLACE(CAST(%s AS text), '{', ''), '}', ''))""", (user_id, stock_name))
    conn.commit()

@dp.message_handler(state=Form.add_stock)
async def process_save_name(message: types.Message, state: FSMContext):
    name = message.text
    user = message.from_user.id
    user_stocks = load_user_stocks(user)
    if name in user_stocks:
        await message.reply("Данная бумага уже добавлена, добавьте другую")
        await Form.add_stock.set()
    else:
        stocks.append(name)
        save_user_stock(user, stocks)
        await message.reply("Ценная бумага добавлена к отслеживаемым")
        await state.finish()


@dp.message_handler(commands=['show_indicators'])
async def show_indicators(message: types.Message):
    user_id = message.from_user.id
    today = date.today()
    cursor = conn.cursor()
    curr = cursor.execute("Select stock_name, min_period, max_period, result_min, result_max from calculate where date =%s", (today,))
    symbol = cursor.fetchall()
    conn.commit()
    for row in symbol:
        stock_name, min_period, max_period, result_min, result_max = row
        await message.answer(f" Сегодня :'{today}'. Акцию '{stock_name}' рекомендал бы продавать в период: '{max_period}' по цене '{result_max}'")
        await message.answer(f" Сегодня :'{today}'. Акцию '{stock_name}' рекомендал бы покупать в период: {min_period} по цене {result_min}")


def calculate(symbol):
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={symbol}&apikey=FX4QCTKHVNOCPT4H"
    response = requests.get(url)
    data = json.loads(response.text)
    period = 30  # период в днях
    moving_average_period = 5  # период скользящего среднего
    closing_prices = []
    if "Time Series (Daily)" not in data:
        raise Exception(f"Указанной бумаги {symbol} не существует")

    for date in list(data["Time Series (Daily)"].keys())[:period]:
        closing_prices.append({"price": float(data["Time Series (Daily)"][date]["4. close"]), "date": date})

    averages = []
    for period in range(0, (int(len(closing_prices) / moving_average_period)) - 1):
        closing_prices_period = closing_prices[(period * moving_average_period):((period + 1) * moving_average_period)]
        prices = list(map(lambda x: x["price"], closing_prices_period))
        dates = list(map(lambda x: x["date"], closing_prices_period))
        moving_average = sum(prices) / moving_average_period
        averages.append({"period": f"{dates[0]}/{dates[-1]}", "average": moving_average})

    min = 1000
    max = 0
    max_period = {}
    min_period = {}
    for period in averages:
        if period["average"] > max:
            max = period["average"]
            max_period = period

        if period["average"] < min:
            min = period["average"]
            min_period = period

    return (min_period, max_period)


def task():
    WAIT_TIME_SECONDS = 60 * 60 * 24
    ticker = threading.Event()
    while not ticker.wait(WAIT_TIME_SECONDS):
        cursor = conn.cursor()
        curr = cursor.execute("Select DISTINCT stock_name from user_stocks")
        symbol = cursor.fetchall()
        today = date.today()
        for stock in symbol:
            result = calculate(stock[0])
            min_period = str(result[0]['period'])
            result_min = str(result[0]['average'])
            max_period = str(result[1]['period'])
            result_max = str(result[1]['average'])
            print(min_period)
            print(result_min)
            print(max_period)
            print(result_max)
            cursor.execute(f"INSERT INTO calculate (date, stock_name, min_period, max_period, result_min,"
                           f"result_max) VALUES ('{today}', '{stock[0]}', '{min_period}', '{max_period}', '{result_min}', '{result_max}')")
            conn.commit()


if __name__ == '__main__':
    threading.Thread(target=task).start()
    logging.basicConfig(level=logging.INFO)
    executor.start_polling(dp, skip_updates=True)
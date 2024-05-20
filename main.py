import datetime
import json
import logging
import os

import pymongo
import telebot
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("BOT_TOKEN")

logging.basicConfig(level=logging.INFO, filename='bot.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

bot = telebot.TeleBot(TOKEN)

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["sampleDB"]
collection = db["sample_collection"]


def aggregate_salaries(dt_from, dt_upto, group_type):
    """
    Агрегирует данные о зарплатах сотрудников по заданным временным промежуткам.

    Args:
      dt_from: Дата и время начала агрегации в ISO формате.
      dt_upto: Дата и время окончания агрегации в ISO формате.
      group_type: Тип агрегации ("hour", "day", "month").

    Returns:
      Словарь с агрегированными данными.
    """

    dt_from = datetime.datetime.fromisoformat(dt_from)
    dt_upto = datetime.datetime.fromisoformat(dt_upto)

    dataset = []
    labels = []

    if group_type == "hour":
        current_date = dt_from
        while current_date <= dt_upto:
            total_salary = 0
            for document in collection.find():
                if "salary" in document and "date" in document:
                    salary_data = document["salary"]
                    date_data = document["date"]
                    if isinstance(salary_data, str) and isinstance(date_data, str):
                        try:
                            # Convert to integer
                            salary_data = int(salary_data)
                            document_datetime = datetime.datetime.fromisoformat(date_data)
                            # Correct Time Window
                            if current_date <= document_datetime < current_date + datetime.timedelta(hours=1):
                                total_salary += salary_data
                        except ValueError as e:
                            logging.error(f"Error converting salary or parsing date: {salary_data}, {date_data}, {e}")

            dataset.append(total_salary)
            labels.append(current_date.isoformat())
            current_date += datetime.timedelta(hours=1)

    elif group_type == "day":
        current_date = dt_from
        while current_date <= dt_upto:
            total_salary = 0
            for document in collection.find():
                if "salary" in document and "date" in document:
                    salary_data = document["salary"]
                    date_data = document["date"]
                    if isinstance(salary_data, int) and isinstance(date_data, str):

                        if datetime.datetime.fromisoformat(date_data).date() == current_date.date():
                            total_salary += salary_data
            dataset.append(total_salary)
            labels.append(current_date.isoformat())
            current_date += datetime.timedelta(days=1)

    elif group_type == "month":
        current_date = dt_from
        while current_date <= dt_upto:
            total_salary = 0
            for document in collection.find():

                if "salary" in document and "date" in document:
                    salary_data = document["salary"]
                    date_data = document["date"]
                    if isinstance(salary_data, int) and isinstance(date_data, str):

                        if datetime.datetime.fromisoformat(
                                date_data).month == current_date.month and datetime.datetime.fromisoformat(
                            date_data).year == current_date.year:
                            total_salary += salary_data
            dataset.append(total_salary)
            labels.append(datetime.datetime(current_date.year, current_date.month, 1).isoformat())
            current_date += datetime.timedelta(days=31)

            if current_date.month != dt_from.month + 1:
                current_date = datetime.datetime(current_date.year, current_date.month, 1)

    else:
        raise ValueError("Неверный тип агрегации.")

    return {"dataset": dataset, "labels": labels}


@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.reply_to(message, "Привет! Отправьте JSON-строку с параметрами запроса.")


@bot.message_handler(func=lambda message: True)
def handle_message(message):
    try:

        data = json.loads(message.text)
        dt_from = data["dt_from"]
        dt_upto = data["dt_upto"]
        group_type = data["group_type"]

        result = aggregate_salaries(dt_from, dt_upto, group_type)

        response = json.dumps(result)

        bot.send_message(message.chat.id, response)

    except Exception as e:
        bot.send_message(message.chat.id, f"Ошибка: {e}")


bot.polling()

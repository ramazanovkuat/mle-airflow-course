# plugins/steps/messages.py
from airflow.providers.telegram.hooks.telegram import TelegramHook
from dotenv import load_dotenv
import os

load_dotenv()
TG_BOT_TOKEN = os.environ.get('TG_BOT_TOKEN')
TG_CHAT_ID = os.environ.get('TG_CHAT_ID')

def send_telegram_failure_message(context):
    hook = TelegramHook(
        telegram_conn_id="test",
        token=TG_BOT_TOKEN,
        chat_id=TG_CHAT_ID,
    )
    dag = context["dag"]
    run_id = context["run_id"]
    task_instance_key_str = context["task_instance_key_str"]

    message = f"Исполнение DAG {dag} с id={run_id} и task_instance_key_str={task_instance_key_str} не завершилось"  # определение текста сообщения
    hook.send_message(
        {"chat_id": TG_CHAT_ID, "text": message}
    )  # отправление сообщения


def send_telegram_success_message(context):
    hook = TelegramHook(
        telegram_conn_id="test",
        token=TG_BOT_TOKEN,
        chat_id=TG_CHAT_ID,
    )
    dag = context["dag"]
    run_id = context["run_id"]
    task_instance_key_str = context["task_instance_key_str"]

    message = f"Исполнение DAG {dag} с id={run_id} и task_instance_key_str={task_instance_key_str} прошло успешно!"  # определение текста сообщения
    hook.send_message(
        {"chat_id": TG_CHAT_ID, "text": message}
    )  # отправление сообщения

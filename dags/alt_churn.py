# dags/churn.py
from steps.messages import send_telegram_success_message, send_telegram_failure_message
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator  # импортируем класс оператора
from steps.churn import (
    create_table,
    extract,
    transform,
    load,
)  # импортируем фукнции с логикой шагов
from airflow.models.baseoperator import chain

with DAG(
    dag_id="alt_churn",
    schedule="@once",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message,
    tags=["ETL"],
) as dag:

    # инициализируем задачи DAG, указывая параметр python_callable
    create_step = PythonOperator(task_id="create", python_callable=create_table)
    extract_step = PythonOperator(task_id="extract", python_callable=extract)
    transform_step = PythonOperator(task_id="transform", python_callable=transform)
    load_step = PythonOperator(task_id="load", python_callable=load)

    [create_step >> extract_step] >> transform_step
    transform_step >> load_step

    # chain([create_step, extract_step], transform_step, load_step)
# plugins/steps/churn.py
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


def create_table():
    from sqlalchemy import inspect
    from sqlalchemy import (
        MetaData,
        Table,
        Column,
        String,
        Integer,
        DateTime,
        UniqueConstraint,
        Float,
    )

    metadata = MetaData()
    users_churn_table = Table(
        "alt_users_churn",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("customer_id", String),
        Column("begin_date", DateTime),
        Column("end_date", DateTime),
        Column("type", String),
        Column("paperless_billing", String),
        Column("payment_method", String),
        Column("monthly_charges", Float),
        Column("total_charges", Float),
        Column("internet_service", String),
        Column("online_security", String),
        Column("online_backup", String),
        Column("device_protection", String),
        Column("tech_support", String),
        Column("streaming_tv", String),
        Column("streaming_movies", String),
        Column("gender", String),
        Column("senior_citizen", Integer),
        Column("partner", String),
        Column("dependents", String),
        Column("multiple_lines", String),
        Column("target", Integer),
        UniqueConstraint("customer_id", name="unique_customer_id"),
    )
    hook = PostgresHook("destination_db")
    db_conn = hook.get_sqlalchemy_engine()
    if not inspect(db_conn).has_table(users_churn_table.name):
        metadata.create_all(db_conn)
    db_conn.dispose()


def extract(**kwargs):
    # ваш код здесь #
    hook = PostgresHook("source_db")
    conn = hook.get_conn()
    sql = f"""
    select
            c.customer_id, c.begin_date, c.end_date, c.type, c.paperless_billing, c.payment_method, c.monthly_charges, c.total_charges,
            i.internet_service, i.online_security, i.online_backup, i.device_protection, i.tech_support, i.streaming_tv, i.streaming_movies,
            p.gender, p.senior_citizen, p.partner, p.dependents,
            ph.multiple_lines
        from contracts as c
        left join internet as i on i.customer_id = c.customer_id
        left join personal as p on p.customer_id = c.customer_id
        left join phone as ph on ph.customer_id = c.customer_id
        """
    data = pd.read_sql(sql, conn)
    ti = kwargs["ti"]  # получение объекта task_instance
    ti.xcom_push(key="extracted_data", value=data)
    conn.close()


def transform(**kwargs):
    ti = kwargs["ti"]
    # task_ids — ключ, по которому в будущем эти данные можно будет выгрузить
    # key - идентификатор требуемых данных, который вы задали
    data = ti.xcom_pull(task_ids="extract", key="extracted_data")
    data["target"] = (data["end_date"] != "No").astype(int)
    data["end_date"].replace({"No": None}, inplace=True)
    # key — название шага, на котором данные были выгружены;
    # value - ключ данных. Это тот самый key, который используется для получения данных .xcom_pull.
    ti.xcom_push(key="transformed_data", value=data)


def load(**kwargs):
    # ваш код здесь #
    ti = kwargs["ti"]  # получение объекта task_instance
    data = ti.xcom_pull(task_ids="transform", key="transformed_data")
    hook = PostgresHook("destination_db")
    hook.insert_rows(
        table="alt_users_churn",
        replace=True,
        target_fields=data.columns.tolist(),
        replace_index=["customer_id"],
        rows=data.values.tolist(),
    )
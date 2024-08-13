import pendulum
import psycopg2
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from bs4 import BeautifulSoup


def get_redshift_connection(autocommit: bool = True) -> psycopg2.extensions.connection:
    hook = PostgresHook(postgres_conn_id="AWS_Redshift")
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

HTTP_OK = 200

def etl() -> None:
    now = pendulum.now("Asia/Seoul").format("YYYY-MM-DD HH:mm:ss")

    cur = get_redshift_connection()
    url = "https://www.weather.go.kr/w/dust/dust-obs-days.do"

    res = requests.get(url)

    if res.status_code == HTTP_OK:
        soup = BeautifulSoup(res.content, "html.parser")
        count = 0
        for i in range(2, 13):
            temp = soup.select_one(f"#pm10-stat > tbody > tr:nth-child(1) > td:nth-child({i})")
            if temp and temp.text.strip() != ".":
                count += int(temp.text.strip())

        cur.execute(f"""
            UPDATE mart_data.natural_disasters
            SET DUST = {count}, updated_at = '{now}'
            WHERE YEAR = '2024'
        """)

    cur.close()

default_args = {
    "owner": "wonwoo",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

dag = DAG(
    "Update_DISCNT_DUST",
    default_args=default_args,
    description="황사 관측일수",
    start_date=pendulum.datetime(2024, 7, 25, tz="Asia/Seoul"),
    schedule_interval="0 0 * * *",
    tags=["크롤", "Daily", "1 time", "mart"],
    catchup=False
)

Update_DISCNT_DUST = PythonOperator(
    task_id="Update_DISCNT_DUST",
    python_callable=etl,
    dag=dag,
)

Update_DISCNT_DUST

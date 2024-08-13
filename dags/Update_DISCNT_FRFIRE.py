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
    url = "https://www.forest.go.kr/kfsweb/kfi/kfs/frfr/selectFrfrStatsNow.do?mn=NKFS_06_09_01"

    res = requests.get(url)

    if res.status_code == HTTP_OK:
        soup = BeautifulSoup(res.content, "html.parser")
        total_year_cnt_element = soup.select_one("#Tab02 > div.tbl_wrap > table > tfoot > tr > td:nth-child(2)")
        if total_year_cnt_element:
            count = total_year_cnt_element.text.strip()
            cur.execute(f"""
                UPDATE mart_data.natural_disasters
                SET FRFIRE = {count}, updated_at = '{now}'
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
    "Update_DISCNT_FRFIRE",
    default_args=default_args,
    description="산불 발생횟수",
    start_date=pendulum.datetime(2024, 7, 25, tz="Asia/Seoul"),
    schedule_interval="0 0 * * *",
    tags=["산림청", "Daily", "1 time", "mart"],
    catchup=False
)

Update_DISCNT_FRFIRE = PythonOperator(
    task_id="Update_DISCNT_FRFIRE",
    python_callable=etl,
    dag=dag,
)

Update_DISCNT_FRFIRE

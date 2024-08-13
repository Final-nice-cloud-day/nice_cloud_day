import pendulum
import psycopg2
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_redshift_connection(autocommit: bool = True) -> psycopg2.extensions.connection:
    hook = PostgresHook(postgres_conn_id="AWS_Redshift")
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

HTTP_OK = 200
TYPHOON_CODE = 3

def etl() -> None:
    now = pendulum.now("Asia/Seoul").format("YYYY-MM-DD HH:mm:ss")

    cur = get_redshift_connection()
    url = "https://apihub.kma.go.kr/api/typ01/url/typ_lst.php"
    key = "KowdqwCsSM-MHasArOjPyQ" # 기상청 허브 api키
    params = {
        "YY": "2024",
        "disp": 1,
        "help": 2,
        "authKey": key
    }

    res = requests.get(url, params=params)

    if res.status_code == HTTP_OK:
        data = res.text

        lines = data.strip().split("\n")

        count = 0
        for line in lines:
            parts = line.split(",")
            if len(parts) > TYPHOON_CODE:
                if parts[3] in ["1", "2"]:
                    count += 1

        cur.execute(f"""
            UPDATE mart_data.natural_disasters
            SET TYPHOON = {count}, updated_at = '{now}'
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
    "Update_DISCNT_TYPHOON",
    default_args=default_args,
    description="태풍 발생횟수",
    start_date=pendulum.datetime(2024, 7, 25, tz="Asia/Seoul"),
    schedule_interval="0 0 * * *",  # 매일 00시에 실행
    tags=["기상청", "Daily", "1 time", "mart"],
    catchup=False
)

Update_DISCNT_TYPHOON = PythonOperator(
    task_id="Update_DISCNT_TYPHOON",
    python_callable=etl,
    dag=dag,
)

Update_DISCNT_TYPHOON

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
from bs4 import BeautifulSoup
import pendulum

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='AWS_Redshift')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def etl():
    now = pendulum.now('Asia/Seoul').format('YYYY-MM-DD HH:mm:ss')

    cur = get_Redshift_connection()
    url = 'https://www.forest.go.kr/kfsweb/kfi/kfs/frfr/selectFrfrStatsNow.do?mn=NKFS_06_09_01'
    
    res = requests.get(url)
    
    if res.status_code == 200:
        soup = BeautifulSoup(res.content, 'html.parser')
        total_year_cnt_element = soup.select_one('#Tab02 > div.tbl_wrap > table > tfoot > tr > td:nth-child(2)')
        if total_year_cnt_element:
            count = total_year_cnt_element.text.strip()
            cur.execute(f"""
                UPDATE mart_data.natural_disasters
                SET FRFIRE = {count}, updated_at = '{now}'
                WHERE YEAR = '2024'
            """)
    
    cur.close()

default_args = {
    'owner': 'wonwoo',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'Update_DISCNT_FRFIRE',
    default_args=default_args,
    description='Update 2024 FRFIRE DIS_CNT data in Redshift',
    start_date=pendulum.datetime(2024, 7, 25, tz='Asia/Seoul'),
    schedule_interval='0 0 * * *',
    tags=['산림청', 'Daily', '1 time', 'mart'],
    catchup=False
)

DISCNT_FRFIRE_task = PythonOperator(
    task_id='Update_DISCNT_FRFIRE',
    python_callable=etl,
    dag=dag,
)

DISCNT_FRFIRE_task
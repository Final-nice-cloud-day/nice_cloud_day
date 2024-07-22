from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests
import pytz
from bs4 import BeautifulSoup

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='AWS_Redshift')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def etl():
    kst = pytz.timezone('Asia/Seoul')
    now = datetime.now(kst)
    now = now.strftime('%Y-%m-%d %H:%M:%S')

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

dag = DAG(
    'Update_DISCNT_FRFIRE',
    description='Update 2024 FRFIRE DIS_CNT data in Redshift',
    start_date=datetime(2024, 7, 20, 0, 0),
    schedule_interval='0 0 * * *',
    catchup=False
)

DISCNT_FRFIRE_task = PythonOperator(
    task_id='Update_DISCNT_FRFIRE',
    python_callable=etl,
    dag=dag,
)

DISCNT_FRFIRE_task
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
    url = 'https://www.weather.go.kr/w/dust/dust-obs-days.do'
    
    res = requests.get(url)
    
    if res.status_code == 200:
        soup = BeautifulSoup(res.content, 'html.parser')
        count = 0
        for i in range(2, 13):
            temp = soup.select_one(f'#pm10-stat > tbody > tr:nth-child(1) > td:nth-child({i})')
            if temp and temp.text.strip() != '.':
                count += int(temp.text.strip())
        
        cur.execute(f"""
            UPDATE mart_data.natural_disasters
            SET DUST = {count}, updated_at = '{now}'
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
    'Update_DISCNT_DUST',
    default_args=default_args,
    description='Update 2024 DUST DIS_CNT data in Redshift',
    start_date=pendulum.datetime(2024, 7, 25, tz='Asia/Seoul'),
    schedule_interval='0 0 * * *',
    tags=['크롤', 'Daily', '1 time', 'mart'],
    catchup=False
)

DISCNT_DUST_task = PythonOperator(
    task_id='Update_DISCNT_DUST',
    python_callable=etl,
    dag=dag,
)

DISCNT_DUST_task
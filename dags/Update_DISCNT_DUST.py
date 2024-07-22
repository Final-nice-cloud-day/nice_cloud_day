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

dag = DAG(
    'Update_DISCNT_DUST',
    description='Update 2024 DUST DIS_CNT data in Redshift',
    start_date=datetime(2024, 7, 20, 0, 0),
    schedule_interval='0 0 * * *',
    catchup=False
)

DISCNT_DUST_task = PythonOperator(
    task_id='Update_DISCNT_DUST',
    python_callable=etl,
    dag=dag,
)

DISCNT_DUST_task
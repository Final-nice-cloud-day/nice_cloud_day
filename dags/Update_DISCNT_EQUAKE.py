from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests
import pytz
import xml.etree.ElementTree as ET

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
    url = 'https://apihub.kma.go.kr/api/typ09/url/eqk/urlNewNotiEqk.do'
    key = 'KowdqwCsSM-MHasArOjPyQ' # 기상청 허브 api키
    params = {
        'frDate' : '20240101',
        'laDate' : '20241231',
        'msgCode' : 102,
        'cntDiv' : 'Y',
        'nkDiv' : 'N',
        'orderTy' : 'xml',
        'authKey': key
    }

    res = requests.get(url, params=params)
    
    if res.status_code == 200:
        temp = ET.fromstring(res.text)

        info = temp.findall('.//info')
        cnt = len(info)

        cur.execute(f"""
            UPDATE mart_data.natural_disasters
            SET EQUAKE = {cnt}, updated_at = '{now}'
            WHERE YEAR = '2024'
        """)
    
    cur.close()

dag = DAG(
    'Update_DISCNT_EQUAKE',
    description='Update 2024 EQUAKE DIS_CNT data in Redshift',
    start_date=datetime(2024, 7, 20, 0, 0),
    schedule_interval='0 0 * * *',
    catchup=False
)

DISCNT_EQUAKE_task = PythonOperator(
    task_id='Update_DISCNT_EQUAKE',
    python_callable=etl,
    dag=dag,
)

DISCNT_EQUAKE_task

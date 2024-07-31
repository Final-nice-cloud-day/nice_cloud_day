from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum
import requests

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='AWS_Redshift')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def etl():
    now = pendulum.now('Asia/Seoul').format('YYYY-MM-DD HH:mm:ss')

    cur = get_Redshift_connection()
    url = 'https://apihub.kma.go.kr/api/typ01/url/typ_lst.php'
    key = 'KowdqwCsSM-MHasArOjPyQ' # 기상청 허브 api키
    params = {
        'YY': '2024',
        'disp': 1,
        'help': 2,
        'authKey': key
    }

    res = requests.get(url, params=params)
    
    if res.status_code == 200:
        data = res.text

        lines = data.strip().split('\n')

        count = 0
        for line in lines:
            parts = line.split(',')
            if len(parts) > 3:
                if parts[3] in ['1', '2']:
                    count += 1
        
        cur.execute(f"""
            UPDATE mart_data.natural_disasters
            SET TYPHOON = {count}, updated_at = '{now}'
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
    'Update_DISCNT_TYPHOON',
    default_args=default_args,
    description='Update 2024 TYPHOON DIS_CNT data in Redshift',
    start_date=pendulum.datetime(2024, 7, 25, tz='Asia/Seoul'),
    schedule_interval='0 0 * * *',  # 매일 00시에 실행
    tags=['기상청', 'Daily', '1 time', 'mart'],
    catchup=False
)

DISCNT_TYPHOON_task = PythonOperator(
    task_id='Update_DISCNT_TYPHOON',
    python_callable=etl,
    dag=dag,
)

DISCNT_TYPHOON_task
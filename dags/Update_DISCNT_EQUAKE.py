from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import xml.etree.ElementTree as ET
import pendulum
import pandas as pd

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='AWS_Redshift')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def etl():
    now = pendulum.now('Asia/Seoul').format('YYYY-MM-DD HH:mm:ss')

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
    
    equake_items =[]

    if res.status_code == 200:
        temp = ET.fromstring(res.text)

        info = temp.findall('.//info')
        cnt = len(info)

        cur.execute(f"""
            UPDATE mart_data.natural_disasters
            SET EQUAKE = {cnt}, updated_at = '{now}'
            WHERE YEAR = '2024'
        """)
    
    for item in temp.findall(".//info"):
        equake_items.append(item)
    
    equake_data = []
    for item in equake_items:
        eqPt = item.find('eqPt').text if item.find('eqPt') is not None else None
        eqLt = item.find('eqLt').text if item.find('eqLt') is not None else None
        eqLn = item.find('eqLn').text if item.find('eqLn') is not None else None
        magMl = item.find('magMl').text if item.find('magMl') is not None else None
        jdLoc = item.find('jdLoc').text if item.find('jdLoc') is not None else None
        eqDate = item.find('eqDate').text if item.find('eqDate') is not None else None
        tmIssue = item.find('tmIssue').text if item.find('tmIssue') is not None else None

        equake_data.append({
            'eqPt': eqPt,
            'eqLt' : eqLt,
            'eqLn' : eqLn,
            'magMl' : magMl,
            'jdLoc' : jdLoc,
            'eqDate' : eqDate,
            'created_at' : tmIssue
        })

    equake_df = pd.DataFrame(equake_data)

    equake_df['eqDate'] = equake_df['eqDate'].astype(str)
    equake_df['eqDate'] = pd.to_datetime(equake_df['eqDate'])
    equake_df['created_at'] = equake_df['created_at'].astype(str)
    equake_df['created_at'] = pd.to_datetime(equake_df['created_at'])

    cur.execute("DROP TABLE mart_data.equake2024")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS mart_data.equake2024 (
            eqPt VARCHAR(64),
            eqLt FLOAT,
            eqLn FLOAT,
            magMl FLOAT,
            jdLoc VARCHAR(8),
            eqDate TIMESTAMP,
            data_key TIMESTAMP,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );
    """)
    for index, row in equake_df.iterrows():
        cur.execute("""
            INSERT INTO mart_data.equake2024 (eqPt, eqLt, eqLn, magMl, jdLoc, eqDate, data_key, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (row['eqPt'], row['eqLt'], row['eqLn'], row['magMl'], row['jdLoc'], row['eqDate'], now, row['created_at'], now))

    cur.close()

default_args = {
    'owner': 'wonwoo',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'Update_DISCNT_EQUAKE',
    default_args=default_args,
    description='Update 2024 EQUAKE DIS_CNT data in Redshift',
    start_date=pendulum.datetime(2024, 7, 25, tz='Asia/Seoul'),
    schedule_interval='0 0 * * *',
    tags=['기상청', 'Daily', '1 time', 'mart'],
    catchup=False
)

DISCNT_EQUAKE_task = PythonOperator(
    task_id='Update_DISCNT_EQUAKE',
    python_callable=etl,
    dag=dag,
)

DISCNT_EQUAKE_task

#24년도 지진정보 테이블 적재하는 대그 작성

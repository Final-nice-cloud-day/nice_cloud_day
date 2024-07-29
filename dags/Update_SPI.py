from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum
import requests
import xml.etree.ElementTree as ET
import pandas as pd
import os

S3_BUCKET_NAME = 'team-okky-1-bucket'
HJDCD_FILE_KEY = 'SPI/hjdCd.csv'
SPI_FILE_KEY = 'SPI/2024_spi.csv'

def get_filtered_area():
    s3 = S3Hook(aws_conn_id='AWS_S3')
    obj = s3.get_key(HJDCD_FILE_KEY, bucket_name=S3_BUCKET_NAME)
    filtered_area = pd.read_csv(obj.get()['Body'])
    return filtered_area

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='AWS_Redshift')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def get_spi_data():
    key = 'RCcAK3YdiixgFhu5bAW3uDSK94160ypF5MiU5kA754R1vQRNoKcHZIByikIHK/0Q/oCHdxAdhZ1W415xKrFLlA=='
    url = 'http://apis.data.go.kr/B500001/drghtIdexSpiAnals/analsInfoList' # 공공데이터 API
    
    filtered_area = get_filtered_area()
    spi_items = []

    today = pendulum.now('Asia/Seoul')
    last_monday = today.start_of('week').subtract(weeks=1)
    last_sunday = last_monday.end_of('week')
    start_date = last_monday.format('YYYYMMDD')
    end_date = last_sunday.format('YYYYMMDD')

    for index, row in filtered_area.iterrows():
        cd = row['행정구역코드']

        params = {
            'serviceKey': key,
            'pageNo': 1,
            'numOfRows': '400',
            'hjdCd': cd,
            'stDt': start_date,
            'edDt': end_date
        }

        response = requests.get(url, params=params)

        try:
            root = ET.fromstring(response.content)
        except ET.ParseError as e:
            print(f"XML Parsing error: {e}")
            continue

        # SPI3 항목을 수집합니다.
        for item in root.findall(".//item"):
            dv_elem = item.find('dv')
            if dv_elem is not None and dv_elem.text == 'SPI3':
                spi_items.append(item)

    data = []
    for item in spi_items:
        anldt = item.find('anldt').text if item.find('anldt') is not None else None
        anlrst = item.find('anlrst').text if item.find('anlrst') is not None else None
        anlval = item.find('anlval').text if item.find('anlval') is not None else None
        dv = item.find('dv').text if item.find('dv') is not None else None
        hjdcd = item.find('hjdcd').text if item.find('hjdcd') is not None else None

        data.append({
            'anldt': anldt,
            'anlrst': anlrst,
            'anlval': anlval,
            'hjdcd': hjdcd
        })
    
    df_spi = pd.DataFrame(data)
    df_spi = df_spi[['anldt', 'anlrst', 'anlval', 'hjdcd']]
    return df_spi

def update_s3_csv():
    s3 = S3Hook(aws_conn_id='AWS_S3')
    
    # 기존 CSV 파일 읽기
    obj = s3.get_key(SPI_FILE_KEY, bucket_name=S3_BUCKET_NAME)
    existing_df = pd.read_csv(obj.get()['Body'], encoding='euc-kr')
    
    # 새로운 데이터 가져오기
    new_data_df = get_spi_data()
    
    # 기존 데이터에 새로운 데이터 추가
    updated_df = pd.concat([existing_df, new_data_df], ignore_index=True)
    
    # 로컬 파일로 저장
    updated_df.to_csv('/tmp/2024_spi.csv', index=False, encoding='euc-kr')
    
    # S3에 업로드
    s3.load_file('/tmp/2024_spi.csv', key=SPI_FILE_KEY, bucket_name=S3_BUCKET_NAME, replace=True)
    
    # 로컬 파일 삭제
    os.remove('/tmp/2024_spi.csv')

    # 분석된 데이터를 필터링하여 적절한 레코드 찾기
    filtered_df = updated_df[(updated_df['anlval'] > 2) | (updated_df['anlval'] < -2)]
    
    if not filtered_df.empty:
        # 레드시프트 추가 데이터 삽입
        cur = get_Redshift_connection()
        execution_time = pendulum.now('Asia/Seoul')
        
        flood_count = len(filtered_df[filtered_df['anlval'] > 2])
        drought_count = len(filtered_df[filtered_df['anlval'] < -2])
        
        # SPI 데이터 삽입
        for index, row in filtered_df.iterrows():
            cur.execute("""
                INSERT INTO mart_data.SPI (anldt, anlrst, anlval, hjdcd, data_key, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (row['anldt'], row['anlrst'], row['anlval'], row['hjdcd'], execution_time, row['anldt'], execution_time))
        
        # FLOOD 및 DROUGHT 컬럼 업데이트
        cur.execute("""
            UPDATE mart_data.natural_disasters
            SET FLOOD = COALESCE(FLOOD, 0) + %s,
                DROUGHT = COALESCE(DROUGHT, 0) + %s,
                updated_at = %s
            WHERE YEAR = '2024'
        """, (flood_count, drought_count, execution_time))
        
        cur.close()

dag = DAG(
    'Update_DISCNT_SPI',
    description='Weekly update of SPI data',
    start_date=pendulum.datetime(2024, 7, 25, tz='Asia/Seoul'),
    schedule_interval='0 0 * * 4',  # 매주 목요일 00시에 실행
    catchup=False
)

update_spi_task = PythonOperator(
    task_id='update_spi_data',
    python_callable=update_s3_csv,
    dag=dag,
)

update_spi_task

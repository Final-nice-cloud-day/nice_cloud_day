from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import requests
import csv
from io import StringIO
import pendulum
import pandas as pd
from psycopg2.extras import execute_values
import logging
import re

kst = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  # 선행작업의존여부N
    'start_date': datetime(2024, 7, 1, 7, 0, 0, tzinfo=kst),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def stn_inf_to_s3(logical_date, **kwargs):
    api_url = "https://apihub.kma.go.kr/api/typ01/url/stn_inf.php?"
    api_key = "HGbLr74hS2qmy6--ITtqog"
    params = {
    'inf' : 'SFC',
    'tm' : '',
    'stn' : '',
    'help': 0,
    'authKey': api_key
    }
    response = requests.get(api_url, params=params)
    logging.info(f"API 상태코드: {response.status_code}")

    if response.status_code == 200:
        response_text = response.text
        logging.info(f"응답 데이터:\n{response_text}")

        if "#START7777" in response_text and "#7777END" in response_text:
            lines = response_text.splitlines()
            data = []

            start_index = 0
            for i, line in enumerate(lines):
                if line.startswith('#START7777'):
                    start_index = i + 3
                    break
            logging.info(f"start_index: {start_index}")
            for line in lines[start_index:]:
                if line.strip() and not line.startswith('#7777END'):
                    # 공백하나 기준으로 분리
                    columns = re.split(r'\s{1,}', line.strip())
                    logging.info(f"columns: {columns}")
                    if columns[-1] == '=':
                        columns = columns[:-1]

                    try:
                        stn_id = columns[0]
                        lon = columns[1]
                        lat = columns[2]
                        stn_sp = columns[3]
                        ht = columns[4]
                        ht_pa = columns[5]
                        ht_ta = columns[6]
                        ht_wd = columns[7]
                        ht_rn = columns[8]
                        stn_ad = columns[9]
                        stn_ko = columns[10]
                        stn_en = columns[11]
                        fct_id = columns[12]
                        law_id = columns[13]
                        basin = columns[14]
                        data.append((stn_id, lon, lat, stn_sp, ht, ht_pa, ht_ta, ht_wd, ht_rn, stn_ad, stn_ko, stn_en, fct_id, law_id, basin))
                           
                    except ValueError as e:
                        logging.warning(f"행을 파싱하는 중 오류 발생: {e}")
                
            if data:
                logical_date_kst = logical_date.in_timezone(kst)
                year = logical_date_kst.strftime('%Y')
                month = logical_date_kst.strftime('%m')
                day = logical_date_kst.strftime('%d')
                formatted_date = logical_date_kst.strftime('%Y_%m_%d')

                csv_buffer = StringIO()
                csv_writer = csv.writer(csv_buffer)
                csv_writer.writerow(['STN_ID', 'LON_DEGRE', 'LAT_DEGRE', 'STN_SP', 'HT_M', 'HT_PA', 'HT_TA', 'HT_WD', 'HT_RN', 'STN_AD', 'STN_KO', 'STN_EN', 'FCT_ID', 'LAW_ID', 'BASIN_CD'])
                csv_writer.writerows(data)
                
                s3_hook = S3Hook(aws_conn_id='AWS_S3')
                bucket_name = 'team-okky-1-bucket'
                s3_key = f'stn_inf/{year}/{month}/{day}/{formatted_date}_stn_inf.csv'
                
                try:
                    s3_hook.load_string(
                        csv_buffer.getvalue(),
                        key=s3_key,
                        bucket_name=bucket_name,
                        replace=True
                    )
                    logging.info(f"저장성공 첫 번째 데이터 행: {data[0]}")
                    kwargs['task_instance'].xcom_push(key='s3_key', value=s3_key)
                except Exception as e:
                    logging.error(f"S3 업로드 실패: {e}")
                    raise ValueError(f"S3 업로드 실패: {e}")
            else:
                logging.error("ERROR : 유효한 데이터가 없어 삽입할 수 없습니다.")
                raise ValueError("ERROR : 유효한 데이터가 없어 삽입할 수 없습니다.")
        else:
            logging.error("ERROR : 데이터 수신 실패", response_text)
            raise ValueError(f"ERROR : 데이터 수신 실패 : {response_text}")
    else:
        logging.error(f"ERROR : 응답 코드 오류 {response.status_code}")
        logging.error(f"ERROR : 메세지 :", response.text)
        raise ValueError(f"ERROR : 응답코드오류 {response.status_code}, 메세지 : {response.text}")
    
def stn_inf_to_redshift(logical_date, **kwargs):
    logging.info("redshift 적재 시작")
    s3_key = kwargs['task_instance'].xcom_pull(task_ids='stn_inf_to_s3', key='s3_key')
    s3_path = f's3://team-okky-1-bucket/{s3_key}'
    s3_hook = S3Hook(aws_conn_id='AWS_S3')
    bucket_name = 'team-okky-1-bucket'
    redshift_hook = PostgresHook(postgres_conn_id='AWS_Redshift')
    
    csv_content = s3_hook.read_key(s3_key, bucket_name)
    logging.info(f"S3 경로: {s3_key}")
    csv_reader = csv.reader(StringIO(csv_content))
    next(csv_reader)  # 헤더 skip
    
    data = []
    for row in csv_reader:
        try:
            stn_id, lon, lat, stn_sp, ht, ht_pa, ht_ta, ht_wd, ht_rn, stn_ad, stn_ko, stn_en, fct_id, law_id, basin = row
            data_key = logical_date + timedelta(hours=9)
            created_at = data_key
            updated_at = data_key
            data.append((stn_id, lon, lat, stn_sp, ht, ht_pa, ht_ta, ht_wd, ht_rn, stn_ad, stn_ko, stn_en, fct_id, law_id, basin, data_key, created_at, updated_at))
        except ValueError as e:
            logging.warning(f"ERROR : 파싱오류: {row}, error: {e}")
        
    
    if data:
        logging.info(f"{len(data)} rows 데이터를 읽었습니다.")
        conn = redshift_hook.get_conn()
        cursor = conn.cursor()

        # 적재를 위한 temp 테이블 생성
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS temp_STN_INF_INFO (
            STN_ID	VARCHAR(3)	NOT NULL,
            LON_DEGRE	DECIMAL(10, 7)	NULL,
            LAT_DEGRE	DECIMAL(10, 7)	NULL,
            STN_SP	VARCHAR(5)	NULL,
            HT_M	DECIMAL(5, 2)	NULL,
            HT_PA	DECIMAL(5, 2)	NULL,
            HT_TA	DECIMAL(3, 2)	NULL,
            HT_WD	DECIMAL(4, 2)	NULL,
            HT_RN	DECIMAL(3, 2)	NULL,
            STN_AD	VARCHAR(6)	NULL,
            STN_KO	VARCHAR(20)	NULL,
            STN_EN	VARCHAR(20)	NULL,
            FCT_ID	VARCHAR(8)	NULL,
            LAW_ID	VARCHAR(10)	NULL,
            BASIN_CD	VARCHAR(10)	NULL,
            DATA_KEY TIMESTAMP NULL,
            CREATED_AT TIMESTAMP NULL,
            UPDATED_AT TIMESTAMP NULL
        );
        """)
        
        insert_temp_query = """
        INSERT INTO temp_STN_INF_INFO (STN_ID, LON_DEGRE, LAT_DEGRE, STN_SP, HT_M, HT_PA, HT_TA, HT_WD, HT_RN, STN_AD, STN_KO, STN_EN, FCT_ID, LAW_ID, BASIN_CD, DATA_KEY, CREATED_AT, UPDATED_AT)
        VALUES %s;
        """
        execute_values(cursor, insert_temp_query, data)
        
        insert_query = """
        INSERT INTO raw_data.stn_inf_info (STN_ID, LON_DEGRE, LAT_DEGRE, STN_SP, HT_M, HT_PA, HT_TA, HT_WD, HT_RN, STN_AD, STN_KO, STN_EN, FCT_ID, LAW_ID, BASIN_CD, DATA_KEY, CREATED_AT, UPDATED_AT)
        SELECT t.STN_ID, t.LON_DEGRE, t.LAT_DEGRE, t.STN_SP, t.HT_M, t.HT_PA, t.HT_TA, t.HT_WD, t.HT_RN, t.STN_AD, t.STN_KO, t.STN_EN, t.FCT_ID, t.LAW_ID, t.BASIN_CD, t.DATA_KEY, t.CREATED_AT, t.UPDATED_AT
        FROM temp_STN_INF_INFO t
        LEFT JOIN raw_data.stn_inf_info f
        ON t.STN_ID = f.STN_ID
        WHERE f.STN_ID IS NULL;
        """
        try:
            cursor.execute(insert_query)
            conn.commit()
            logging.info(f"Redshift 적재 완료: {s3_path}")
        except Exception as e:
            raise ValueError(f"Redshift 로드 실패: {e}")
    else:
        logging.error("ERROR : 적재할 데이터가 없습니다.")
        raise ValueError("ERROR : 적재할 데이터가 없습니다.")
    
    
with DAG(
    'Daily_1_stn_inf_to_s3_and_redshif',
    default_args=default_args,
    description='stn_inf upload to S3',
    schedule_interval='0 7 * * *',
    catchup=True,
    dagrun_timeout=timedelta(hours=2),
) as dag:
    dag.timezone = kst
    
    stn_inf_to_s3_task = PythonOperator(
        task_id='stn_inf_to_s3',
        python_callable=stn_inf_to_s3,
        execution_timeout=timedelta(hours=1),
    )
    
    stn_inf_to_redshift_task = PythonOperator(
        task_id='stn_inf_to_redshift',
        python_callable=stn_inf_to_redshift,
        execution_timeout=timedelta(hours=1),
    )

    stn_inf_to_s3_task  >> stn_inf_to_redshift_task

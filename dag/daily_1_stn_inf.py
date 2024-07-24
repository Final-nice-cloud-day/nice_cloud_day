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

kst = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  # 선행작업의존여부N
    'start_date': datetime(2024, 7, 23, 7, 0, 0, tzinfo=kst),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    #'retry_delay': timedelta(minutes=5),
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
                    columns = line.split(',')
                    columns = [col.strip() for col in columns if col.strip()]  
                    
                    if columns[-1] == '=':
                        columns = columns[:-1]

                    try:
                        stn_id = columns[0]
                        lon = columns[1]
                        stn_sp = columns[2]
                        ht = columns[3]
                        ht_pa = columns[4]
                        ht_ta = columns[5]
                        ht_wd = columns[6]
                        ht_rn = columns[7]
                        stn_cd = columns[8]
                        stn_ko = columns[9]
                        stn_en = columns[10]
                        stn_ad = columns[11]
                        fct_id = columns[12]
                        law_id = columns[13]
                        basin = columns[14]
                        data.append((stn_id, lon, stn_sp, ht, ht_pa, ht_ta, ht_wd, ht_rn, stn_cd, stn_ko, stn_en, stn_ad, fct_id, law_id, basin))
                        logging.info(f"Data: {data}")
                    except ValueError as e:
                        logging.warning(f"행을 파싱하는 중 오류 발생: {e}")
                
            if data:
                logical_date_kst = logical_date.in_timezone(kst)
                date_str = logical_date_kst.strftime('%Y%m%d')
                year = date_str.strftime('%Y')
                month = date_str.strftime('%m')
                day = date_str.strftime('%d')
                formatted_date = date_str.strftime('%Y_%m_%d')

                csv_buffer = StringIO()
                csv_writer = csv.writer(csv_buffer)
                csv_writer.writerow(['STN_ID', 'LON', 'STN_SP', 'HT', 'HT_PA', 'HT_TA', 'HT_WD', 'HT_RN', 'STN_CD', 'STN_KO', 'STN_EN', 'STN_AD', 'FCT_ID', 'LAW_ID', 'BASIN'])
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
    

with DAG(
    'Daily_1_stn_inf_to_s3',
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
    
    # stn_inf_to_redshift_task = PythonOperator(
    #     task_id='stn_inf_to_redshift',
    #     python_callable=stn_inf_to_redshift,
    #     execution_timeout=timedelta(hours=1),
    # )

    stn_inf_to_s3_task # >> stn_inf_to_redshift_task

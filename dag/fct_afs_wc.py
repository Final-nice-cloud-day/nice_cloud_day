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
    'start_date': datetime(2024, 7, 21, 10, 0, 0, tzinfo=kst),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fct_afs_wc_to_s3(logical_date, **kwargs):
    api_url = "https://apihub.kma.go.kr/api/typ01/url/fct_afs_wc.php"
    api_key = "HGbLr74hS2qmy6--ITtqog"
    
    logical_date_kst = logical_date.in_timezone(kst)
    
    one_hour_before = logical_date_kst - timedelta(hours=1)
    tmfc1 = one_hour_before.strftime('%Y%m%d%H')
    
    tmfc2 = one_hour_before.strftime('%Y%m%d%H')

    params = {
    'stn': '',
    'reg': '',
    'tmfc': '',
    'tmfc1': tmfc1,
    'tmfc2': tmfc2,
    'tmef1': '',
    'tmef2': '',
    'mode': 0,
    'disp': 1,
    'help': 0,
    'authKey': api_key
}

    response = requests.get(api_url, params=params)
    logging.info(f"API 상태코드: {response.status_code}")

    if response.status_code == 200:
        response_text = response.text

        if "#START7777" in response_text and "#7777END" in response_text:
            lines = response_text.splitlines()
            data = []

            start_index = 0
            for i, line in enumerate(lines):
                if line.startswith('# REG_ID'):
                    start_index = i + 1
                    break

            for line in lines[start_index:]:
                if line.strip():
                    if line.startswith('#7777END'):
                        break
                    columns = line.split(',')
                    columns = [col.strip() for col in columns if col.strip()]  

                    if columns[-1] == '=':
                        columns = columns[:-1]

                    
                    if len(columns) < 12:
                        columns += [None] * (12 - len(columns))

                    try:
                        reg_id = columns[0]
                        tm_fc = datetime.strptime(columns[1], '%Y%m%d%H%M') if columns[1] else None
                        tm_ef = datetime.strptime(columns[2], '%Y%m%d%H%M') if columns[2] else None
                        mod = columns[3]
                        stn = columns[4]
                        c = columns[5]
                        min_val = columns[6]
                        max_val = columns[7]
                        min_l = columns[8]
                        min_h = columns[9]
                        max_l = columns[10]
                        max_h = columns[11]
                        data.append((reg_id, tm_fc, tm_ef, mod, stn, c, min_val, max_val, min_l, min_h, max_l, max_h))
                    except ValueError as e:
                        logging.warning(f"행을 파싱하는 중 오류 발생: {e}")
                
            if data:
                # s3 버킷 디렉토리 생성 기준을 tm_fc 기준으로
                year = tm_fc.strftime('%Y')
                month = tm_fc.strftime('%m')
                day = tm_fc.strftime('%d')
                formatted_date = tm_fc.strftime('%Y_%m_%d')

                csv_buffer = StringIO()
                csv_writer = csv.writer(csv_buffer)
                csv_writer.writerow(['REG_ID','TM_FC','TM_EF','MODE_KEY','STN_ID','CNT_CD','MIN_TA','MAX_TA','MIN_L_TA','MIN_H_TA','MAX_L_TA','MAX_H_TA'])
                csv_writer.writerows(data)
                
                s3_hook = S3Hook(aws_conn_id='AWS_S3')
                bucket_name = 'team-okky-1-bucket'
                s3_key = f'fct_afs_wc/{year}/{month}/{day}/{formatted_date}_fct_afs_wc.csv'
                
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
    
def fct_afs_wc_to_redshift(**kwargs):
    logging.info("redshift 적재 시작")
    s3_key = kwargs['task_instance'].xcom_pull(task_ids='fct_afs_wc_to_s3', key='s3_key')
    s3_path = f's3://team-okky-1-bucket/{s3_key}'
    s3_hook = S3Hook(aws_conn_id='AWS_S3')
    redshift_hook = PostgresHook(postgres_conn_id='AWS_Redshift')
    bucket_name = 'team-okky-1-bucket'
    logical_date = kwargs['logical_date'].in_timezone(kst)
    
    csv_content = s3_hook.read_key(s3_key, bucket_name)
    logging.info(f"S3 경로: {s3_key}")
    
    csv_reader = csv.reader(StringIO(csv_content))
    header = next(csv_reader)  # Skip 헤더
    
    data = []
    for row in csv_reader:
        try:
            reg_id, tm_fc, tm_ef, mood_key, stn_id, cnt_cd, min_ta, max_ta, min_l_ta, min_h_ta, max_l_ta, max_h_ta = row
            tm_st = datetime.strptime(tm_ed, '%Y-%m-%d %H:%M:%S')
            tm_ed = datetime.strptime(tm_ed, '%Y-%m-%d %H:%M:%S')
            data.append((reg_id, tm_fc, tm_ef, mood_key, stn_id, cnt_cd, min_ta, max_ta, min_l_ta, min_h_ta, max_l_ta, max_h_ta, logical_date, tm_st, tm_st))
        except ValueError as e:
            logging.warning(f"ERROR : 파싱오류: {row}, error: {e}")
            
    if data:
        logging.info(f"{len(data)} rows 데이터를 읽었습니다.")
        conn = redshift_hook.get_conn()
        cursor = conn.cursor()

        # Redshift에 삽입할 SQL 쿼리 작성
        insert_query = """
            INSERT INTO raw_data.fct_afs_wc_info (REG_ID, TM_FC, TM_EF, MODE_KEY, STN_ID, CNT_CD, MIN_TA, MAX_TA, MIN_L_TA, MIN_H_TA, MAX_L_TA, MAX_H_TA, DATA_KEY, CREATED_AT, UPDATED_AT)
            VALUES %s;
        """
        
        try:
            execute_values(cursor, insert_query, data)
            conn.commit()
            logging.info(f"Redshift 적재 완료: {s3_path}")
        except Exception as e:
            raise ValueError(f"Redshift 로드 실패: {e}")
    else:
        logging.error("ERROR : 적재할 데이터가 없습니다.")
        raise ValueError("ERROR : 적재할 데이터가 없습니다.")

with DAG(
    'fct_afs_wc_to_s3_redshift',
    default_args=default_args,
    description='fct_afs_wc upload to S3 and Redshift',
    schedule_interval='0 10,19 * * *',
    catchup=True,
    dagrun_timeout=timedelta(hours=2)
) as dag:
    dag.timezone = kst
    
    fct_afs_wc_to_s3_task = PythonOperator(
        task_id='fct_afs_wc_to_s3',
        python_callable=fct_afs_wc_to_s3,
        provide_context=True,
        execution_timeout=timedelta(hours=1),
    )
    
    fct_afs_wc_to_redshift_task = PythonOperator(
        task_id='fct_afs_wc_to_redshift',
        python_callable=fct_afs_wc_to_redshift,
        provide_context=True,
        execution_timeout=timedelta(hours=1),
    )

    fct_afs_wc_to_s3_task >> fct_afs_wc_to_redshift_task

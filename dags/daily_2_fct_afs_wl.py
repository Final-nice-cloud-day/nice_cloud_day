from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from io import StringIO
from psycopg2.extras import execute_values
from common.alert import SlackAlert
import requests
import csv
import pendulum
import pandas as pd
import logging

slackbot = SlackAlert('#airflow_log') 
kst = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'chansu',
    'depends_on_past': True,
    'start_date': pendulum.datetime(2024, 8, 4, tz=kst),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
    'on_failure_callback': slackbot.failure_alert,
    'on_success_callback': slackbot.success_alert,
}
    
def fct_afs_wl_to_s3(data_interval_end, **kwargs):
    api_url = "https://apihub.kma.go.kr/api/typ01/url/fct_afs_wl.php"
    api_key = "HGbLr74hS2qmy6--ITtqog"
    
    data_interval_end_kst = data_interval_end.in_timezone(kst)
    
    one_hour_before = data_interval_end_kst - pendulum.duration(hours=1)
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
                if line.strip() and not line.startswith('#7777END'):
                    columns = line.split(',')
                    columns = [col.strip() for col in columns if col.strip()]  

                    if columns[-1] == '=':
                        columns = columns[:-1]

                    if len(columns) < 12:
                        columns += [None] * (12 - len(columns))

                    try:
                        reg_id = columns[0]
                        tm_st = pendulum.parse(columns[1], strict=False) if columns[1] else None
                        tm_ed = pendulum.parse(columns[2], strict=False) if columns[2] else None
                        mod = columns[3]
                        stn = columns[4]
                        c = columns[5]
                        sky = columns[6]
                        pre = columns[7]
                        conf = columns[8]
                        wf = columns[9]
                        rn_st = columns[10]
                        data.append((reg_id, tm_st, tm_ed, mod, stn, c, sky, pre, conf, wf, rn_st))
                    except ValueError as e:
                        logging.warning(f"행을 파싱하는 중 오류 발생: {e}")
            
            if data:
                df = pd.DataFrame(data, columns=['REG_ID', 'TM_ST', 'TM_ED', 'MODE_KEY', 'STN_ID', 'CNT_CD', 'WF_SKY_CD', 'WF_PRE_CD', 'CONF_LV', 'WF_INFO', 'RN_ST'])

                year = tm_st.strftime('%Y')
                month = tm_st.strftime('%m')
                day = tm_st.strftime('%d')
                hour = tm_st.strftime('%H')
                formatted_date = tm_st.strftime('%Y_%m_%d_%H')

                s3_hook = S3Hook(aws_conn_id='AWS_S3')
                bucket_name = 'team-okky-1-bucket'
                s3_key = f'fct_afs_wl/{year}/{month}/{day}/{hour}/{formatted_date}fct_afs_wl.csv'
                
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False, chunksize=100000)
                
                try:
                    s3_hook.load_string(
                        csv_buffer.getvalue(),
                        key=s3_key,
                        bucket_name=bucket_name,
                        replace=True
                    )
                    logging.info(f"저장성공 첫 번째 데이터 행: {data[0]}")
                    logging.info(f"{len(data)} 건 저장되었습니다.")
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

def fct_afs_wl_to_redshift(data_interval_end, **kwargs):
    logging.info("redshift 적재 시작")
    s3_key = kwargs['task_instance'].xcom_pull(task_ids='fct_afs_wl_to_s3', key='s3_key')
    s3_path = f's3://team-okky-1-bucket/{s3_key}'
    s3_hook = S3Hook(aws_conn_id='AWS_S3')
    redshift_hook = PostgresHook(postgres_conn_id='AWS_Redshift')
    bucket_name = 'team-okky-1-bucket'
    
    csv_content = s3_hook.read_key(s3_key, bucket_name)
    logging.info(f"S3 경로: {s3_key}")
    
    csv_reader = csv.reader(StringIO(csv_content))
    header = next(csv_reader)  # Skip 헤더
    
    data = []
    for row in csv_reader:
        try:
            reg_id, tm_st, tm_ed, mood_key, stn_id, cnt_cd, wf_sky_cd, wf_pre_cd, conf_lv, wf_info, rn_st = row
            #data_key = data_interval_end.in_timezone(kst)
            data_key = data_interval_end + pendulum.duration(hours=9)
            created_at = tm_st
            updated_at = tm_st
            data.append((reg_id, tm_st, tm_ed, mood_key, stn_id, cnt_cd, wf_sky_cd, wf_pre_cd, conf_lv, wf_info, rn_st, data_key, created_at, updated_at))
        except ValueError as e:
            logging.warning(f"ERROR : 파싱오류: {row}, error: {e}")
            
    if data:
        logging.info(f"{len(data)} rows 데이터를 읽었습니다.")
        conn = redshift_hook.get_conn()
        cursor = conn.cursor()

        # Redshift에 삽입할 SQL 쿼리 작성
        insert_query = """
            INSERT INTO raw_data.fct_afs_wl_info (REG_ID, TM_ST, TM_ED, MODE_KEY, STN_ID, CNT_CD, WF_SKY_CD, WF_PRE_CD, CONF_LV, WF_INFO, RN_ST, DATA_KEY, CREATED_AT, UPDATED_AT)
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
    'fct_afs_wc_to_s3_and_redshift',
    default_args=default_args,
    description='중기기온예보 s3 & redshift 적재',
    schedule_interval='0 7,19 * * *',
    catchup=True,
    dagrun_timeout=pendulum.duration(hours=2),
    tags=['중기', 'Daily', '2 time', 'raw'],
) as dag:
    dag.timezone = kst
    
    fct_afs_wl_to_s3_task = PythonOperator(
        task_id='fct_afs_wl_to_s3',
        python_callable=fct_afs_wl_to_s3,
        provide_context=True,
        execution_timeout=pendulum.duration(hours=1),
    )
    
    fct_afs_wl_to_redshift_task = PythonOperator(
        task_id='fct_afs_wl_to_redshift',
        python_callable=fct_afs_wl_to_redshift,
        provide_context=True,
        execution_timeout=pendulum.duration(hours=1),
    )

    fct_afs_wl_to_s3_task >> fct_afs_wl_to_redshift_task

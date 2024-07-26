from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import logging
import requests
import pendulum
from airflow.models import Variable

def fct_shrt_reg_to_s3(**kwargs):
    logical_date = kwargs.get('data_interval_end')

    # 한국 시간대 설정
    local_tz = pendulum.timezone("Asia/Seoul")
    local_execution_date = logical_date.in_timezone(local_tz)

    # 정확한 데이터 시간을 계산하기 위해 1시간 빼기
    data_time = local_execution_date.subtract(hours=1)

    if data_time.hour in [5, 11, 17]:
        if data_time.hour == 5:
            current_hour = "05"
            current_time = data_time.strftime('%Y%m%d%H')
            logging.info(f"current_time(KST) : {current_time}")
        elif data_time.hour == 11:
            current_hour = "11"
            current_time = data_time.strftime('%Y%m%d%H')
            logging.info(f"current_time(KST) : {current_time}")
        elif data_time.hour == 17:
            current_hour = "17"
            current_time = data_time.strftime('%Y%m%d%H')
            logging.info(f"current_time(KST) : {current_time}")
    else:
        raise ValueError("이 DAG는 오직 6, 12, 18시에만 동작합니다.")

    url = "https://apihub.kma.go.kr/api/typ01/url/fct_shrt_reg.php?"
    serviceKey = Variable.get("son_api_key")
    params = {
        'mode': 1,
        'disp': 1,
        'help': 0,
        'authKey': serviceKey
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # 200이 아닌 경우 예외 발생

        data = response.text

        if "#START7777" in data and "#7777END" in data:
            lines = data.splitlines()
            header = []
            body = []

            start_index = 0
            for i, line in enumerate(lines):
                if line.startswith("# REG"):
                    header = line.strip().split()[1:]
                    start_index = i + 1 
                    break

            for line in lines[start_index:]:
                if line.startswith('#7777END'):
                    break
                body.append(line.strip().split())
        
            df = pd.DataFrame(body, columns=header)
            logging.info("데이터 프레임 생성 <성공>")
        else:
            logging.error("Invalid data format: Missing start or end markers")
            raise ValueError("Invalid data format: Missing start or end markers")

    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP 에러 발생: {http_err}")
        raise
    except Exception as err:
        logging.error(f"예상하지 못한 에러 발생: {err}")
        raise
        
    df['TM_ST'] = pd.to_datetime(df['TM_ST'], format='%Y%m%d%H%M')
    df['TM_ED'] = pd.to_datetime(df['TM_ED'], format='%Y%m%d%H%M')

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    s3_bucket = 'team-okky-1-bucket'
    s3_folder_path = f"fct_shrt_reg/{local_execution_date.year}/{local_execution_date.strftime('%m')}/{local_execution_date.strftime('%d')}/{current_hour}"
    s3_file_path = f'{s3_folder_path}/fct_shrt_reg_info_{current_time}.csv'

    s3_hook = S3Hook(aws_conn_id='AWS_S3')
    try:
        s3_hook.load_string(
            csv_buffer.getvalue(),
            key=s3_file_path,
            bucket_name=s3_bucket,
            replace=True
        )
        logging.info(f"파일 업로드 성공 (경로 : {s3_file_path})")
    except Exception as e:
        logging.error(f"파일 업로드 실패 (경로 : {s3_file_path}): {e}")

    return s3_file_path

def load_to_redshift(**kwargs):
    logical_date = kwargs.get('data_interval_end')
    
    # 한국 시간대 설정
    local_tz = pendulum.timezone("Asia/Seoul")
    local_execution_date = logical_date.in_timezone(local_tz)

    # 정확한 데이터 시간을 계산하기 위해 1시간 빼기
    data_time = local_execution_date.subtract(hours=1)

    if data_time.hour in [5, 11, 17]:
        if data_time.hour == 5:
            current_time = data_time.strftime('%Y%m%d%H')
            logging.info(f"current_time(KST) : {current_time}")
        elif data_time.hour == 11:
            current_time = data_time.strftime('%Y%m%d%H')
            logging.info(f"current_time(KST) : {current_time}")
        elif data_time.hour == 17:
            current_time = data_time.strftime('%Y%m%d%H')
            logging.info(f"current_time(KST) : {current_time}")
    else:
        raise ValueError("이 DAG는 오직 6, 12, 18시에만 동작합니다.")
    
    ti = kwargs['ti']
    s3_file_path = ti.xcom_pull(task_ids='fct_shrt_reg_to_s3')
    s3_bucket = 'team-okky-1-bucket'
    
    s3_hook = S3Hook(aws_conn_id='AWS_S3')

    s3_object = s3_hook.get_key(s3_file_path, bucket_name=s3_bucket)
    s3_data = s3_object.get()['Body'].read().decode('utf-8')

    df = pd.read_csv(StringIO(s3_data))
    logging.info("데이터프레임 첫 몇 줄:\n%s", df.head())

    # 새로운 컬럼 추가
    df['DATA_KEY'] = pd.to_datetime(current_time, format='%Y%m%d%H', errors='coerce')
    df['CREATE_AT'] = df['TM_ST']
    df['UPDATE_AT'] = df['TM_ST']
    logging.info("데이터프레임 필수 컬럼 추가:\n%s", df.head())

    # 컬럼 형식 변환
    df['TM_ED'] = pd.to_datetime(df['TM_ED'], errors='coerce')
    df['CREATE_AT'] = pd.to_datetime(df['CREATE_AT'], errors='coerce')
    df['UPDATE_AT'] = pd.to_datetime(df['UPDATE_AT'], errors='coerce')

    df_filtered = df[df['TM_ED'].dt.year == 2100]

    logging.info("필터링된 데이터프레임 첫 몇 줄:\n%s", df_filtered.head())

    csv_buffer = StringIO()
    df_filtered.to_csv(csv_buffer, index=False)

    modified_s3_file_path = s3_file_path.replace('fct_shrt_reg_info_', 'modified_fct_shrt_reg_info_')
    s3_hook.load_string(
        csv_buffer.getvalue(),
        key=modified_s3_file_path,
        bucket_name=s3_bucket,
        replace=True
    )

    redshift_hook = PostgresHook(postgres_conn_id='AWS_Redshift')
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()

    truncate_sql = "TRUNCATE TABLE raw_data.fct_shrt_reg_list;"
    
    copy_sql = f"""
        COPY raw_data.fct_shrt_reg_list
        FROM 's3://{s3_bucket}/{modified_s3_file_path}'
        IAM_ROLE 'arn:aws:iam::862327261051:role/service-role/AmazonRedshift-CommandsAccessRole-20240716T180249'
        CSV
        IGNOREHEADER 1
        DATEFORMAT 'auto'
        TIMEFORMAT 'auto';
    """

    try:
        cursor.execute(truncate_sql)
        logging.info("테이블 비우기 성공")

        cursor.execute(copy_sql)
        conn.commit()
        logging.info(f"데이터 적재 성공 'fct_shrt_reg_list'.")
    except Exception as e:
        conn.rollback()
        logging.error(f"데이터 적재 실패: {e}")
    finally:
        cursor.close()
        conn.close()

local_tz = pendulum.timezone("Asia/Seoul") # 스케줄링을 한국 시간 기준으로 하기 위해서 설정

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 22, tzinfo=local_tz),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'fct_shrt_reg_to_s3_redshift_v2',
    default_args=default_args,
    description='단기예보구역 fct_shrt_reg_to_s3 // S3 to Redshift',
    schedule_interval='0 6,12,18 * * *',
    catchup=True
)

fct_shrt_reg_to_s3_task = PythonOperator(
    task_id='fct_shrt_reg_to_s3',
    python_callable=fct_shrt_reg_to_s3,
    provide_context=True,
    dag=dag
)

load_to_redshift_task = PythonOperator(
    task_id='load_to_redshift',
    python_callable=load_to_redshift,
    provide_context=True,
    dag=dag
)

fct_shrt_reg_to_s3_task >> load_to_redshift_task

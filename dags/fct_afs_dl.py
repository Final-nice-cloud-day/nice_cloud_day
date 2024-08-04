from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import pandas as pd
from io import StringIO
import pendulum
import logging
from airflow.models import Variable

kst = pendulum.timezone("Asia/Seoul")  # 스케줄링 한국시간 기준으로

default_args = {
    'owner': 'bongho',
    'start_date': pendulum.datetime(2024, 7, 31, 18, 0, 0, tz=kst),
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5)
}

dag = DAG(
    'fct_afs_dl_to_s3_redshift',
    default_args=default_args,
    description='단기 육상 예보 s3 & Redshift 적재',
    schedule_interval='0 6,12,18 * * *',
    catchup=True,
    tags=['단기', 'Daliy', '3time', 'raw_data']
)

def get_current_time(data_interval_end):
    # 한국 시간대 설정
    kst = pendulum.timezone("Asia/Seoul")
    kst_data_interval_end = data_interval_end.in_timezone(kst)
    
    # 정확한 데이터 시간을 계산하기 위해 1시간 빼기
    data_time = kst_data_interval_end.subtract(hours=1)

    if data_time.hour in [5, 11, 17]:
        return data_time, data_time.strftime('%Y%m%d%H')
    else:
        raise ValueError("이 DAG는 오직 6, 12, 18시에만 동작합니다.")
    
def fct_afs_dl_to_s3(**kwargs):
    run_time = kwargs.get('data_interval_end')

    # 한국 시간대 설정
    kst = pendulum.timezone("Asia/Seoul")
    kst_run_time = run_time.in_timezone(kst)

    # 정확한 데이터 시간을 계산하기 위해 1시간 빼기
    data_time = kst_run_time.subtract(hours=1)

    if data_time.hour in [5, 11, 17]:
        current_time = data_time.strftime('%Y%m%d%H')
        logging.info(f"current_time(KST) : {current_time}")
    else:
        raise ValueError("이 DAG는 오직 6, 12, 18시에만 동작합니다.")

    url = "https://apihub.kma.go.kr/api/typ01/url/fct_afs_dl.php"
    serviceKey = Variable.get("son_api_key")

    params = {
        "authKey": serviceKey,
        "tmfc1": current_time,
        "disp": "1",
        "help": "1"
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()

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
                body.append(line.strip().split(',')[:-1])
        
            df = pd.DataFrame(body, columns=header)
            logging.info("데이터 프레임 생성 <성공>")
        else:
            logging.error("잘못된 데이터 형식 : #START7777와 #7777END가 없음")
            raise ValueError("Invalid data format: Missing start or end markers")

    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP 에러 발생: {http_err}")
        raise
    except Exception as err:
        logging.error(f"예상하지 못한 에러 발생: {err}")
        raise

    df['TM_FC'] = pd.to_datetime(df['TM_FC'], format='%Y%m%d%H%M')
    df['TM_EF'] = pd.to_datetime(df['TM_EF'], format='%Y%m%d%H%M')

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    s3_bucket = 'team-okky-1-bucket'
    s3_folder_path = f"fct_afs_dl/{kst_run_time.year}/{kst_run_time.strftime('%m')}/{kst_run_time.strftime('%d')}/{current_time[-2:]}"
    s3_file_path = f'{s3_folder_path}/fct_afs_dl_{current_time}.csv'

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
    run_time = kwargs.get('data_interval_end')

    # 한국 시간대 설정
    kst = pendulum.timezone("Asia/Seoul")
    kst_run_time = run_time.in_timezone(kst)

    # 정확한 데이터 시간을 계산하기 위해 1시간 빼기
    data_time = kst_run_time.subtract(hours=1)

    if data_time.hour in [5, 11, 17]:
        current_time = data_time.strftime('%Y%m%d%H')
        logging.info(f"current_time(KST) : {current_time}")
    else:
        raise ValueError("이 DAG는 오직 6, 12, 18시에만 동작합니다.")
    
    ti = kwargs['ti']
    s3_file_path = ti.xcom_pull(task_ids='fct_afs_dl_to_s3')
    s3_bucket = 'team-okky-1-bucket'

    s3_hook = S3Hook(aws_conn_id='AWS_S3')
    s3_object = s3_hook.get_key(s3_file_path, bucket_name=s3_bucket)
    s3_data = s3_object.get()['Body'].read().decode('utf-8')

    df = pd.read_csv(StringIO(s3_data))
    logging.info("데이터프레임 첫 몇 줄:\n%s", df.head())
    
    # 새로운 컬럼 추가
    df['DATA_KEY'] = pd.to_datetime(current_time, format='%Y%m%d%H', errors='coerce')
    df['CREATE_AT'] = df['TM_FC']
    df['UPDATE_AT'] = df['TM_FC']
    logging.info("데이터프레임 필수 컬럼 추가:\n%s", df.head())

    df_filtered = df.drop(columns=['STN', 'C', 'MAN_ID', 'MAN_FC'])
    logging.info("필터링된 데이터프레임 첫 몇 줄:\n%s", df_filtered.head())

    csv_buffer = StringIO()
    df_filtered.to_csv(csv_buffer, index=False)

    modified_s3_file_path = s3_file_path.replace('fct_afs_dl_', 'modified_fct_afs_dl_')
    s3_hook.load_string(
        csv_buffer.getvalue(),
        key=modified_s3_file_path,
        bucket_name=s3_bucket,
        replace=True
    )
    
    redshift_hook = PostgresHook(postgres_conn_id='AWS_Redshift')
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()

    copy_sql = f"""
        COPY raw_data.FCT_AFS_DL_INFO
        FROM 's3://{s3_bucket}/{modified_s3_file_path}'
        IAM_ROLE 'arn:aws:iam::862327261051:role/service-role/AmazonRedshift-CommandsAccessRole-20240716T180249'
        CSV
        IGNOREHEADER 1
        DATEFORMAT 'auto'
        TIMEFORMAT 'auto';
    """

    try:
        cursor.execute(copy_sql)
        conn.commit()
        logging.info(f"데이터 적재 성공 'FCT_AFS_DL_INFO'.")
    except Exception as e:
        conn.rollback()
        logging.error(f"데이터 적재 실패: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

fct_afs_dl_to_s3_task = PythonOperator(
    task_id='fct_afs_dl_to_s3',
    python_callable=fct_afs_dl_to_s3,
    provide_context=True,
    dag=dag
)

load_to_redshift_task = PythonOperator(
    task_id='load_to_redshift',
    python_callable=load_to_redshift,
    provide_context=True,
    dag=dag
)

fct_afs_dl_to_s3_task >> load_to_redshift_task
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import pandas as pd
from io import StringIO
import pendulum
import logging
from airflow.models import Variable

def fct_afs_dl_to_s3(**kwargs):
    logical_date = kwargs.get('logical_date')
    
    # 한국 시간대로 설정
    local_tz = pendulum.timezone("Asia/Seoul")
    local_execution_date = logical_date.in_timezone(local_tz)

    if local_execution_date.hour in [6, 12, 18]:
        if local_execution_date.hour == 6:
            current_hour = "05"
            current_time = local_execution_date.replace(hour=5).strftime('%Y%m%d%H')
            logging.info(f"current_time(KST) : {current_time}")
        elif local_execution_date.hour == 12:
            current_hour = "11"
            current_time = local_execution_date.replace(hour=11).strftime('%Y%m%d%H')
            logging.info(f"current_time(KST) : {current_time}")
        elif local_execution_date.hour == 18:
            current_hour = "17"
            current_time = local_execution_date.replace(hour=17).strftime('%Y%m%d%H')
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
                body.append(line.strip().split(',')[:-1])
        
            df = pd.DataFrame(body, columns=header)
            logging.info("데이터 프레임 생성 <성공>")
        else:
            logging.error("Invalid data format: Missing start or end markers")
            raise ValueError("Invalid data format: Missing start or end markers")

    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")
        raise
    except Exception as err:
        logging.error(f"Other error occurred: {err}")
        raise

    df['TM_FC'] = pd.to_datetime(df['TM_FC'], format='%Y%m%d%H%M')
    df['TM_EF'] = pd.to_datetime(df['TM_EF'], format='%Y%m%d%H%M')

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    s3_bucket = 'team-okky-1-bucket'
    s3_folder_path = f"fct_afs_dl/{local_execution_date.year}/{local_execution_date.strftime('%m')}/{local_execution_date.strftime('%d')}/{current_hour}"
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

    # Redshift 연결 설정
    redshift_hook = PostgresHook(postgres_conn_id='AWS_Redshift')
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()

    copy_sql = f"""
        COPY raw_data.WH_FCT_AFS_DL_INFO
        FROM 's3://team-okky-1-bucket/{s3_folder_path}/fct_afs_dl_{current_time}.csv'
        IAM_ROLE 'arn:aws:iam::862327261051:role/service-role/AmazonRedshift-CommandsAccessRole-20240716T180249'
        CSV
        IGNOREHEADER 1
        DATEFORMAT 'auto'
        TIMEFORMAT 'auto';
    """

    try:
        cursor.execute(copy_sql)
        conn.commit()
        logging.info(f"데이터 적재 성공 'WH_FCT_AFS_DL_INFO'.")
    except Exception as e:
        conn.rollback()
        logging.error(f"데이터 적재 실패: {e}")
    finally:
        cursor.close()
        conn.close()


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'fct_afs_dl_to_s3',
    default_args=default_args,
    description='단기육상예보 fct_afs_dl upload to S3 // S3 to Redshift',
    schedule_interval='0 21,3,9 * * *',  # UTC 기준, KST로는 6시, 12시, 18시
    catchup=True # ec2 서버 중지 후 재시작 -> 그 동안 안 들어온 데이터 저장 가능
)

fct_afs_dl_to_s3_task = PythonOperator(
    task_id='fct_afs_dl_to_s3',
    python_callable=fct_afs_dl_to_s3,
    provide_context=True,
    dag=dag
)

fct_afs_dl_to_s3_task

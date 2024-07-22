from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import logging
import requests
import pendulum
from airflow.models import Variable

def fct_shrt_reg_to_s3(**kwargs):
    logical_date = kwargs.get('logical_date')
    
    # 한국 시간대 설정
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
        logging.error(f"HTTP error occurred: {http_err}")
        raise
    except Exception as err:
        logging.error(f"Other error occurred: {err}")
        raise

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


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'fct_shrt_reg_to_s3',
    default_args=default_args,
    description='단기 예보구역 fct_shrt_reg_to_s3',
    schedule_interval='0 21,3,9 * * *',  # UTC 기준, KST로는 6시, 12시, 18시
    catchup=True
)

fct_shrt_reg_to_s3_task = PythonOperator(
    task_id='fct_shrt_reg_to_s3',
    python_callable=fct_shrt_reg_to_s3,
    provide_context=True,
    dag=dag
)

fct_shrt_reg_to_s3_task
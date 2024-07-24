from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime, timedelta
import requests
import pandas as pd
import io
import pendulum
import re

kst = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 1, 7, 0, 0, tzinfo=kst),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def create_url(authKey, category, **kwargs):
    url = f'https://apihub.kma.go.kr/api/{category}?'
    for key, value in kwargs.items():
        url += f'{key}={value}&'
    url += f'authKey={authKey}'
    return url

def get_data(url):
    response = requests.get(url)
    res_txt = response.text
    lines = res_txt.split('\n')

    # 컬럼명 추출 및 정제
    header_line = next(line for line in lines if line.startswith('# REG_UP'))
    columns = [col.strip() for col in re.split(r'\s{2,}', header_line.strip('# '))]
    columns = [re.sub(r'-+$', '', col) for col in columns]

    # 데이터 추출
    data = []
    for line in lines:
        if line.strip() and not line.startswith('#'):
            row = [field.strip() for field in line.split(',')]
            if len(row) >= len(columns):
                data.append(row[:len(columns)])

    # DataFrame 생성
    if data:
        df = pd.DataFrame(data, columns=columns)
        return df
    else:
        print("No data extracted")
        return pd.DataFrame()

def special_weather_to_s3(**kwargs):
    authKey = "MM6wHKaySvyOsBymsnr8UA"
    category = "typ01/url/wrn_now_data.php"
    api_url = create_url(authKey, category)

    now = pendulum.now('Asia/Seoul')
    formatted_date = now.strftime('%Y%m%d%H%M')
    params = {
        # "tmfc1": formatted_date,
        # "tmfc2": formatted_date,
        # "subcd": 11,
        # "disp": 0,
        "help": 0,
    }

    url = create_url(authKey, category, **params)
    df = get_data(url)

    year = now.strftime('%Y')
    month = now.strftime('%m')
    day = now.strftime('%d')
    formatted_date = now.strftime('%Y_%m_%d')

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, date_format='%Y-%m-%d %H:%M')

    s3_hook = S3Hook(aws_conn_id='AWS_S3')
    bucket_name = 'team-okky-1-bucket'
    s3_key = f'special_weather_now/{year}/{month}/{day}/{formatted_date}_special_weather_now.csv'

    try:
        s3_hook.load_string(
            csv_buffer.getvalue(),
            key=s3_key,
            bucket_name=bucket_name,
            replace=True
        )
        print(f"Successfully saved data for {formatted_date}")
    except Exception as e:
        print(f"S3 upload failed for {formatted_date}: {e}")
        raise ValueError(f"S3 upload failed: {e}")
    else:
        print(f"No valid data to insert for {formatted_date}")

with DAG(
    'special_weather_now',
    default_args=default_args,
    description='Special weather data upload to S3',
    schedule_interval='0 7 * * *',
    catchup=False,
) as dag:
    dag.timezone = kst

    special_weather_to_s3_task = PythonOperator(
        task_id='special_weather_now_to_s3',
        python_callable=special_weather_to_s3,
        op_kwargs={},
    )

    special_weather_to_s3_task

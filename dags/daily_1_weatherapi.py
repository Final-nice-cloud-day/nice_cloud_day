from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
import requests
import csv
from io import StringIO
import pendulum
import pandas as pd
from psycopg2.extras import execute_values
import logging

kst = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'chansu',
    'depends_on_past': True,  # 선행작업의존여부
    'start_date': pendulum.datetime(2024, 7, 29, tz=kst),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

def weatherAPI_to_s3(data_interval_end, **kwargs):
    api_url = "http://api.weatherapi.com/v1/forecast.json?"
    api_key = 'df6f070513664f3fb8e183525242807'
    
    params = {
    'q' : 'seoul',
    'key': api_key,
    'days' : 14,
    'lang' : 'ko'
    }
    
    response = requests.get(api_url, params=params)
    logging.info(f"API 상태코드: {response.status_code}")


    if response.status_code == 200:
        data = response.json()
    
    # 필요한 데이터 추출
    forecast_days = data['forecast']['forecastday']
    weather_data = []

    for day in forecast_days:
        date = day['date']
        day_data = day['day']
        tm_key = data_interval_end + pendulum.duration(hours=9)
        year = tm_key.strftime('%Y')
        month = tm_key.strftime('%m')
        day = tm_key.strftime('%d')
        formatted_date = f"{year}_{month}_{day}"
        weather_data.append({
            'date': date,
            'max_temp_c': day_data['maxtemp_c'],
            'min_temp_c': day_data['mintemp_c'],
            'avg_temp_c': day_data['avgtemp_c'],
            'max_wind_kph': day_data['maxwind_kph'],
            'total_precip_mm': day_data['totalprecip_mm'],
            'avg_humidity': day_data['avghumidity'],
            'condition': day_data['condition']['text']
        })
        
    weather_df = pd.DataFrame(weather_data)
    s3_hook = S3Hook(aws_conn_id='AWS_S3')
    bucket_name = 'team-okky-1-bucket'
    s3_key = f'weatherAPI/{year}/{month}/{day}/{formatted_date}weatherAPI.csv'
    
    csv_buffer = StringIO()
    weather_df.to_csv(csv_buffer, index=False)
    
    try:
        s3_hook.load_string(
            csv_buffer.getvalue(),
            key=s3_key,
            bucket_name=bucket_name,
            replace=True
        )
        kwargs['task_instance'].xcom_push(key='s3_key', value=s3_key)
    except Exception as e:
        logging.error(f"S3 업로드 실패: {e}")
        raise ValueError(f"S3 업로드 실패: {e}")
    
def weatherAPI_to_redshift(data_interval_end, **kwargs):
    logging.info("redshift 적재 시작")
    s3_key = kwargs['task_instance'].xcom_pull(task_ids='weatherAPI_to_s3', key='s3_key')
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
            date, max_temp_c, min_temp_c, avg_temp_c, max_wind_kph, total_precip_mm, avg_humidity, condition = row
            #data_key = data_interval_end.in_timezone(kst)
            date = date.replace("-", "")
            data_key = data_interval_end + pendulum.duration(hours=9)
            created_at = data_key
            updated_at = data_key
            data.append((date, max_temp_c, min_temp_c, avg_temp_c, max_wind_kph, total_precip_mm, avg_humidity, condition, data_key, created_at, updated_at))
        except ValueError as e:
            logging.warning(f"ERROR : 파싱오류: {row}, error: {e}")
            
    if data:
        logging.info(f"{len(data)} rows 데이터를 읽었습니다.")
        conn = redshift_hook.get_conn()
        cursor = conn.cursor()

        # Redshift에 삽입할 SQL 쿼리 작성
        insert_query = """
            INSERT INTO raw_data.WeatherAPI_LIST (DATE,MAX_TEMP_C,MIN_TEMP_C ,AVG_TEMP_C,MAX_WIND_KPH,TOTAL_PRECIP_MM ,AVG_HUMIDITY ,CONDITION, DATA_KEY, CREATED_AT, UPDATED_AT)
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
    'weatherAPI_to_s3_redshift_task',
    default_args=default_args,
    description='weatherAPI upload to S3 and Redshift',
    schedule_interval='0 7 * * *',
    catchup=True,
    dagrun_timeout=pendulum.duration(hours=2),
    tags=['중기', 'Daily', '1 time', 'raw'],
) as dag:
    dag.timezone = kst
    
    weatherAPI_to_s3_task = PythonOperator(
        task_id='weatherAPI_to_s3',
        python_callable=weatherAPI_to_s3,
        provide_context=True,
        execution_timeout=pendulum.duration(hours=1),
    )

    weatherAPI_to_redshift_task = PythonOperator(
        task_id='weatherAPI_to_redshift',
        python_callable=weatherAPI_to_redshift,
        provide_context=True,
        execution_timeout=pendulum.duration(hours=1),
    )

    weatherAPI_to_s3_task >> weatherAPI_to_redshift_task


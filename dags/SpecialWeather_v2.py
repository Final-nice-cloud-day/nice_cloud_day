from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import requests
import pandas as pd
import io
import time
import pendulum

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

def special_weather_to_s3(**kwargs):
    api_url = "https://apihub.kma.go.kr/api/typ01/url/wrn_met_data.php?"
    api_key = "b5G4PidGSZORuD4nRvmTow"

    start_date = kwargs.get('start_date')
    end_date = kwargs.get('end_date')
    current_date = start_date

    while current_date < end_date:
        next_date = current_date + timedelta(days=1)
        
        params = {
            "tmfc1": current_date.strftime("%Y%m%d%H%M"),
            "tmfc2": next_date.strftime("%Y%m%d%H%M"),
            "subcd": 11,
            "disp": 0,
            "help": 0,
            "authKey": api_key
        }

        response = requests.get(api_url, params=params)

        if response.status_code == 200:
            response_text = response.text
            lines = response_text.splitlines()
            columns = lines[1].strip('#').split(',')
            columns = [col.strip() for col in columns]
            data_lines = '\n'.join(lines[2:])
            data_io = io.StringIO(data_lines)
            df = pd.read_csv(data_io, names=columns, sep=',', skipinitialspace=True)
            df = df.iloc[:-1, :-1]

            if not df.empty:
                now = pendulum.now('Asia/Seoul')
                data_key = now.strftime('%Y-%m-%d %H:%M')
                df['data_key'] = data_key

                for col in ['TM_FC', 'TM_IN', 'TM_EF']:
                    if not pd.api.types.is_datetime64_any_dtype(df[col]):
                        df[col] = pd.to_datetime(df[col], format='%Y%m%d%H%M').dt.tz_localize('UTC').dt.tz_convert(kst)

                df['TM_FC'] = df['TM_FC'].dt.strftime('%Y-%m-%d %H:%M')
                df['TM_IN'] = df['TM_IN'].dt.strftime('%Y-%m-%d %H:%M')
                df['TM_EF'] = df['TM_EF'].dt.strftime('%Y-%m-%d %H:%M')

                current_time = pd.Timestamp.now(tz=kst)
                df['created_at'] = current_time
                df['updated_at'] = current_time

                year = current_date.strftime('%Y')
                month = current_date.strftime('%m')
                day = current_date.strftime('%d')
                formatted_date = current_date.strftime('%Y_%m_%d')

                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False, date_format='%Y-%m-%d %H:%M')
                
                s3_hook = S3Hook(aws_conn_id='AWS_S3')
                bucket_name = 'team-okky-1-bucket'
                s3_key = f'special_weather/{year}/{month}/{day}/{formatted_date}_special_weather.csv'
                
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
        else:
            print(f"API request failed: {response.status_code}")
            print("Message:", response.text)
            raise ValueError(f"API request failed: {response.status_code}, Message: {response.text}")

        current_date = next_date
        time.sleep(1)

def preprocess_data_in_s3(**kwargs):
    bucket_name = 'team-okky-1-bucket'
    year = kwargs['year']
    month = kwargs['month']
    day = kwargs['day']
    formatted_date = f"{year}_{month}_{day}"
    s3_key = f'special_weather/{year}/{month}/{day}/{formatted_date}_special_weather.csv'
    s3_processed_key = f'special_weather/processed/{year}/{month}/{day}/{formatted_date}_special_weather_processed.csv'

    s3_hook = S3Hook(aws_conn_id='AWS_S3')
    
    try:
        data = s3_hook.read_key(key=s3_key, bucket_name=bucket_name)
        df = pd.read_csv(io.StringIO(data))

        # 여기서 컬럼 수정/제거 작업을 수행합니다.
        df = df.drop(columns=['TM_FC', 'TM_IN'])

        lvl_mapping = {
            '1.0': '예비',
            '2.0': '주의보',
            '3.0': '경보'
        }

        cmd_mapping = {
            '1.0': '발표',
            '2.0': '대치',
            '3.0': '해제',
            '4.0': '대치해제(자동)',
            '5.0': '연장',
            '6.0': '변경',
            '7.0': '변경해제'
        }

        wrn_mapping = {
            'W' : '강풍',
            'R' : '호우',
            'C' : '한파',
            'D' : '건조',
            'O' : '해일',
            'N' : '지진해일',
            'V' : '풍랑',
            'T' : '태풍',
            'S' : '대설',
            'Y' : '황사',
            'H' : '폭염',
            'F' : '안개'
        }

        df['LVL'] = df['LVL'].astype(str).map(lvl_mapping).fillna(df['LVL'])
        df['CMD'] = df['CMD'].astype(str).map(cmd_mapping).fillna(df['CMD'])
        df['WRN'] = df['WRN'].astype(str).map(wrn_mapping).fillna(df['WRN'])
        
        df['STN_ID'] = df['STN']
        df['WRN_LVL'] = df['LVL']
        df['WRN_CMD'] = df['CMD']
        df['WRN_ID'] = df['WRN']

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, date_format='%Y-%m-%d %H:%M')

        s3_hook.load_string(
            csv_buffer.getvalue(),
            key=s3_processed_key,
            bucket_name=bucket_name,
            replace=True
        )
        print(f"Successfully preprocessed data for {formatted_date}")
    except Exception as e:
        print(f"Data preprocessing failed for {formatted_date}: {e}")
        raise ValueError(f"Data preprocessing failed: {e}")

def create_table_if_not_exists(redshift_hook):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS raw_data.WRN_MET_DATA (
        TM_EF TIMESTAMP,
        STN_ID INTEGER,
        REG_ID VARCHAR(50),
        WRN_ID VARCHAR(11),
        WRN_LVL VARCHAR(11),
        WRN_CMD VARCHAR(50),
        WRN_GRD VARCHAR(50),
        CNT INTEGER,
        RPT VARCHAR(50),
        data_key TIMESTAMP,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    );
    """
    redshift_hook.run(create_table_query)

def load_s3_to_redshift(**kwargs):
    bucket_name = 'team-okky-1-bucket'
    year = kwargs['year']
    month = kwargs['month']
    day = kwargs['day']
    formatted_date = f"{year}_{month}_{day}"
    s3_key = f'special_weather/processed/{year}/{month}/{day}/{formatted_date}_special_weather_processed.csv'

    redshift_hook = PostgresHook(postgres_conn_id='redshift_default')

    create_table_if_not_exists(redshift_hook)
    
    copy_query = f"""
    COPY raw_data.WRN_MET_DATA
    FROM 's3://{bucket_name}/{s3_key}'
    IAM_ROLE 'arn:aws:iam::862327261051:role/service-role/AmazonRedshift-CommandsAccessRole-20240716T180249'
    CSV
    IGNOREHEADER 1
    DATEFORMAT 'auto'
    TIMEFORMAT 'auto'
    """
    try:
        redshift_hook.run(copy_query)
        print(f"Successfully copied data for {formatted_date} to Redshift")
    except Exception as e:
        print(f"Redshift copy failed for {formatted_date}: {e}")
        raise ValueError(f"Redshift copy failed: {e}")

with DAG(
    'special_weather_v2',
    default_args=default_args,
    description='Special weather data upload to S3 and Redshift with preprocessing',
    schedule_interval='0 7 * * *',
    catchup=False,
) as dag:
    dag.timezone = kst

    special_weather_to_s3_task = PythonOperator(
        task_id='special_weather_to_s3_v2',
        python_callable=special_weather_to_s3,
        op_kwargs={
            'start_date': datetime(2024, 7, 1),
            'end_date': datetime.now(),
        },
    )

    preprocess_data_task = PythonOperator(
        task_id='preprocess_data_in_s3',
        python_callable=preprocess_data_in_s3,
        op_kwargs={
            'year': '{{ execution_date.strftime("%Y") }}',
            'month': '{{ execution_date.strftime("%m") }}',
            'day': '{{ execution_date.strftime("%d") }}',
        },
    )

    load_s3_to_redshift_task = PythonOperator(
        task_id='load_s3_to_redshift',
        python_callable=load_s3_to_redshift,
        op_kwargs={
            'year': '{{ execution_date.strftime("%Y") }}',
            'month': '{{ execution_date.strftime("%m") }}',
            'day': '{{ execution_date.strftime("%d") }}',
        },
    )

    special_weather_to_s3_task >> preprocess_data_task >> load_s3_to_redshift_task

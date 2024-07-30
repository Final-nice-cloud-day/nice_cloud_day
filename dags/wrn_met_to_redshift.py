from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
import pendulum
import requests
import pandas as pd
import io
import time

# 타임존 설정
kst = pendulum.timezone("Asia/Seoul")

# 기본 DAG 설정
default_args = {
    'owner': 'doyoung',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2022, 6, 27, 7, 0, 0, tz=kst),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

def special_weather_to_s3(**kwargs):
    start_date = pendulum.parse(kwargs['ds'], tz=kst)
    end_date = start_date.add(days=1)
    api_url = "https://apihub-org.kma.go.kr/api/typ01/url/wrn_met_data.php?"
    api_key = "SVszY16IQS6bM2NeiIEu4Q"
    s3_hook = S3Hook(aws_conn_id='AWS_S3')
    bucket_name = 'team-okky-1-bucket'

    params = {
        "tmfc1": start_date.format("YYYYMMDDHHmm"),
        "tmfc2": end_date.format("YYYYMMDDHHmm"),
        "disp": 0,
        "help": 0,
        "authKey": api_key
    }

    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()
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
            data_key = now.format('YYYY-MM-DD HH:mm')
            df['data_key'] = data_key

            for col in ['TM_FC', 'TM_IN', 'TM_EF']:
                if not pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = pd.to_datetime(df[col], format='%Y%m%d%H%M').dt.tz_localize('UTC').dt.tz_convert(kst)

            df['TM_FC'] = df['TM_FC'].dt.strftime('%Y-%m-%d %H:%M')
            df['TM_IN'] = df['TM_IN'].dt.strftime('%Y-%m-%d %H:%M')
            df['TM_EF'] = df['TM_EF'].dt.strftime('%Y-%m-%d %H:%M')

            df['created_at'] = pd.to_datetime(df['TM_FC']).dt.strftime('%Y-%m-%d %H:%M')
            df['updated_at'] = pd.to_datetime(df['TM_IN']).dt.strftime('%Y-%m-%d %H:%M')

            year = start_date.format('YYYY')
            month = start_date.format('MM')
            day = start_date.format('DD')
            formatted_date = start_date.format('YYYY_MM_DD')

            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, date_format='%Y-%m-%d %H:%M', encoding='utf-8-sig')
            
            s3_key = f'special_weather/{year}/{month}/{day}/{formatted_date}_special_weather.csv'
            
            s3_hook.load_string(
                csv_buffer.getvalue(),
                key=s3_key,
                bucket_name=bucket_name,
                replace=True
            )
            print(f"Successfully saved data for {formatted_date}")

            # 날짜를 XCom에 푸시
            kwargs['ti'].xcom_push(key='date', value=start_date.format('YYYY-MM-DD'))
        else:
            print(f"No valid data to insert for {start_date.format('YYYY-MM-DD')}")
    except requests.HTTPError as e:
        print(f"API request failed for {start_date.format('YYYY-MM-DD')}: {e}")
        raise
    except Exception as e:
        print(f"Failed to process data for {start_date.format('YYYY-MM-DD')}: {e}")
        raise

def preprocess_data_in_s3(**kwargs):
    date = kwargs['ti'].xcom_pull(task_ids='fetch_weather_data', key='date')
    bucket_name = 'team-okky-1-bucket'
    formatted_date = date.replace('-', '_')
    s3_key = f'special_weather/{date[:4]}/{date[5:7]}/{date[8:10]}/{formatted_date}_special_weather.csv'
    s3_processed_key = f'special_weather/processed/{date[:4]}/{date[5:7]}/{date[8:10]}/{formatted_date}_special_weather_processed.csv'

    s3_hook = S3Hook(aws_conn_id='AWS_S3')
    
    try:
        data = s3_hook.read_key(key=s3_key, bucket_name=bucket_name)
        df = pd.read_csv(io.StringIO(data))

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
            'W': '강풍',
            'R': '호우',
            'C': '한파',
            'D': '건조',
            'O': '해일',
            'N': '지진해일',
            'V': '풍랑',
            'T': '태풍',
            'S': '대설',
            'Y': '황사',
            'H': '폭염',
            'F': '안개'
        }

        df['LVL'] = df['LVL'].astype(str).map(lvl_mapping).fillna(df['LVL'])
        df['CMD'] = df['CMD'].astype(str).map(cmd_mapping).fillna(df['CMD'])
        df['WRN'] = df['WRN'].astype(str).map(wrn_mapping).fillna(df['WRN'])
        
        df['STN'] = df['STN'].astype(float).astype(int)
        df['GRD'] = df['GRD'].astype(int)
        df['CNT'] = df['CNT'].astype(int)
        df['RPT'] = df['RPT'].astype(int)

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, date_format='%Y-%m-%d %H:%M', encoding='utf-8-sig')

        s3_hook.load_string(
            csv_buffer.getvalue(),
            key=s3_processed_key,
            bucket_name=bucket_name,
            replace=True
        )
        print(f"Successfully preprocessed data for {formatted_date}")
    except Exception as e:
        print(f"Data preprocessing failed for {formatted_date}: {e}")
        raise

def load_s3_to_redshift(**kwargs):
    date = kwargs['ti'].xcom_pull(task_ids='fetch_weather_data', key='date')
    bucket_name = 'team-okky-1-bucket'
    formatted_date = date.replace('-', '_')
    s3_key = f'special_weather/processed/{date[:4]}/{date[5:7]}/{date[8:10]}/{formatted_date}_special_weather_processed.csv'

    redshift_hook = PostgresHook(postgres_conn_id='AWS_Redshift')

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
        raise

def create_table_if_not_exists(redshift_hook):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS raw_data.WRN_MET_DATA (
        TM_EF TIMESTAMP,
        STN_ID INTEGER,
        REG_ID VARCHAR(50),
        WRN_ID VARCHAR(11),
        WRN_LVL VARCHAR(11),
        WRN_CMD VARCHAR(11),
        WRN_GRD INTEGER,
        CNT INTEGER,
        RPT INTEGER,
        data_key TIMESTAMP,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
    );
    """
    redshift_hook.run(create_table_query)

def create_seasonal_warning_summary(**kwargs):
    redshift_hook = PostgresHook(postgres_conn_id='AWS_Redshift')
    
    check_table_query = """
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables 
        WHERE table_schema = 'mart_data'
        AND table_name = 'seasonal_warning_summary'
    );
    """
    
    table_exists = redshift_hook.get_first(check_table_query)[0]
    
    if not table_exists:
        create_table_query = """
        CREATE TABLE mart_data.seasonal_warning_summary (
            year INTEGER,
            month INTEGER,
            season VARCHAR(10),
            WRN_ID VARCHAR(11),
            total_warnings INTEGER
        );
        """
        try:
            redshift_hook.run(create_table_query)
            print("Successfully created seasonal_warning_summary table")
        except Exception as e:
            print(f"Failed to create seasonal_warning_summary table: {e}")
            raise
    
    truncate_query = "TRUNCATE TABLE mart_data.seasonal_warning_summary;"
    
    insert_query = """
    INSERT INTO mart_data.seasonal_warning_summary
    SELECT 
        EXTRACT(YEAR FROM TM_EF) AS year,
        EXTRACT(MONTH FROM TM_EF) AS month,
        CASE 
            WHEN EXTRACT(MONTH FROM TM_EF) IN (12, 1, 2) THEN 'Winter'
            WHEN EXTRACT(MONTH FROM TM_EF) IN (3, 4, 5) THEN 'Spring'
            WHEN EXTRACT(MONTH FROM TM_EF) IN (6, 7, 8) THEN 'Summer'
            ELSE 'Fall'
        END AS season,
        WRN_ID,
        COUNT(*) AS total_warnings
    FROM raw_data.WRN_MET_DATA
    GROUP BY year, month, season, WRN_ID
    ORDER BY year, month;
    """
    
    try:
        redshift_hook.run(truncate_query)
        redshift_hook.run(insert_query)
        print("Successfully populated seasonal_warning_summary table")
    except Exception as e:
        print(f"Failed to populate seasonal_warning_summary table: {e}")
        raise

dag = DAG(
    'weather_data_pipeline_v33333',
    default_args=default_args,
    description='A simple DAG to process weather data',
    schedule_interval='0 09,18 * * *',
    start_date=pendulum.datetime(2024, 7, 30, tz=kst),
    catchup=False,
    tags=['특보', 'raw_data', 'mart_data', 'Daily', '2time'],
)

task1 = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=special_weather_to_s3,
    provide_context=True,
    dag=dag,
)

task2 = PythonOperator(
    task_id='preprocess_weather_data',
    python_callable=preprocess_data_in_s3,
    provide_context=True,
    dag=dag,
)

task3 = PythonOperator(
    task_id='load_data_to_redshift',
    python_callable=load_s3_to_redshift,
    provide_context=True,
    dag=dag,
)

task4 = PythonOperator(
    task_id='create_seasonal_warning_summary',
    python_callable=create_seasonal_warning_summary,
    provide_context=True,
    dag=dag,
)

task1 >> task2 >> task3 >> task4

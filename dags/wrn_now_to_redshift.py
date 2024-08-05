from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pendulum

# 한국 표준시(KST) 설정
kst = pendulum.timezone("Asia/Seoul")

# 기본 인자 설정
default_args = {
    'owner': 'doyoung',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 7, 30, 7, 0, 0, tz=kst),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

def drop_and_create_wrn_now_data(redshift_hook):
    # raw_data.WRN_NOW_DATA 테이블 삭제 후 재생성
    drop_table_query = "DROP TABLE IF EXISTS raw_data.WRN_NOW_DATA;"
    create_table_query = """
    CREATE TABLE raw_data.WRN_NOW_DATA (
        REG_UP VARCHAR(50),
        REG_UP_KO VARCHAR(50),
        REG_ID VARCHAR(50),
        REG_KO VARCHAR(50),
        TM_FC TIMESTAMP,
        TM_EF TIMESTAMP,
        WRN_ID VARCHAR(11),
        WRN_LVL VARCHAR(11),
        WRN_CMD VARCHAR(11),
        ED_TM VARCHAR(110)
    );
    """
    try:
        redshift_hook.run(drop_table_query)
        redshift_hook.run(create_table_query)
        print("raw_data.WRN_NOW_DATA table dropped and created successfully.")
    except Exception as e:
        print(f"Table drop and create failed: {e}")

def load_s3_to_redshift(year, month, day):
    bucket_name = 'team-okky-1-bucket'
    formatted_date = f"{year}_{month}_{day}"
    s3_key = f'special_weather_now/wrn_comparison_target/weather_data.csv'

    redshift_hook = PostgresHook(postgres_conn_id='AWS_Redshift')
    
    drop_and_create_wrn_now_data(redshift_hook)

    copy_query = f"""
    COPY raw_data.WRN_NOW_DATA
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

def truncate_mart_table(redshift_hook):
    # mart_data.WRN_NOW_DATA 테이블 비우기
    truncate_query = "TRUNCATE TABLE mart_data.WRN_NOW_DATA;"
    try:
        redshift_hook.run(truncate_query)
        print("mart_data.WRN_NOW_DATA table truncated successfully.")
    except Exception as e:
        print(f"Truncate failed: {e}")

def copy_raw_to_mart(redshift_hook):
    # raw_data.WRN_NOW_DATA 테이블의 데이터를 mart_data.WRN_NOW_DATA 테이블로 복사
    copy_query = """
    INSERT INTO mart_data.WRN_NOW_DATA (
        REG_UP,
        REG_UP_KO,
        REG_ID,
        REG_KO,
        TM_FC,
        TM_EF,
        WRN_ID,
        WRN_LVL
    )
    SELECT 
        REG_UP,
        REG_UP_KO,
        REG_ID,
        REG_KO,
        TM_FC,
        TM_EF,
        WRN_ID,
        WRN_LVL
    FROM raw_data.WRN_NOW_DATA;
    """
    try:
        redshift_hook.run(copy_query)
        print("Successfully copied data from raw_data.WRN_NOW_DATA to mart_data.WRN_NOW_DATA.")
    except Exception as e:
        print(f"Data copy failed: {e}")

def update_code_column_with_join(redshift_hook):
    # Update query 수정
    update_query = """
    UPDATE mart_data.WRN_NOW_DATA
    SET code = rm.code
    FROM raw_data.WRN_NOW_DATA wn
    INNER JOIN raw_data.region_mapping rm ON wn.REG_UP_KO = rm.region_name
    WHERE mart_data.WRN_NOW_DATA.REG_UP_KO = wn.REG_UP_KO;
    """
    try:
        redshift_hook.run(update_query)
        print("Successfully updated code column using JOIN")
    except Exception as e:
        print(f"Update failed: {e}")

def create_mart_table_if_not_exists(redshift_hook):
    # mart_data.WRN_NOW_DATA 테이블 생성
    create_table_query = """
    CREATE TABLE IF NOT EXISTS mart_data.WRN_NOW_DATA (
        REG_UP VARCHAR(50),
        REG_UP_KO VARCHAR(50),
        REG_ID VARCHAR(50),
        REG_KO VARCHAR(50),
        TM_FC TIMESTAMP,
        TM_EF TIMESTAMP,
        WRN_ID VARCHAR(11),
        WRN_LVL VARCHAR(11),
        code VARCHAR(10)
    );
    """
    try:
        redshift_hook.run(create_table_query)
        print("mart_data.WRN_NOW_DATA table created successfully.")
    except Exception as e:
        print(f"Mart table creation failed: {e}")

# DAG 정의
dag = DAG(
    'wrn_now_data_to_redshift',
    default_args=default_args,
    description='기상 데이터를 처리하고 enrich하는 DAG',
    schedule_interval='0 */4 * * *',
    catchup=False,
    tags=['특보', 'raw', 'mart', 'Daily', '6time']
)

start_date = pendulum.datetime(2024, 7, 30, tz=kst)

# PythonOperator에서 redshift_hook을 전달하는 방식 수정
task1 = PythonOperator(
    task_id='load_data_to_redshift',
    python_callable=load_s3_to_redshift,
    op_args=[start_date.year, start_date.month, start_date.day],
    dag=dag,
)

task2 = PythonOperator(
    task_id='create_mart_table',
    python_callable=create_mart_table_if_not_exists,
    op_args=[PostgresHook(postgres_conn_id='AWS_Redshift')],
    dag=dag,
)

task3 = PythonOperator(
    task_id='truncate_mart_table',
    python_callable=truncate_mart_table,
    op_args=[PostgresHook(postgres_conn_id='AWS_Redshift')],
    dag=dag,
)

task4 = PythonOperator(
    task_id='copy_raw_to_mart',
    python_callable=copy_raw_to_mart,
    op_args=[PostgresHook(postgres_conn_id='AWS_Redshift')],
    dag=dag,
)

task5 = PythonOperator(
    task_id='update_code_column',
    python_callable=update_code_column_with_join,
    op_args=[PostgresHook(postgres_conn_id='AWS_Redshift')],
    dag=dag,
)

# 작업 의존성 설정
task1 >> task2 >> task3 >> task4 >> task5

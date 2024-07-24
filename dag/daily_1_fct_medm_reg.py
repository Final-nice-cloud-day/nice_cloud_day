from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import requests
import csv
from io import StringIO
import pendulum
from psycopg2.extras import execute_values
import logging

kst = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  # 선행작업의존여부N
    'start_date': datetime(2024, 7, 1, 7, 0, 0, tzinfo=kst),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fct_medm_reg_to_s3(**kwargs):
    api_url = "https://apihub.kma.go.kr/api/typ01/url/fct_medm_reg.php?"
    api_key = "HGbLr74hS2qmy6--ITtqog"

    params = {
        'mode': 1,
        'disp': 1,
        'help': 0,
        'authKey': api_key
    }

    response = requests.get(api_url, params=params)

    if response.status_code == 200:
        response_text = response.text
        
        if "#START7777" in response_text and "#7777END" in response_text:
            lines = response_text.splitlines()
            data = []
            
            start_index = 0
            end_index = len(lines)
            
            for i, line in enumerate(lines):
                if line.startswith('# REG_ID'):
                    start_index = i + 1
                elif line.startswith('#7777END'):
                    end_index = i
                    break
            
            for line in lines[start_index:end_index]:
                if line.strip():
                    columns = line.split()
                    if len(columns) >= 5:
                        try:
                            reg_id = columns[0]
                            tm_st = datetime.strptime(columns[1], '%Y%m%d%H%M')
                            tm_ed = datetime.strptime(columns[2], '%Y%m%d%H%M')
                            reg_sp = columns[3]
                            reg_name = ' '.join(columns[4:])
                            data.append((reg_id, tm_st, tm_ed, reg_sp, reg_name))
                        except ValueError as e:
                            logging.warning(f"행을 파싱하는 중 오류 발생: {e}")
            
            if data:
                # s3 버킷 디렉토리 생성 기준을 tm_st 기준으로
                max_tm_st = max(data, key=lambda x: x[1])[1]
                year = max_tm_st.strftime('%Y')
                month = max_tm_st.strftime('%m')
                day = max_tm_st.strftime('%d')
                formatted_date = max_tm_st.strftime('%Y_%m_%d')

                csv_buffer = StringIO()
                csv_writer = csv.writer(csv_buffer)
                csv_writer.writerow(['REG_ID', 'TM_ST', 'TM_ED', 'REG_SP', 'REG_NAME'])
                csv_writer.writerows(data)
                
                s3_hook = S3Hook(aws_conn_id='AWS_S3')
                bucket_name = 'team-okky-1-bucket'
                s3_key = f'fct_medm_reg/{year}/{month}/{day}/{formatted_date}_fct_medm_reg.csv'
                
                try:
                    s3_hook.load_string(
                        csv_buffer.getvalue(),
                        key=s3_key,
                        bucket_name=bucket_name,
                        replace=True
                    )
                    logging.info(f"저장성공 첫 번째 데이터 행: {data[0]}")
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
    
def fct_medm_reg_to_redshift(logical_date, **kwargs):
    logging.info("redshift 적재 시작")
    s3_key = kwargs['task_instance'].xcom_pull(task_ids='fct_medm_reg_to_s3', key='s3_key')
    s3_path = f's3://team-okky-1-bucket/{s3_key}'
    s3_hook = S3Hook(aws_conn_id='AWS_S3')
    bucket_name = 'team-okky-1-bucket'
    redshift_hook = PostgresHook(postgres_conn_id='AWS_Redshift')
    
    csv_content = s3_hook.read_key(s3_key, bucket_name)
    logging.info(f"S3 경로: {s3_key}")
    csv_reader = csv.reader(StringIO(csv_content))
    next(csv_reader)  # 헤더 skip
    
    data = []
    for row in csv_reader:
        try:
            reg_id, tm_st, tm_ed, reg_sp, reg_name = row
            data_key = logical_date + timedelta(hours=9)
            created_at = tm_st
            updated_at = tm_st
            if tm_ed == datetime(2100, 12, 31, 0, 0, 0):
                data.append((reg_id, tm_st, tm_ed, reg_sp, reg_name, data_key, created_at, updated_at))
        except ValueError as e:
            logging.warning(f"ERROR : 파싱오류: {row}, error: {e}")
        
    
    if data:
        logging.info(f"{len(data)} rows 데이터를 읽었습니다.")
        conn = redshift_hook.get_conn()
        cursor = conn.cursor()

        # 적재를 위한 temp
        cursor.execute("""
        CREATE TEMP TABLE temp_fct_medm_reg_list (
            REG_ID VARCHAR(256),
            TM_ST TIMESTAMP,
            TM_ED TIMESTAMP,
            REG_SP VARCHAR(256),
            REG_NAME VARCHAR(256),
            DATA_KEY TIMESTAMP,
            CREATED_AT TIMESTAMP,
            UPDATED_AT TIMESTAMP
        );
        """)
        
        insert_temp_query = """
        INSERT INTO temp_fct_medm_reg_list (REG_ID, TM_ST, TM_ED, REG_SP, REG_NAME, DATA_KEY, CREATED_AT, UPDATED_AT)
        VALUES %s;
        """
        execute_values(cursor, insert_temp_query, data)
        
        # Insert new records from the temporary table into the main table
        insert_query = """
        INSERT INTO raw_data.fct_medm_reg_list (REG_ID, TM_ST, TM_ED, REG_SP, REG_NAME, DATA_KEY, CREATED_AT, UPDATED_AT)
        SELECT t.REG_ID, t.TM_ST, t.TM_ED, t.REG_SP, t.REG_NAME, t.DATA_KEY, t.CREATED_AT, t.UPDATED_AT
        FROM temp_fct_medm_reg_list t
        LEFT JOIN raw_data.fct_medm_reg_list f
        ON t.REG_ID = f.REG_ID AND t.TM_ST = f.TM_ST AND t.TM_ED = f.TM_ED
        WHERE f.REG_ID IS NULL;
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
    'Daily_1_fct_medm_reg_to_s3_and_redshift',
    default_args=default_args,
    description='fct_medm_reg upload to S3 and redshift',
    schedule_interval='0 7 * * *',
    catchup=False,
) as dag:
    dag.timezone = kst
    
    fct_medm_reg_to_s3_task = PythonOperator(
        task_id='fct_medm_reg_to_s3',
        python_callable=fct_medm_reg_to_s3,
    )
    
    load_to_redshift_task = PythonOperator(
        task_id='fct_medm_reg_to_redshift',
        python_callable=fct_medm_reg_to_redshift,
        provide_context=True,
    )

    fct_medm_reg_to_s3_task >> load_to_redshift_task

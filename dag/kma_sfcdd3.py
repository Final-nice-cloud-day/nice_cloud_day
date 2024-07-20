from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime, timedelta
import requests
import boto3
import csv
from io import StringIO
import pendulum

kst = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  # 선행작업의존여부N
    'start_date': datetime(2024, 7, 21, 9, 0, 0, tzinfo=kst),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
def kma_sfcdd3_to_s3(**kwargs):
    api_url = "https://apihub.kma.go.kr/api/typ01/url/kma_sfcdd3.php?"
    api_key = "HGbLr74hS2qmy6--ITtqog"

    params = {
    'tm1' : '',
    'tm2' : '',
    'stn' : '',
    'help': 0,
    'authKey': api_key
}

    response = requests.get(api_url, params=params)

    if response.status_code == 200:
        response_text = response.text

    if "#START7777" in response_text and "#7777END" in response_text:
        lines = response_text.splitlines()
        area_data = []

        start_index = 0
        for i, line in enumerate(lines):
            if line.startswith('# REG_ID'):
                start_index = i + 1
                break

        for line in lines[start_index:]:
            if line.strip():
                if line.startswith('#7777END'):
                    break
                columns = line.split(',')
                columns = [col.strip() for col in columns if col.strip()]  

                if columns[-1] == '=':
                    columns = columns[:-1]

                
                if len(columns) < 12:
                    columns += [None] * (12 - len(columns))

                try:
                    reg_id = columns[0]
                    tm_fc = datetime.strptime(columns[1], '%Y%m%d%H%M') if columns[1] else None
                    tm_ef = datetime.strptime(columns[2], '%Y%m%d%H%M') if columns[2] else None
                    mod = columns[3]
                    stn = columns[4]
                    c = columns[5]
                    min_val = columns[6]
                    max_val = columns[7]
                    min_l = columns[8]
                    min_h = columns[9]
                    max_l = columns[10]
                    max_h = columns[11]
                    area_data.append((reg_id, tm_fc, tm_ef, mod, stn, c, min_val, max_val, min_l, min_h, max_l, max_h))
                except ValueError as e:
                    print(f"WARN: 잘못된 날짜 형식 - {line}")
            
            if area_data:
                # s3 버킷 디렉토리 생성 기준을 tm_st 기준으로
                max_tm_st = max(area_data, key=lambda x: x[1])[1]
                year = max_tm_st.strftime('%Y')
                month = max_tm_st.strftime('%m')
                day = max_tm_st.strftime('%d')
                formatted_date = max_tm_st.strftime('%Y_%m_%d')

                csv_buffer = StringIO()
                csv_writer = csv.writer(csv_buffer)
                csv_writer.writerow(['REG_ID','TM_FC','TM_EF','MODE_KEY','STN_ID','CNT_CD','MIN_TA','MAX_TA','MIN_L_TA','MIN_H_TA','MAX_L_TA','MAX_H_TA'])
                csv_writer.writerows(area_data)
                
                s3_hook = S3Hook(aws_conn_id='AWS_S3')
                bucket_name = 'team-okky-1-bucket'
                s3_key = f'kma_sfcdd3/{year}/{month}/{day}/{formatted_date}_kma_sfcdd3.csv'
                
                try:
                    s3_hook.load_string(
                        csv_buffer.getvalue(),
                        key=s3_key,
                        bucket_name=bucket_name,
                        replace=True
                    )
                    print("저장성공")
                except Exception as e:
                    print(f"S3 업로드 실패: {e}")
                    raise ValueError(f"S3 업로드 실패: {e}")
            else:
                print("No valid data to insert.")
                raise ValueError("No valid data to insert.")
        else:
            print("저장실패")
            print("오류메세지:", response_text)
            raise ValueError(f"저장실패: 오류메세지: {response_text}")
    else:
        print(f"응답코드오류: {response.status_code}")
        print("메세지:", response.text)
        raise ValueError(f"응답코드오류: {response.status_code}, 메세지: {response.text}")

with DAG(
    'kma_sfcdd3_to_s3',
    default_args=default_args,
    description='kma_sfcdd3 upload to S3',
    schedule_interval='0 9 * * *',
    catchup=False,
) as dag:
    dag.timezone = kst
    
    kma_sfcdd3_to_s3_task = PythonOperator(
        task_id='kma_sfcdd3_to_s3',
        python_callable=kma_sfcdd3_to_s3,
    )

    kma_sfcdd3_to_s3_task

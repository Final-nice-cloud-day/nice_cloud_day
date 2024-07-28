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
    'owner': 'airflow',
    'depends_on_past': False,  # 선행작업의존여부N
    'start_date': pendulum.datetime(2024, 7, 1, tz=kst),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

def parse_fixed_width(line):
    columns = line.split()
    columns = [col.strip() for col in columns if col.strip()]
    
    return columns
    
def kma_sfcdd3_to_s3(**kwargs):
    api_url = "https://apihub.kma.go.kr/api/typ01/url/kma_sfcdd3.php?"
    api_key = "HGbLr74hS2qmy6--ITtqog"
    # 초기적재를 위해서 아래 부분 주석 처리
    # date_str = data_interval_end.strftime('%Y%m%d')
    
    params = {
    'tm1' : '20200101',
    'tm2' : '20240701',
    'disp' : 0 ,
    'stn' : '',
    'help': 0,
    'authKey': api_key
}

    response = requests.get(api_url, params=params)
    logging.info(f"API 상태코드: {response.status_code}")

    if response.status_code == 200:
        response_text = response.text
        logging.info(f"응답 데이터:\n{response_text}")

        if "#START7777" in response_text and "#7777END" in response_text:
            lines = response_text.splitlines()
            data = []

            start_index = 0
            for i, line in enumerate(lines):
                if line.startswith('#START7777'):
                    start_index = i + 5
                    break

            logging.info(f"start_index: {start_index}")

            for line in lines[start_index:]:
                if line.strip():
                    parsed_data = parse_fixed_width(line)
                    data.append(parsed_data)
                    logging.info(f"Parsed data: {parsed_data}")

            if data:   
                df = pd.DataFrame(data, columns=[
                    'TM', 'STN', 'WS_AVG', 'WR_DAY', 'WD_MAX', 'WS_MAX',
                    'WS_MAX_TM', 'WD_INS', 'WS_INS', 'WS_INS_TM', 'TA_AVG',
                    'TA_MAX', 'TA_MAX_TM', 'TA_MIN', 'TA_MIN_TM', 'TD_AVG',
                    'TS_AVG', 'TG_MIN', 'HM_AVG', 'HM_MIN', 'HM_MIN_TM',
                    'PV_AVG', 'EV_S', 'EV_L', 'FG_DUR', 'PA_AVG', 'PS_AVG',
                    'PS_MAX', 'PS_MAX_TM', 'PS_MIN', 'PS_MIN_TM', 'CA_TOT',
                    'SS_DAY', 'SS_DUR', 'SS_CMB', 'SI_DAY', 'SI_60M_MAX',
                    'SI_60M_MAX_TM', 'RN_DAY', 'RN_D99', 'RN_DUR', 'RN_60M_MAX',
                    'RN_60M_MAX_TM', 'RN_10M_MAX', 'RN_10M_MAX_TM', 'RN_POW_MAX',
                    'RN_POW_MAX_TM', 'SD_NEW', 'SD_NEW_TM', 'SD_MAX', 'SD_MAX_TM',
                    'TE_05', 'TE_10', 'TE_15', 'TE_30', 'TE_50'
                ])
                
                tm_key = data[0][0]
                year = tm_key[:4]
                month = tm_key[4:6]
                day = tm_key[6:8]
                formatted_date = f"{year}_{month}_{day}"
                
                s3_hook = S3Hook(aws_conn_id='AWS_S3')
                bucket_name = 'team-okky-1-bucket'
                s3_key = f'kma_sfcdd3/{year}/{month}/{day}/{formatted_date}_kma_sfcdd3.csv'
                
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False, chunksize=100000)
                
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

def kma_stcdd3_to_redshift(data_interval_end, **kwargs):
    logging.info("redshift 적재 시작")
    s3_key = kwargs['task_instance'].xcom_pull(task_ids='kma_sfcdd3_to_s3', key='s3_key')
    s3_path = f's3://team-okky-1-bucket/{s3_key}'
    s3_hook = S3Hook(aws_conn_id='AWS_S3')
    redshift_hook = PostgresHook(postgres_conn_id='AWS_Redshift')
    bucket_name = 'team-okky-1-bucket'
    
    csv_content = s3_hook.read_key(s3_key, bucket_name)
    logging.info(f"S3 경로: {s3_key}")
    
    csv_reader = csv.reader(StringIO(csv_content))
    next(csv_reader)  # Skip 헤더
    
    data = []
    for row in csv_reader:
        try:
            tm, stn, ws_avg, wr_day, wd_max, ws_max, ws_max_tm, wd_ins, ws_ins, ws_ins_tm,  \
                ta_avg, ta_max, ta_max_tm, ta_min, ta_min_tm, td_avg, ts_avg, tg_min, hm_avg, \
                hm_min, hm_min_tm, pv_avg, ev_s, ev_l, fg_dur, pa_avg, ps_avg, ps_max, ps_max_tm, \
                ps_min, ps_min_tm, ca_tot, ss_day, ss_dur, ss_cmb, si_day, si_60m_max, si_60m_max_tm, \
                rn_day, rn_d99, rn_dur, rn_60m_max, rn_60m_max_tm, rn_10m_max, rn_10m_max_tm, rn_pow_max, \
                rn_pow_max_tm, sd_new, sd_new_tm, sd_max, sd_max_tm, te_05, te_10, te_15, te_30, te_50 = row

            #data_key = data_interval_end.in_timezone(kst)
            data_key = data_interval_end + pendulum.duration(hours=9)
            created_at = pendulum.parse(tm, strict=False)
            updated_at = pendulum.parse(tm, strict=False)
            data.append((tm, stn, ws_avg, wr_day, wd_max, ws_max, ws_max_tm, wd_ins, ws_ins, ws_ins_tm, 
                            ta_avg, ta_max, ta_max_tm, ta_min, ta_min_tm, td_avg, ts_avg, tg_min, hm_avg, 
                            hm_min, hm_min_tm, pv_avg, ev_s, ev_l, fg_dur, pa_avg, ps_avg, ps_max, ps_max_tm, 
                            ps_min, ps_min_tm, ca_tot, ss_day, ss_dur, ss_cmb, si_day, si_60m_max, si_60m_max_tm, 
                            rn_day, rn_d99, rn_dur, rn_60m_max, rn_60m_max_tm, rn_10m_max, rn_10m_max_tm, rn_pow_max, 
                            rn_pow_max_tm, sd_new, sd_new_tm, sd_max, sd_max_tm, te_05, te_10, te_15, te_30, te_50, 
                            data_key, created_at, updated_at))
        except ValueError as e:
            logging.warning(f"ERROR : 파싱오류: {row}, error: {e}")
            
    if data:
        logging.info(f"{len(data)} rows 데이터를 읽었습니다.")
        conn = redshift_hook.get_conn()
        cursor = conn.cursor()

        # Redshift에 삽입할 SQL 쿼리 작성
        insert_query = """
            INSERT INTO raw_data.kma_sfcdd3_list (TM_ID, STN_ID, WS_AVG, WR_DAY, WD_MAX, WS_MAX, WS_MAX_TM, WD_INS,
                                                    WS_INS, WS_INS_TM, TA_AVG, TA_MAX, TA_MAX_TM, TA_MIN, TA_MIN_TM,
                                                    TD_AVG, TS_AVG, TG_MIN, HM_AVG, HM_MIN, HM_MIN_TM, PV_AVG, EV_S,
                                                    EV_L, FG_DUR, PA_AVG, PS_AVG, PS_MAX, PS_MAX_TM, PS_MIN, PS_MIN_TM,
                                                    CA_TOT, SS_DAY, SS_DUR, SS_CMB, SI_DAY, SI_60M_MAX, SI_60M_MAX_TM,
                                                    RN_DAY, RN_D99, RN_DUR, RN_60M_MAX, RN_60M_MAX_TM, RN_10M_MAX,
                                                    RN_10M_MAX_TM, RN_POW_MAX, RN_POW_MAX_TM, SD_NEW, SD_NEW_TM, SD_MAX,
                                                    SD_MAX_TM, TE_05, TE_10, TE_15, TE_30, TE_50, DATA_KEY, CREATED_AT, UPDATED_AT)
            VALUES %s;
        """
        logging.info(f"{data}")
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
    'Inital_kma_sfcdd3_to_s3_and_redshift_v1.00',
    default_args=default_args,
    description='kma_sfcdd3 upload to S3',
    schedule_interval=None,
    catchup=False,
    dagrun_timeout=pendulum.duration(hours=2),
    tags=['Inital'],
) as dag:
    dag.timezone = kst
    
    kma_sfcdd3_to_s3_task = PythonOperator(
        task_id='kma_sfcdd3_to_s3',
        python_callable=kma_sfcdd3_to_s3,
        execution_timeout=pendulum.duration(hours=1),
    )
    
    kma_sfcdd3_to_redshift_task = PythonOperator(
        task_id='kma_stcdd3_to_redshift',
        python_callable=kma_stcdd3_to_redshift,
        execution_timeout=pendulum.duration(hours=1),
    )

    kma_sfcdd3_to_s3_task >> kma_sfcdd3_to_redshift_task

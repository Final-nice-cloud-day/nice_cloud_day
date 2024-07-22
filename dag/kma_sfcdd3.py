from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime, timedelta
import requests
import boto3
import csv
from io import StringIO
import pendulum
import logging

kst = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  # 선행작업의존여부N
    'start_date': datetime(2024, 7, 21, 9, 0, 0, tzinfo=kst),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    #'retry_delay': timedelta(minutes=5),
}
def kma_sfcdd3_to_s3(**kwargs):
    api_url = "https://apihub.kma.go.kr/api/typ01/url/kma_sfcdd3.php?"
    api_key = "HGbLr74hS2qmy6--ITtqog"
    params = {
    'tm1' : '20240101',
    'tm2' : '20240701',
    'stn' : '',
    'help': 0,
    'authKey': api_key
}

    response = requests.get(api_url, params=params)
    logging.info(f"API 상태코드: {response.status_code}")

    if response.status_code == 200:
        response_text = response.text

        if "#START7777" in response_text and "#7777END" in response_text:
            lines = response_text.splitlines()
            data = []

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
                        tm = columns[0]
                        stn = columns[1]
                        ws_avg = columns[2]
                        wr_day = columns[3]
                        wd_max = columns[4]
                        ws_max = columns[5]
                        ws_max_tm = columns[6]
                        wd_ins = columns[7]
                        ws_ins = columns[8]
                        ws_ins_tm = columns[9]
                        ta_avg = columns[10]
                        ta_max = columns[11]
                        ta_max_tm = columns[12]
                        ta_min = columns[13]
                        ta_min_tm = columns[14]
                        td_avg = columns[15]
                        ts_avg = columns[16]
                        tg_min = columns[17]
                        hm_avg = columns[18]
                        hm_min = columns[19]
                        hm_min_tm = columns[20]
                        pv_avg = columns[21]
                        ev_s = columns[22]
                        ev_l = columns[23]
                        fg_dur = columns[24]
                        pa_avg = columns[25]
                        ps_avg = columns[26]
                        ps_max = columns[27]
                        ps_max_tm = columns[28]
                        ps_min = columns[29]
                        ps_min_tm = columns[30]
                        ca_tot = columns[31]
                        ss_day = columns[32]
                        ss_dur = columns[33]
                        ss_cmb = columns[34]
                        si_day = columns[35]
                        si_60m_max = columns[36]
                        si_60m_max_tm = columns[37]
                        rn_day = columns[38]
                        rn_d99 = columns[39]
                        rn_dur = columns[40]
                        rn_60m_max = columns[41]
                        rn_60m_max_tm = columns[42]
                        rn_10m_max = columns[43]
                        rn_10m_max_tm = columns[44]
                        rn_pow_max = columns[45]
                        rn_pow_max_tm = columns[46]
                        sd_new = columns[47]
                        sd_new_tm = columns[48]
                        sd_max = columns[49]
                        sd_max_tm = columns[50]
                        te_05 = columns[51]
                        te_10 = columns[52]
                        te_15 = columns[53]
                        te_30 = columns[54]
                        te_50 = columns[55]
                        data.append((tm, stn, ws_avg, wr_day, wd_max, ws_max, ws_max_tm, wd_ins, ws_ins, ws_ins_tm, ta_avg, ta_max, ta_max_tm, ta_min, ta_min_tm, td_avg, ts_avg, tg_min, hm_avg, hm_min, hm_min_tm, pv_avg, ev_s, ev_l, fg_dur, pa_avg, ps_avg, ps_max, ps_max_tm, ps_min, ps_min_tm, ca_tot, ss_day, ss_dur, ss_cmb, si_day, si_60m_max, si_60m_max_tm, rn_day, rn_d99, rn_dur, rn_60m_max, rn_60m_max_tm, rn_10m_max, rn_10m_max_tm, rn_pow_max, rn_pow_max_tm, sd_new, sd_new_tm, sd_max, sd_max_tm, te_05, te_10, te_15, te_30, te_50))
                    except ValueError as e:
                        logging.warning(f"행을 파싱하는 중 오류 발생: {e}")
                
            if data:
                # s3 버킷 디렉토리 생성 기준을 tm_st 기준으로
                year = tm.strftime('%Y')
                month = tm.strftime('%m')
                day = tm.strftime('%d')
                formatted_date = tm.strftime('%Y_%m_%d')

                csv_buffer = StringIO()
                csv_writer = csv.writer(csv_buffer)
                csv_writer.writerow(['TM', 'STN_ID', 'WS_AVG', 'WR_DAY', 'WD_MAX', 'WS_MAX', 'WS_MAX_TM', 'WD_INS', 'WS_INS', 'WS_INS_TM', 'TA_AVG', 'TA_MAX', 'TA_MAX_TM', 'TA_MIN', 'TA_MIN_TM', 'TD_AVG', 'TS_AVG', 'TG_MIN', 'HM_AVG', 'HM_MIN', 'HM_MIN_TM', 'PV_AVG', 'EV_S', 'EV_L', 'FG_DUR', 'PA_AVG', 'PS_AVG', 'PS_MAX', 'PS_MAX_TM', 'PS_MIN', 'PS_MIN_TM', 'CA_TOT', 'SS_DAY', 'SS_DUR', 'SS_CMB', 'SI_DAY', 'SI_60M_MAX', 'SI_60M_MAX_TM', 'RN_DAY', 'RN_D99', 'RN_DUR', 'RN_60M_MAX', 'RN_60M_MAX_TM', 'RN_10M_MAX', 'RN_10M_MAX_TM', 'RN_POW_MAX', 'RN_POW_MAX_TM', 'SD_NEW', 'SD_NEW_TM', 'SD_MAX', 'SD_MAX_TM', 'TE_05', 'TE_10', 'TE_15', 'TE_30', 'TE_50'])
                csv_writer.writerows(data)
                
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

with DAG(
    'kma_sfcdd3_to_s3',
    default_args=default_args,
    description='kma_sfcdd3 upload to S3',
    schedule_interval='0 10 * * *',
    catchup=False,
) as dag:
    dag.timezone = kst
    
    kma_sfcdd3_to_s3_task = PythonOperator(
        task_id='kma_sfcdd3_to_s3',
        python_callable=kma_sfcdd3_to_s3,
    )

    kma_sfcdd3_to_s3_task

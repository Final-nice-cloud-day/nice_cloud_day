from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime, timedelta
import requests
import boto3
import csv
from io import StringIO
from bs4 import BeautifulSoup
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

def parse_fixed_width(line):
    return {
        "tm": line[0:8].strip(),
        "stn": line[9:12].strip(),
        "ws_avg": line[13:17].strip(),
        "wr_day": line[18:22].strip(),
        "wd_max": line[23:25].strip(),
        "ws_max": line[26:30].strip(),
        "ws_max_tm": line[31:35].strip(),
        "wd_ins": line[36:38].strip(),
        "ws_ins": line[39:43].strip(),
        "ws_ins_tm": line[44:48].strip(),
        "ta_avg": line[49:53].strip(),
        "ta_max": line[54:58].strip(),
        "ta_max_tm": line[59:63].strip(),
        "ta_min": line[64:68].strip(),
        "ta_min_tm": line[69:73].strip(),
        "td_avg": line[74:78].strip(),
        "ts_avg": line[79:83].strip(),
        "tg_min": line[84:88].strip(),
        "hm_avg": line[89:94].strip(),
        "hm_min": line[95:99].strip(),
        "hm_min_tm": line[100:104].strip(),
        "pv_avg": line[105:109].strip(),
        "ev_s": line[110:114].strip(),
        "ev_l": line[115:119].strip(),
        "fg_dur": line[120:124].strip(),
        "pa_avg": line[125:130].strip(),
        "ps_avg": line[131:136].strip(),
        "ps_max": line[137:142].strip(),
        "ps_max_tm": line[143:147].strip(),
        "ps_min": line[148:152].strip(),
        "ps_min_tm": line[153:157].strip(),
        "ca_tot": line[158:162].strip(),
        "ss_day": line[163:167].strip(),
        "ss_dur": line[168:172].strip(),
        "ss_cmb": line[173:177].strip(),
        "si_day": line[178:182].strip(),
        "si_60m_max": line[183:187].strip(),
        "si_60m_max_tm": line[188:192].strip(),
        "rn_day": line[193:197].strip(),
        "rn_d99": line[198:202].strip(),
        "rn_dur": line[203:207].strip(),
        "rn_60m_max": line[208:212].strip(),
        "rn_60m_max_tm": line[213:217].strip(),
        "rn_10m_max": line[218:222].strip(),
        "rn_10m_max_tm": line[223:227].strip(),
        "rn_pow_max": line[228:232].strip(),
        "rn_pow_max_tm": line[233:237].strip(),
        "sd_new": line[238:242].strip(),
        "sd_new_tm": line[243:247].strip(),
        "sd_max": line[248:252].strip(),
        "sd_max_tm": line[253:257].strip(),
        "te_05": line[258:262].strip(),
        "te_10": line[263:267].strip(),
        "te_15": line[268:272].strip(),
        "te_30": line[273:277].strip(),
        "te_50": line[278:282].strip()
    }
    
def kma_sfcdd3_to_s3(**kwargs):
    api_url = "https://apihub.kma.go.kr/api/typ01/url/kma_sfcdd3.php?"
    api_key = "HGbLr74hS2qmy6--ITtqog"
    params = {
    'tm1' : '20240101',
    'tm2' : '20240102',
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
            end_index = len(lines)
            
            for i, line in enumerate(lines):
                if line.startswith('#START7777'):
                    start_index = i + 5
                elif line.startswith('#7777END'):
                    end_index = i
                    break
                
            logging.info(f"start_index: {start_index}, end_index: {end_index}")
            
            for line in lines[start_index:end_index]:
                if line.strip():
                    parsed_data = parse_fixed_width(line)
                    data.append(parsed_data)
                    logging.info(f"Parsed data: {parsed_data}")

            if data:
                csv_buffer = StringIO()
                csv_writer = csv.writer(csv_buffer)
                csv_writer.writerow([
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

                for row in data:
                    csv_writer.writerow([
                        row["tm"], row["stn"], row["ws_avg"], row["wr_day"], row["wd_max"], row["ws_max"],
                        row["ws_max_tm"], row["wd_ins"], row["ws_ins"], row["ws_ins_tm"], row["ta_avg"],
                        row["ta_max"], row["ta_max_tm"], row["ta_min"], row["ta_min_tm"], row["td_avg"],
                        row["ts_avg"], row["tg_min"], row["hm_avg"], row["hm_min"], row["hm_min_tm"],
                        row["pv_avg"], row["ev_s"], row["ev_l"], row["fg_dur"], row["pa_avg"], row["ps_avg"],
                        row["ps_max"], row["ps_max_tm"], row["ps_min"], row["ps_min_tm"], row["ca_tot"],
                        row["ss_day"], row["ss_dur"], row["ss_cmb"], row["si_day"], row["si_60m_max"],
                        row["si_60m_max_tm"], row["rn_day"], row["rn_d99"], row["rn_dur"], row["rn_60m_max"],
                        row["rn_60m_max_tm"], row["rn_10m_max"], row["rn_10m_max_tm"], row["rn_pow_max"],
                        row["rn_pow_max_tm"], row["sd_new"], row["sd_new_tm"], row["sd_max"], row["sd_max_tm"],
                        row["te_05"], row["te_10"], row["te_15"], row["te_30"], row["te_50"]
                    ])

                tm = datetime.strptime(data[0]["tm"], '%Y%m%d') if data[0]["tm"] else datetime.min
                year = tm.strftime('%Y')
                month = tm.strftime('%m')
                day = tm.strftime('%d')
                formatted_date = tm.strftime('%Y_%m_%d')

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
    schedule_interval='0 7 * * *',
    catchup=False,
) as dag:
    dag.timezone = kst
    
    kma_sfcdd3_to_s3_task = PythonOperator(
        task_id='kma_sfcdd3_to_s3',
        python_callable=kma_sfcdd3_to_s3,
    )

    kma_sfcdd3_to_s3_task

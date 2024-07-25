import requests
import json
import csv
import time
import logging
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from plugins import s3

redshift_conn_id = "AWS_Redshift" # 'redshift_dev_db'
s3_conn_id = "AWS_S3" # 'aws_conn_choi'
s3_bucket = "team-okky-1-bucket" # 'yonggu-practice-bucket'
data_dir = Variable.get("DATA_DIR")

schema = 'raw_data' # 'yonggu_choi_14'
tables_info = [
    {
        "table_name": "rainfall_info",
        "table_schema": [
            "obs_id int primary key",
            "obs_name varchar(50)",
            "rel_river varchar(20)",
            "lat varchar(20)",
            "lon varchar(20)",
            "gov_agency varchar(30)",
            "opened_at date",
            "first_at timestamp",
            "last_at timestamp",
            "addr varchar(100)",
            "etc_addr varchar(200)",
            "data_key timestamp",
            "created_at timestamp",
            "updated_at timestamp",
        ]
    },
    {
        "table_name": "rainfall_data",
        "table_schema": [
            "obs_id int not null",
            "obs_date timestamp not null",
            "rainfall float",
            "data_key timestamp",
            "created_at timestamp",
            "updated_at timestamp",
            "primary key(obs_id, obs_date)",
        ]
    },
]

default_args = {
    'owner': 'yonggu',
    'start_date': pendulum.datetime(2024, 7, 23, tz='Asia/Seoul'),
    'email': ['yonggu.choi.14@gmail.com'],
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=3),
    'max_active_runs': 1,
}

dag = DAG(
    dag_id="stream_rainfall_collection", # DAG name
    schedule_interval='0 10,15,19 * * *',
    tags=['stream_rainfall_check'],
    catchup=True,
    default_args=default_args 
)

def copy_to_s3(**context):
    table = context["params"]["table"]
    s3_key = context["params"]["s3_key"]
    flag = context["params"]["flag"]
    date = context['task_instance'].xcom_pull(key="return_value", task_ids='collect_entire_stream_rainfall_list')

    local_files_to_upload = []

    # 테이블 정보
    if not flag:
        file_name = 'info/' + table
        local_files_to_upload.append(f"""{data_dir}/{file_name}.csv""")
    # 강수량 데이터
    else:
        for each_date in date:
            file_name = table + '_' + each_date
            local_files_to_upload.append(f"""{data_dir}/{file_name}.csv""")
    
    replace = True
    s3.upload_to_s3(s3_conn_id, s3_bucket, s3_key, local_files_to_upload, replace)

def get_entire_rainfall_list(**context):
    url = f"""https://api.hrfco.go.kr/{Variable.get('water_api_key')}/rainfall/info.json"""
    response = requests.get(url)
    data = json.loads(response.text)
    
    entire_list = [["obs_id", "obs_name", "lat", "lon", "gov_agency", "addr", "etc_addr"]]
    for elements in data["content"]:
        entire_list.append([elements["rfobscd"].strip(), elements["obsnm"].strip(), elements["lat"].strip(), elements["lon"].strip(), 
                            elements["agcnm"].strip(), elements["addr"].strip(), elements["etcaddr"].strip()])
    
    print(f"entire_list_count: {len(entire_list) - 1}")

    with open("/opt/airflow/data/rainfall/list.csv", "w") as file:
        writer = csv.writer(file, quotechar = '"', quoting = csv.QUOTE_ALL)
        writer.writerows(entire_list)

    # 'id' 열 추출
    id_data = [i[0] for i in entire_list]
    id_data_reshape = [[x] for x in id_data]

    with open("/opt/airflow/data/rainfall/extracted_id_list.csv", "w") as file:
        writer = csv.writer(file, quoting = csv.QUOTE_NONE)
        writer.writerows(id_data_reshape)

def read_csv_file(file_path):
    with open(file_path, mode='r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # 헤더 건너뛰기
        
        id_list = [row[0] for row in csv_reader]
        return id_list

def convert_date_pair(dates, now):
    date_pair = []
    curr_end_date = now.format('YYYYMMDD')

    if not dates:
        # 현재 달의 첫날
        start_date = now.set(day=1).format('YYYYMMDD')
        date_pair = [(start_date, curr_end_date, start_date[:6])]
    else:
        for year in dates:
            try:
                year_int = int(year)
                start_date = f"{year}0101"
                end_date = f"{year}1231"
                end_date = pendulum.from_format(end_date, 'YYYYMMDD').date()

                # 오늘에 해당하는 달(ex. 2024-07-17 -> 202407) 이후 데이터는 수집하지 않음
                if now.date() < end_date:
                    last_month_date = now.set(day=1).subtract(days=1)
                    end_date = last_month_date.format('YYYYMMDD')
                # 오늘에 해당하는 달 데이터 수집
                else:
                    end_date = end_date.format('YYYYMMDD')

                date_pair.append((start_date, end_date, year))
            except ValueError:
                # year가 정수가 아닐 때 처리
                logging.warning(f"Invalid year: {year}")
                continue
    
    return date_pair

def insert_history_time(record, now, today):
    # 새로운 데이터를 저장할 리스트
    obs_id, obs_date, rainfall = record
    
    # 생성 시간: obs_date를 사용
    created_at = pendulum.from_format(obs_date, 'YYYYMMDD').format('YYYY-MM-DD HH:mm:SS')
    
    # 수정 시간: obs_date가 오늘 날짜인 경우 현재 시간으로 설정
    if obs_date == today:
        updated_at = now
    else:
        updated_at = created_at
    
    # 수집 시간: 현재 시간
    data_key = now
    
    # 새로운 레코드 생성
    new_record = record + [data_key, created_at, updated_at]
    
    return new_record

def get_entire_stream_rainfall_list(**kwargs):
    entire_list = [["obs_id", "obs_date", "rainfall", "data_key", "created_at", "updated_at"]]
    row = read_csv_file("/opt/airflow/data/rainfall/extracted_id_list.csv")

    logging.info(f"row count>> {len(row)}")

    dag_date = kwargs["data_interval_end"]
    now = dag_date.in_timezone('Asia/Seoul')
    now_str = now.format('YYYY-MM-DD HH:mm:ss')
    today = now.format('YYYYMMDD')

    # DAG 실행 전 conf 파라미터 전달받아 실행하고자 하는 시작 연도, 종료 연도 입력
    conf = kwargs['dag_run'].conf

    start_date = conf.get('start_date')
    end_date = conf.get('end_date')

    logging.debug(start_date, end_date)

    dates = []
    if (start_date is None) and (end_date is None):
        dates = None
    else:
        for year in range(int(start_date), int(end_date) + 1):
            dates.append(str(year))
    
    date_pair = convert_date_pair(dates, now)

    # 파라미터 전달하지 않은 경우 이번 달에 해당하는 데이터만 수집
    if dates is None:
        dates = [date_pair[0][2],]

    for date in date_pair:
        for id in row:
            url = f"""https://api.hrfco.go.kr/{Variable.get('water_api_key')}/rainfall/list/1D/{id}/{date[0]}/{date[1]}.json"""
            response = requests.get(url)
            data = json.loads(response.text)

            try:
                elements = data["content"]
            except KeyError: # 철원군(삼합교)의 경우 데이터 수집되고 있지 않음
                print(f"{id}: {data}")
                continue

            for element in elements:
                record = [element["rfobscd"].strip(), element["ymdhm"].strip(), element["rf"]]
                new_record = insert_history_time(record, now_str, today)
                entire_list.append(new_record)
            
            time.sleep(0.1)

        with open(f"/opt/airflow/data/rainfall_data_{date[2]}.csv", "w") as file:
            writer = csv.writer(file, quotechar = '"', quoting = csv.QUOTE_ALL)
            writer.writerows(entire_list)
        
        logging.info(f"{date} year - {len(entire_list) - 1} loading complete.")

        # list 초기화
        entire_list = entire_list[:1]
    
    return dates

def get_associate_rainfall_list(**context):
    entire_list = [["obs_id", "rel_river", "opened_at", "first_at", "last_at"]]
    row = read_csv_file("/opt/airflow/data/rainfall/extracted_id_list.csv")

    for id in row:
        url = f"""http://www.wamis.go.kr:8080/wamis/openapi/wkw/rf_obsinfo?obscd={id}&output=json"""
        response = requests.get(url)
        data = json.loads(response.text)
        missing_cnt = 0

        try:
            elements = data["list"][0]
        except:
            print(f"{id}: {data}")
            missing_cnt += 1
            continue

        entire_list.append([str(elements["obscd"]).strip(), str(elements["bbsnnm"]).strip(), str(elements["opendt"]).strip(), 
                            str("NULL" if elements.get("hrdtstart") is None else elements.get("hrdtstart")).strip(), 
                            str("NULL" if elements.get("hrdtend") is None else elements.get("hrdtend")).strip()])
    
    logging.info(f"entire_list_count: {len(entire_list) - 1}, missing_count: {missing_cnt}")

    with open("/opt/airflow/data/rainfall/associate_list.csv", "w") as file:
        writer = csv.writer(file, quotechar = '"', quoting = csv.QUOTE_ALL)
        writer.writerows(entire_list)

task1 = PythonOperator(
    task_id='collect_entire_rainfall_list',
    python_callable=get_entire_rainfall_list,
    dag=dag
)

task2 = PythonOperator(
    task_id='collect_associate_rainfall_list',
    python_callable=get_associate_rainfall_list,
    dag=dag
)

task3 = BashOperator(
    task_id='join_rainfall_info_list',
    bash_command='python /opt/airflow/include/join_rainfall_csv_file.py',
    dag=dag
)

task4 = PythonOperator(
    task_id='collect_entire_stream_rainfall_list',
    python_callable=get_entire_stream_rainfall_list,
    provide_context=True,
    dag=dag
)

task5 = SQLExecuteQueryOperator(
    task_id = 'table_setting_in_redshfit',
    conn_id = redshift_conn_id,
    sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{tables_info[0]["table_name"]} ({",".join(tables_info[0]["table_schema"])});
    CREATE TABLE IF NOT EXISTS {schema}.{tables_info[1]["table_name"]} ({",".join(tables_info[1]["table_schema"])}) DISTSTYLE KEY DISTKEY(obs_id) SORTKEY(obs_date);
    """,
    autocommit = True,
    split_statements = True,
    return_last = False,
    dag = dag
)

task6_1 = PythonOperator(
    task_id = 'copy_{}_to_s3'.format(tables_info[0]["table_name"]),
    python_callable = copy_to_s3,
    params = {
        "table": tables_info[0]["table_name"],
        "s3_key": f"""rainfall/{tables_info[0]["table_name"]}.csv""",
        "flag": False
    },
    dag = dag
)

task6_2 = PythonOperator(
    task_id = 'copy_{}_to_s3'.format(tables_info[1]["table_name"]),
    python_callable = copy_to_s3,
    params = {
        "table": tables_info[1]["table_name"],
        "s3_key": f"""rainfall/{tables_info[1]["table_name"]}_DATE.csv""",
        "flag": True
    },
    dag = dag
)

task7_1 = S3ToRedshiftOperator(
    task_id = 'run_copy_sql_{}'.format(tables_info[0]["table_name"]),
    s3_bucket = s3_bucket,
    s3_key = f"""rainfall/{tables_info[0]["table_name"]}.csv""",
    schema = schema,
    table = tables_info[0]["table_name"],
    column_list = ["obs_id","obs_name","lat","lon","gov_agency","addr","etc_addr","rel_river","opened_at","first_at","last_at","data_key","created_at","updated_at"],
    copy_options = ["csv", "IGNOREHEADER AS 1", "QUOTE AS '\"'", "DELIMITER ','", "EMPTYASNULL", "ACCEPTANYDATE DATEFORMAT AS 'auto'", " TIMEFORMAT AS 'auto'"],
    method = 'REPLACE',
    redshift_conn_id = redshift_conn_id,
    aws_conn_id = s3_conn_id,
    dag = dag
)

task7_2 = S3ToRedshiftOperator(
    task_id = 'run_copy_sql_{}'.format(tables_info[1]["table_name"]),
    s3_bucket = s3_bucket,
    s3_key = f"""rainfall/{tables_info[1]["table_name"]}_""",
    schema = schema,
    table = tables_info[1]["table_name"],
    copy_options = ["csv", "IGNOREHEADER AS 1", "QUOTE AS '\"'", "DELIMITER ','", "TIMEFORMAT AS 'auto'"],
    method = 'UPSERT',
    upsert_keys = ["obs_id", "obs_date"],
    redshift_conn_id = redshift_conn_id,
    aws_conn_id = s3_conn_id,
    dag = dag
)

task1 >> task2 >> task3 >> task4 >> task5 >> task6_1 >> task6_2 >> task7_1 >> task7_2

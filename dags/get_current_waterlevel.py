import requests
import json
import csv
import time

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

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
        "table_name": "water_level_info",
        "table_schema": [
            "obs_id int primary key",
            "obs_name varchar(50)",
            "river varchar(20)",
            "rel_river varchar(20)",
            "lat varchar(20)",
            "lon varchar(20)",
            "gov_agency varchar(30)",
            "opened_at date",
            "first_at timestamp",
            "last_at timestamp",
            "attn_level float",
            "warn_level float",
            "danger_level float",
        ]
    },
    {
        "table_name": "water_level_data",
        "table_schema": [
            "obs_id int primary key",
            "tm_obs timestamp not null",
            "water_level float",
            "flow float",
        ]
    },
]

default_args = {
    'owner': 'yonggu',
    'start_date': datetime(2024, 7, 10),
    'email': ['yonggu.choi.14@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'max_active_runs': 1,
}

dag = DAG(
    dag_id="water_level_collection", # DAG name
    schedule_interval=None,
    tags=['water_level_check'],
    catchup=False,
    default_args=default_args 
)

def copy_to_s3(**context):
    table = context["params"]["table"]
    s3_key = context["params"]["s3_key"]
    date = context["params"]["date"]

    file_name = table + '_' + date if date is not None else 'info/' + table
    local_files_to_upload = [data_dir + '/' + '{}.csv'.format(file_name)]
    replace = True

    s3.upload_to_s3(s3_conn_id, s3_bucket, s3_key, local_files_to_upload, replace)


def get_entire_stream_list(**context):
    url = f"""https://api.hrfco.go.kr/{Variable.get('water_api_key')}/waterlevel/info.json"""
    response = requests.get(url)
    data = json.loads(response.text)
    
    entire_list = [["obs_id", "obs_name", "lat", "lon", "gov_agency", "attn_level", "warn_level", "danger_level"]]
    for elements in data["content"]:
        entire_list.append([elements["wlobscd"].strip(), elements["obsnm"].strip(), elements["lat"].strip(), elements["lon"].strip(), elements["agcnm"].strip(), 
                            elements["attwl"].strip(), elements["wrnwl"].strip(), elements["almwl"].strip()])
    
    print(f"entire_list_count: {len(entire_list) - 1}")

    with open("/opt/airflow/data/waterlevel/list.csv", "w") as file:
        writer = csv.writer(file, quotechar = '"', quoting = csv.QUOTE_ALL)
        writer.writerows(entire_list)

    # 'id' 열 추출
    id_data = [i[0] for i in entire_list]
    id_data_reshape = [[x] for x in id_data]

    with open("/opt/airflow/data/waterlevel/extracted_id_list.csv", "w") as file:
        writer = csv.writer(file, quoting = csv.QUOTE_NONE)
        writer.writerows(id_data_reshape)


def read_csv_file(file_path):
    with open(file_path, mode='r', encoding='utf-8') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # 헤더 건너뛰기
        
        id_list = [row[0] for row in csv_reader]
        return id_list

def convert_date_pair(dates):
    if not dates:
        now = datetime.now()
        # 현재 달의 첫날
        start_date = now.replace(day=1).strftime('%Y%m%d')
        end_date = now.strftime('%Y%m%d')
        dates = [(start_date, end_date, start_date[:6])]
    else:
        result = []
        for year in dates:
            try:
                year_int = int(year)
                start_date = f"{year}0101"
                end_date = f"{year}1231"
                result.append((start_date, end_date, year))
            except ValueError:
                # year가 정수가 아닐 때 처리
                print(f"Invalid year: {year}")
                continue
        dates = result
    
    return dates

def get_entire_stream_waterlevel_list(*dates):
    entire_list = [["obs_id", "updated_at", "water_level", "flow"]]
    row = read_csv_file("/opt/airflow/data/waterlevel/extracted_id_list.csv")

    print(f"row count>> {len(row)}")
    idx = 0

    convert_dates = convert_date_pair(dates)

    for date in convert_dates:
        for id in row:
            url = f"""https://api.hrfco.go.kr/{Variable.get('water_api_key')}/waterlevel/list/1D/{id}/{date[0]}/{date[1]}.json"""
            response = requests.get(url)
            data = json.loads(response.text)

            try:
                elements = data["content"]
            except KeyError: # 철원군(삼합교)의 경우 데이터 수집되고 있지 않음
                print(f"{id}: {data}")
                continue

            for element in elements:
                entire_list.append([element["wlobscd"].strip(), element["ymdhm"].strip(), element["wl"].strip(), element["fw"].strip()])
            print(f"year - idx>> {date} - {idx}")

            idx += 1
            time.sleep(0.5)

        with open(f"/opt/airflow/data/water_level_data_{date[2]}.csv", "w") as file:
            writer = csv.writer(file, quotechar = '"', quoting = csv.QUOTE_ALL)
            writer.writerows(entire_list)

def get_associate_stream_list(**context):
    entire_list = [["obs_id", "river", "rel_river", "opened_at", "first_at", "last_at"]]
    row = read_csv_file("/opt/airflow/data/waterlevel/extracted_id_list.csv")

    for id in row:
        url = f"""http://www.wamis.go.kr:8080/wamis/openapi/wkw/wl_obsinfo?obscd={id}&output=json"""
        response = requests.get(url)
        data = json.loads(response.text)
        missing_cnt = 0

        try:
            elements = data["list"][0]
        except:
            print(f"{id}: {data}")
            missing_cnt += 1
            continue

        entire_list.append([str(elements["wlobscd"]).strip(), str("NULL" if elements.get("rivnm") is None else elements.get("rivnm")).strip(), str(elements["bbsncd"]).strip(), 
                            str(elements["obsopndt"]).strip(), str(elements["sistartobsdh"]).strip() + ":00:00", str(elements["siendobsdh"]).strip() + ":00:00"])
    
    print(f"entire_list_count: {len(entire_list) - 1}, missing_count: {missing_cnt}")

    with open("/opt/airflow/data/waterlevel/associate_list.csv", "w") as file:
        writer = csv.writer(file, quotechar = '"', quoting = csv.QUOTE_ALL)
        writer.writerows(entire_list)

def upload_to_s3():
    s3.upload_to_s3(s3_conn_id, s3_bucket, s3_key, [path], True)

task1 = PythonOperator(
    task_id='collect_entire_stream_list',
    python_callable=get_entire_stream_list,
    dag=dag
)

task2 = PythonOperator(
    task_id='collect_associate_stream_list',
    python_callable=get_associate_stream_list,
    dag=dag
)

task3 = BashOperator(
    task_id='join_stream_info_list',
    bash_command='python /opt/airflow/include/join_waterlevel_csv_file.py',
    dag=dag
)

task4 = PythonOperator(
    task_id='collect_entire_stream_level_list',
    python_callable=get_entire_stream_waterlevel_list,
    #op_args=['2023'],
    dag=dag
)

task5 = SQLExecuteQueryOperator(
    task_id = 'table_setting_in_redshfit',
    conn_id = redshift_conn_id,
    sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{tables_info[0]["table_name"]} ({",".join(tables_info[0]["table_schema"])});
    CREATE TABLE IF NOT EXISTS {schema}.{tables_info[1]["table_name"]} ({",".join(tables_info[1]["table_schema"])});
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
        "s3_key": f"""waterlevel/{tables_info[0]["table_name"]}.csv""",
        "date": None
    },
    dag = dag
)

task6_2 = PythonOperator(
    task_id = 'copy_{}_to_s3'.format(tables_info[1]["table_name"]),
    python_callable = copy_to_s3,
    params = {
        "table": tables_info[1]["table_name"],
        "s3_key": f"""waterlevel/{tables_info[1]["table_name"]}_202407.csv""",
        "date": "202407"
    },
    dag = dag
)

task7_1 = S3ToRedshiftOperator(
    task_id = 'run_copy_sql_{}'.format(tables_info[0]["table_name"]),
    s3_bucket = s3_bucket,
    s3_key = f"""waterlevel/{tables_info[0]["table_name"]}.csv""",
    schema = schema,
    table = tables_info[0]["table_name"],
    column_list = ["obs_id","obs_name","lat","lon","gov_agency","attn_level","warn_level","danger_level","river","rel_river","opened_at","first_at","last_at"],
    copy_options = ["csv", "IGNOREHEADER AS 1", "QUOTE AS '\"'", "DELIMITER ','", "EMPTYASNULL", "DATEFORMAT AS 'auto'", "ACCEPTANYDATE TIMEFORMAT AS 'YYYYMMDDHH24:MI:SS'"],
    method = 'REPLACE',
    redshift_conn_id = redshift_conn_id,
    aws_conn_id = s3_conn_id,
    dag = dag
)

task7_2 = S3ToRedshiftOperator(
    task_id = 'run_copy_sql_{}'.format(tables_info[1]["table_name"]),
    s3_bucket = s3_bucket,
    s3_key = f"""waterlevel/{tables_info[1]["table_name"]}_202407.csv""",
    schema = schema,
    table = tables_info[1]["table_name"],
    copy_options = ["csv", "IGNOREHEADER AS 1", "QUOTE AS '\"'", "DELIMITER ','", "TIMEFORMAT AS 'auto'"],
    method = 'REPLACE',
    redshift_conn_id = redshift_conn_id,
    aws_conn_id = s3_conn_id,
    dag = dag
)

task1 >> task2 >> task3 >> task4 >> task5 >> task6_1 >> task6_2 >> task7_1 >> task7_2

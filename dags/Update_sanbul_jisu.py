import xml.etree.ElementTree as ET

import pandas as pd
import pendulum
import psycopg2
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

API_KEY = "RCcAK3YdiixgFhu5bAW3uDSK94160ypF5MiU5kA754R1vQRNoKcHZIByikIHK/0Q/oCHdxAdhZ1W415xKrFLlA=="
API_URL = "http://apis.data.go.kr/1400377/forestPoint/forestPointListSigunguSearch"

def get_redshift_connection(autocommit: bool = True) -> psycopg2.extensions.connection:
    hook = PostgresHook(postgres_conn_id="AWS_Redshift")
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def fetch_sanbul_data() -> pd.DataFrame:
    params = {
        "serviceKey": API_KEY,
        "pageNo": 1,
        "numOfRows": "300",
        "_type": "xml",
        "excludeForecast": "1"
    }

    response = requests.get(API_URL, params=params)

    try:
        root = ET.fromstring(response.content)
    except ET.ParseError as e:
        print(f"XML Parsing Error: {e}")
        return pd.DataFrame()

    sanbul_items = []
    for item in root.findall(".//item"):
        sanbul_items.append(item)

    sanbul_data = []
    for item in sanbul_items:
        sanbul_data.append({
            "analdate": item.find("analdate").text if item.find("analdate") is not None else None,
            "doname": item.find("doname").text if item.find("doname") is not None else None,
            "upplocalcd": item.find("upplocalcd").text if item.find("upplocalcd") is not None else None,
            "sigun": item.find("sigun").text if item.find("sigun") is not None else None,
            "sigucode": item.find("sigucode").text if item.find("sigucode") is not None else None,
            "maxi": item.find("maxi").text if item.find("maxi") is not None else None,
            "meanavg": item.find("meanavg").text if item.find("meanavg") is not None else None,
            "mini": item.find("mini").text if item.find("mini") is not None else None
        })

    sanbul_df = pd.DataFrame(sanbul_data)
    sanbul_df["analdate"] = sanbul_df["analdate"].astype(str)
    sanbul_df["analdate"] = pd.to_datetime(sanbul_df["analdate"])
    sanbul_df.loc[sanbul_df["doname"] == "전북특별자치도", "doname"] = "전라북도"

    return sanbul_df

def enrich_and_upload_data(**kwargs) -> None:
    sanbul_df = fetch_sanbul_data()

    if sanbul_df.empty:
        print("No data to process.")
        return

    cur = get_redshift_connection()

    #지역 코드 가져오기
    cur.execute("SELECT * FROM mart_data.korea_region_codes")
    rows = cur.fetchall()
    k_regioncode = pd.DataFrame(rows)

    for index, row in k_regioncode.iterrows():
        sanbul_df.loc[sanbul_df["doname"] == row[0], "iso_3166_code"] = row[1]

    now = pendulum.now("Asia/Seoul").format("YYYY-MM-DD HH:mm:ss")

    for index, row in sanbul_df.iterrows():
        cur.execute("""
            INSERT INTO mart_data.sanbul_jisu (sigucode, analdate, doname, upplocalcd, sigun, maxi, meanavg, mini, iso_3166_code, data_key, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (row["sigucode"], row["analdate"], row["doname"], row["upplocalcd"], row["sigun"], row["maxi"], row["meanavg"], row["mini"], row["iso_3166_code"], now, row["analdate"], now))

    cur.close()

default_args = {
    "owner": "wonwoo",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

dag = DAG(
    "Update_Sanbul_Jisu",
    default_args=default_args,
    description="3시간간격 산불지수",
    start_date=pendulum.datetime(2024, 7, 25, tz="Asia/Seoul"),
    schedule_interval="5 0-23/3 * * *",  # 매 3시간 + 5분 마다 실행
    tags=["공공", "Daily", "8 time", "mart"],
    catchup=False
)

update_sanbul_data = PythonOperator(
    task_id="update_sanbul_data",
    python_callable=enrich_and_upload_data,
    provide_context=True,
    dag=dag,
)

update_sanbul_data

import io
import xml.etree.ElementTree as ET

import chardet
import pandas as pd
import pendulum
import psycopg2
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

S3_BUCKET_NAME = "team-okky-1-bucket"
HJDCD_FILE_KEY = "SPI/hjdCd.csv"
SPI_FILE_KEY = "SPI/2024_spi.csv"

def get_filtered_area() -> pd.DataFrame:
    s3 = S3Hook(aws_conn_id="AWS_S3")
    obj = s3.get_key(HJDCD_FILE_KEY, bucket_name=S3_BUCKET_NAME)
    raw_data = obj.get()["Body"].read()
    result = chardet.detect(raw_data)
    encoding = result["encoding"]
    filtered_area = pd.read_csv(io.BytesIO(raw_data), encoding=encoding)  # 감지된 인코딩 사용
    return filtered_area

def get_redshift_connection(autocommit: bool = True) -> psycopg2.extensions.connection:
    hook = PostgresHook(postgres_conn_id="AWS_Redshift")
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def get_spi_data() -> pd.DataFrame:
    key = "RCcAK3YdiixgFhu5bAW3uDSK94160ypF5MiU5kA754R1vQRNoKcHZIByikIHK/0Q/oCHdxAdhZ1W415xKrFLlA=="
    url = "http://apis.data.go.kr/B500001/drghtIdexSpiAnals/analsInfoList"  # 공공데이터 API

    filtered_area = get_filtered_area()
    spi_items = []

    today = pendulum.now("Asia/Seoul")
    last_monday = today.start_of("week").subtract(weeks=1)
    last_sunday = last_monday.end_of("week")
    start_date = last_monday.format("YYYYMMDD")
    end_date = last_sunday.format("YYYYMMDD")

    for index, row in filtered_area.iterrows():
        cd = row["행정구역코드"]

        params = {
            "serviceKey": key,
            "pageNo": 1,
            "numOfRows": "400",
            "hjdCd": cd,
            "stDt": start_date,
            "edDt": end_date
        }

        response = requests.get(url, params=params)

        try:
            root = ET.fromstring(response.content)
        except ET.ParseError as e:
            print(f"XML Parsing error: {e}")
            continue

        # SPI3 항목을 수집합니다.
        for item in root.findall(".//item"):
            dv_elem = item.find("dv")
            if dv_elem is not None and dv_elem.text == "SPI3":
                spi_items.append(item)

    data = []
    for item in spi_items:
        anldt = item.find("anldt").text if item.find("anldt") is not None else None
        anlrst = item.find("anlrst").text if item.find("anlrst") is not None else None
        anlval = item.find("anlval").text if item.find("anlval") is not None else None
        hjdcd = item.find("hjdcd").text if item.find("hjdcd") is not None else None

        data.append({
            "anldt": anldt,
            "anlrst": anlrst,
            "anlval": anlval,
            "hjdcd": hjdcd
        })

    df_spi = pd.DataFrame(data)
    df_spi = df_spi[["anldt", "anlrst", "anlval", "hjdcd"]]
    return df_spi

FLOOD_SPI = 2
DROUGT_SPI = -2

def update_s3_csv() -> None:
    s3 = S3Hook(aws_conn_id="AWS_S3")

    # 기존 CSV 파일 읽기
    obj = s3.get_key(SPI_FILE_KEY, bucket_name=S3_BUCKET_NAME)
    raw_data = obj.get()["Body"].read()
    result = chardet.detect(raw_data)
    encoding = result["encoding"]

    existing_df = pd.read_csv(io.BytesIO(raw_data), encoding=encoding)

    # 새로운 데이터 가져오기
    new_data_df = get_spi_data()

    # 기존 데이터에 새로운 데이터 추가
    updated_df = pd.concat([existing_df, new_data_df], ignore_index=True)

    new_data_df["anlval"] = pd.to_numeric(new_data_df["anlval"], errors="coerce")

    # 메모리에 CSV 파일 저장
    csv_buffer = io.StringIO()
    updated_df.to_csv(csv_buffer, index=False, encoding="euc-kr")

    # S3에 업로드
    s3.load_string(
        string_data=csv_buffer.getvalue(),
        key=SPI_FILE_KEY,
        bucket_name=S3_BUCKET_NAME,
        replace=True
    )

    # 분석된 데이터를 필터링하여 적절한 레코드 찾기
    filtered_df = new_data_df[(new_data_df["anlval"] > FLOOD_SPI) | (new_data_df["anlval"] < DROUGT_SPI)]
    filtered_df["anldt"] = filtered_df["anldt"].astype(str)
    filtered_df["anldt"] = pd.to_datetime(filtered_df["anldt"])

    if not filtered_df.empty:
        # 레드시프트 추가 데이터 삽입
        cur = get_redshift_connection()
        now = pendulum.now("Asia/Seoul").format("YYYY-MM-DD HH:mm:ss")

        flood_count = len(filtered_df[filtered_df["anlval"] > FLOOD_SPI])
        drought_count = len(filtered_df[filtered_df["anlval"] < DROUGT_SPI])

        # SPI 데이터 삽입
        for index, row in filtered_df.iterrows():
            cur.execute("""
                INSERT INTO mart_data.SPI (anldt, anlrst, anlval, hjdcd, data_key, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (row["anldt"], row["anlrst"], row["anlval"], row["hjdcd"], now, row["anldt"], now))

        # FLOOD 및 DROUGHT 컬럼 업데이트
        cur.execute("""
            UPDATE mart_data.natural_disasters
            SET FLOOD = COALESCE(FLOOD, 0) + %s,
                DROUGHT = COALESCE(DROUGHT, 0) + %s,
                updated_at = %s
            WHERE YEAR = '2024'
        """, (flood_count, drought_count, now))

        cur.close()

default_args = {
    "owner": "wonwoo",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

dag = DAG(
    "Update_DISCNT_SPI",
    default_args=default_args,
    description="지난주 SPI지수 및 홍수/가뭄 관측일",
    start_date=pendulum.datetime(2024, 7, 25, tz="Asia/Seoul"),
    schedule_interval="0 0 * * 4",  # 매주 목요일 00시에 실행
    tags=["기상청", "Weekly", "1 time", "mart"],
    catchup=False
)

update_spi_data = PythonOperator(
    task_id="update_spi_data",
    python_callable=update_s3_csv,
    dag=dag,
)

update_spi_data

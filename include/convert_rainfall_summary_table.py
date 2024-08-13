import os
import logging

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

import gspread
from oauth2client.service_account import ServiceAccountCredentials

import pandas as pd
from sqlalchemy import create_engine

def write_variable_to_local_file(variable_name, local_file_path):
    content = Variable.get(variable_name)
    f = open(local_file_path, "w")
    f.write(content)
    f.close()

# 현재 파일의 절대 경로를 기반으로 상대 경로를 절대 경로로 변환
def get_absolute_path(relative_path):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(dir_path, relative_path)

# Google Sheets API 설정
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
gs_json_file_path = get_absolute_path('./de3-practice-lakestar77-credentials.json')
write_variable_to_local_file('gsheet_access_token', gs_json_file_path)

creds = ServiceAccountCredentials.from_json_keyfile_name(gs_json_file_path, scope)
client = gspread.authorize(creds)

# Google Sheets에서 데이터 가져오기
spreadsheet = client.open("localinfo_list")
sheet = spreadsheet.worksheet("localname_list")
data = sheet.get_all_records()

# 데이터 프레임으로 변환
df = pd.DataFrame(data)

# Redshift에 연결 설정
engine = PostgresHook(postgres_conn_id='AWS_Redshift').get_sqlalchemy_engine()
engine.execute("DROP VIEW IF EXISTS mart_data.rainfall_data_local_summary_final CASCADE;")
engine.execute("DROP VIEW IF EXISTS mart_data.rainfall_data_local_summary CASCADE;")

# 데이터 프레임을 Redshift 테이블로 로드
df.to_sql(name='sido_mapping', schema='raw_data', con=engine, index=False, if_exists='replace')

# Redshift SQL 쿼리 실행
query = """
CREATE OR REPLACE VIEW mart_data.rainfall_data_local_summary AS
WITH river_sido_counts AS (
    SELECT
        rel_river,
        split_part(addr, ' ', 1) as sido,
        COUNT(sido) as sido_count
    FROM raw_data.rainfall_info
    WHERE sido IS NOT NULL
    GROUP BY rel_river, sido
),
total_river_counts AS (
    SELECT
        rel_river,
        SUM(sido_count) as total_sido_count
    FROM river_sido_counts
    GROUP BY rel_river
),
filtered_sido_rivers AS (
    SELECT
        rsc.rel_river,
        rsc.sido
    FROM
        river_sido_counts rsc
    INNER JOIN total_river_counts trc
    ON rsc.rel_river = trc.rel_river
    WHERE
        trc.total_sido_count < 20
        OR (trc.total_sido_count >= 20 AND rsc.sido_count >= 3)
)
SELECT
    rd.obs_date,
    ri.rel_river,
    COALESCE(sm.standard_sido, split_part(ri.addr, ' ', 1)) as sido,
    AVG(rd.rainfall) AS avg_rainfall,
    krc.iso_3166_code AS iso_code
FROM
    raw_data.rainfall_data rd
INNER JOIN
    raw_data.rainfall_info ri
    ON rd.obs_id = ri.obs_id
LEFT JOIN
    raw_data.sido_mapping sm
    ON split_part(ri.addr, ' ', 1) = sm.raw_sido
INNER JOIN
    filtered_sido_rivers fsi
    ON ri.rel_river = fsi.rel_river
    AND COALESCE(sm.standard_sido, split_part(ri.addr, ' ', 1)) = fsi.sido
LEFT JOIN
    mart_data.korea_region_codes krc
    ON COALESCE(sm.standard_sido, split_part(ri.addr, ' ', 1)) = krc.region_name
GROUP BY
    rd.obs_date,
    ri.rel_river,
    COALESCE(sm.standard_sido, split_part(ri.addr, ' ', 1)),
    krc.iso_3166_code
ORDER BY
    rd.obs_date DESC;
"""

query_final = """
CREATE OR REPLACE VIEW mart_data.rainfall_data_local_summary_final AS
SELECT obs_date, rel_river, sido, today, yesterday, today - yesterday as diff
FROM (
    SELECT
        obs_date,
        rel_river,
        sido,
        ROUND(avg_rainfall, 2) AS today,
        ROUND(LAG(avg_rainfall) OVER (PARTITION BY rel_river, sido ORDER BY obs_date), 2) AS yesterday
    FROM mart_data.rainfall_data_local_summary
    ORDER BY obs_date DESC
) AS a
ORDER BY obs_date DESC;
"""

query_final_result = """
SELECT * FROM mart_data.rainfall_data_local_summary_final
WHERE obs_date = current_date
ORDER BY obs_date DESC;
"""

with engine.connect() as conn:
    conn.execute(query)
    conn.execute(query_final)
    result_df = pd.read_sql_query(query_final_result, conn)

logging.info(f"today convert length: {len(result_df)}")


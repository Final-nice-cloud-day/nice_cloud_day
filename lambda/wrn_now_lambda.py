import pandas as pd
import numpy as np
import requests
import psycopg2
import psycopg2.extras
import os
import json
import re
from datetime import datetime, timezone, timedelta

def send_slack_message(message):
    slack_webhook_url = '*******'
    payload = {
        'text': message
    }
    response = requests.post(slack_webhook_url, json=payload)
    if response.status_code != 200:
        print(f"Slack 메시지 전송 실패: {response.status_code}, {response.text}")

def format_for_slack(df, source):
    kst = timezone(timedelta(hours=9))
    current_time = datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')

    if source == 'API':
        header = f"|| 특보 발생 || 현재시각: {current_time} || \n"
    elif source == 'Redshift':
        header = f"|| 특보 해제 || 현재시각: {current_time} || \n"
    else:
        header = f"|| 데이터 업데이트 || 현재시각: {current_time} || \n"
    
    message_lines = [header]
    message_lines.append('==================================')

    for _, row in df.iterrows():
        message_lines.append(f"지역 : {row.get('reg_up_ko', 'N/A')}")
        message_lines.append(f"상세 위치 : {row.get('reg_ko', 'N/A')}")
        message_lines.append(f"발효시간 : {row.get('tm_ef', 'N/A')}")
        message_lines.append(f"특보명 : {row.get('wrn_id', 'N/A')}")
        message_lines.append(f"수준 : {row.get('wrn_lvl', 'N/A')}")
        message_lines.append('----------------------------------')
    message_lines.append('==================================')
    return "\n".join(message_lines)

def add_data_key(df):
    kst = timezone(timedelta(hours=9))
    current_time = datetime.now(kst).strftime('%Y-%m-%d %H:%M:%S')
    df['data_key'] = current_time
    return df

def modify_reg_up_ko(df):
    replacements = {
        '강원도': '강원특별자치도',
        '제주도': '제주특별자치도',
        '전북자치도': '전라북도'
    }
    df['REG_UP_KO'] = df['REG_UP_KO'].replace(replacements)
    return df

def upload_to_redshift(df, table_name, conn_params):
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        drop_table_query = "DROP TABLE IF EXISTS raw_data.WRN_NOW_DATA;"
        # Create the table if it doesn't exist
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            REG_UP VARCHAR(50),
            REG_UP_KO VARCHAR(50),
            REG_ID VARCHAR(50),
            REG_KO VARCHAR(50),
            TM_FC TIMESTAMP,
            TM_EF TIMESTAMP,
            WRN_ID VARCHAR(11),
            WRN_LVL VARCHAR(11),
            WRN_CMD VARCHAR(11),
            ED_TM VARCHAR(110),
            data_key TIMESTAMP
        );
        """
        cursor.execute(drop_table_query)
        cursor.execute(create_table_query)

        # Define the insert query
        insert_query = f"""
        INSERT INTO {table_name} (
            REG_UP, REG_UP_KO, REG_ID, REG_KO, TM_FC, TM_EF, WRN_ID, WRN_LVL, WRN_CMD, ED_TM, data_key
        ) VALUES %s;
        """
        data_tuples = [tuple(x) for x in df.values]
        psycopg2.extras.execute_values(cursor, insert_query, data_tuples)
        
        conn.commit()

        truncate_table_query = "TRUNCATE TABLE mart_data.WRN_NOW_DATA;"
        cursor.execute(truncate_table_query)

        insert_mart_data_query = f"""
        INSERT INTO mart_data.WRN_NOW_DATA (
            REG_UP, REG_UP_KO, REG_ID, REG_KO, TM_FC, TM_EF, WRN_ID, WRN_LVL, WRN_CMD, ED_TM, data_key
        ) SELECT
            REG_UP, REG_UP_KO, REG_ID, REG_KO, TM_FC, TM_EF, WRN_ID, WRN_LVL, WRN_CMD, ED_TM, data_key
        FROM raw_data.WRN_NOW_DATA;
        """
        cursor.execute(insert_mart_data_query)

        update_query = """
        UPDATE mart_data.WRN_NOW_DATA
        SET code = rm.iso_3166_code
        FROM raw_data.WRN_NOW_DATA wn
        INNER JOIN mart_data.korea_region_codes rm
        ON wn.REG_UP_KO = rm.region_name
        WHERE mart_data.WRN_NOW_DATA.REG_UP_KO = wn.REG_UP_KO;
        """
        cursor.execute(update_query)
        print("mart_data update")

        conn.commit()
        cursor.close()
        conn.close()
        print("데이터를 Redshift에 성공적으로 업로드했습니다.")
    
    except Exception as e:
        print(f"Redshift 업로드 오류: {str(e)}")

def lambda_handler(event, context):
    url = os.getenv('API_wrn_now')
    table_name = 'raw_data.WRN_NOW_DATA'
    conn_params = {
        'dbname': os.getenv('REDSHIFT_DBNAME'),
        'user': os.getenv('REDSHIFT_USER'),
        'password': os.getenv('REDSHIFT_PASSWORD'),
        'host': os.getenv('REDSHIFT_HOST'),
        'port': os.getenv('REDSHIFT_PORT')
    }
    
    print(f"URL: {url}")
    print(f"Redshift Table Name: {table_name}")

    try:
        # API 호출하여 데이터 가져오기
        response = requests.get(url)
        response.raise_for_status()
        
        # 응답 데이터에서 줄 분리 및 컬럼 추출
        res_txt = response.text
        lines = res_txt.split('\n')
        header_line = next(line for line in lines if line.startswith('# REG_UP'))
        columns = [col.strip() for col in re.split(r'\s{2,}', header_line.strip('# '))]
        columns = [re.sub(r'-+$', '', col) for col in columns]

        # 데이터 추출
        data = []
        for line in lines:
            if line.strip() and not line.startswith('#'):
                row = [field.strip() for field in line.split(',')]
                if len(row) >= len(columns):
                    data.append(row[:len(columns)])

        if data:
            api_df = pd.DataFrame(data, columns=columns)

            # 날짜 형식 변환
            if 'TM_FC' in api_df.columns:
                api_df['TM_FC'] = pd.to_datetime(api_df['TM_FC'], errors='coerce')
            if 'TM_EF' in api_df.columns:
                api_df['TM_EF'] = pd.to_datetime(api_df['TM_EF'], errors='coerce')
            
            # NaN과 빈 문자열을 일관되게 처리
            api_df.replace('', np.nan, inplace=True)

            api_df = modify_reg_up_ko(api_df)


            api_df = api_df.rename(columns={
                'WRN': 'WRN_ID',
                'LVL': 'WRN_LVL',
                'CMD': 'WRN_CMD'
            })

            conn = psycopg2.connect(**conn_params)
            query = f"SELECT * FROM {table_name};"
            existing_df = pd.read_sql(query, conn)
            conn.close()

            api_df = api_df.drop(columns=['data_key'], errors='ignore')
            existing_df = existing_df.drop(columns=['data_key'], errors='ignore')
            api_df = api_df.drop(columns=['ed_tm'], errors='ignore')
            existing_df = existing_df.drop(columns=['ed_tm'], errors='ignore')
            api_df.columns = [col.lower() for col in api_df.columns]

            combined_df = pd.merge(api_df, existing_df, how='outer', indicator=True)
            only_in_api_df = combined_df[combined_df['_merge'] == 'left_only'].drop(columns='_merge')
            only_in_redshift_df = combined_df[combined_df['_merge'] == 'right_only'].drop(columns='_merge')

            api_df = add_data_key(api_df)

            data_changed = False

            if not only_in_api_df.empty:
                print("API 데이터프레임에만 있는 행들:")
                print(only_in_api_df)

                # Slack 메시지 전송 (API에만 있는 데이터)
                slack_message = format_for_slack(only_in_api_df, 'API')
                send_slack_message(slack_message)
                data_changed = True
            
            if not only_in_redshift_df.empty:
                print("Redshift 데이터프레임에만 있는 행들:")
                print(only_in_redshift_df)

                # Slack 메시지 전송 (Redshift에만 있는 데이터)
                slack_message = format_for_slack(only_in_redshift_df, 'Redshift')
                send_slack_message(slack_message)
                data_changed = True

            # Insert new or changed data into Redshift
            if data_changed == True:
                upload_to_redshift(api_df, table_name, conn_params)
            else:
                print("데이터 변경 없음. Redshift 업데이트 불필요.")
        
        else:
            print("API에서 추출된 데이터가 없습니다.")
        
    except Exception as e:
        print(f"오류 발생: {str(e)}")

    return {
        'statusCode': 200,
        'body': json.dumps('처리 완료')
    }

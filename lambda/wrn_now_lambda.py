import pandas as pd
import numpy as np
from io import StringIO
import requests
import boto3
import os
import json
import re
from datetime import datetime, timezone, timedelta

def send_slack_message(message):
    slack_webhook_url = 'https://hooks.slack.com/services/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'
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
    elif source == 'S3':
        header = f"|| 특보 해제 || 현재시각: {current_time} || \n"
    else:
        header = f"|| 데이터 업데이트 || 현재시각: {current_time} || \n"
    
    message_lines = [header]
    message_lines.append('==================================')

    for _, row in df.iterrows():
        message_lines.append(f"지역 : {row.get('REG_UP_KO', 'N/A')}")
        message_lines.append(f"상세 위치 : {row.get('REG_KO', 'N/A')}")
        message_lines.append(f"발효시간 : {row.get('TM_EF', 'N/A')}")
        message_lines.append(f"특보명 : {row.get('WRN', 'N/A')}")
        message_lines.append(f"수준 : {row.get('LVL', 'N/A')}")
        message_lines.append('----------------------------------')
    message_lines.append('==================================')
    return "\n".join(message_lines)

def lambda_handler(event, context):
    url = os.getenv('API_wrn_now')
    bucket_name = os.getenv('S3_wrn_now')
    s3_key = 'special_weather_now/wrn_comparison_target/weather_data.csv'
    
    print(f"URL: {url}")
    print(f"S3 Bucket Name: {bucket_name}")
    print(f"S3 Key: {s3_key}")

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
                api_df['TM_FC'] = pd.to_datetime(api_df['TM_FC'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M')
            if 'TM_EF' in api_df.columns:
                api_df['TM_EF'] = pd.to_datetime(api_df['TM_EF'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M')
            
            # NaN과 빈 문자열을 일관되게 처리
            api_df.replace('', np.nan, inplace=True)

            # S3 클라이언트 생성
            s3_client = boto3.client('s3')
            
            # 기존 CSV 파일을 S3에서 다운로드
            try:
                s3_response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
                s3_data = s3_response['Body'].read().decode('utf-8')
                existing_df = pd.read_csv(StringIO(s3_data))
                
                # NaN과 빈 문자열을 일관되게 처리
                existing_df.replace('', np.nan, inplace=True)
                

                # 데이터프레임 비교: api_df에서만 있는 행과 s3에만 있는 행 찾기
                combined_df = pd.merge(api_df, existing_df, how='outer', indicator=True)
                only_in_api_df = combined_df[combined_df['_merge'] == 'left_only'].drop(columns='_merge')
                only_in_s3_df = combined_df[combined_df['_merge'] == 'right_only'].drop(columns='_merge')
            
                data_changed = False

                if not only_in_api_df.empty:
                    print("API 데이터프레임에만 있는 행들:")
                    print(only_in_api_df)

                    # Slack 메시지 전송 (API에만 있는 데이터)
                    slack_message = format_for_slack(only_in_api_df, 'API')
                    send_slack_message(slack_message)
                    data_changed = True
                
                if not only_in_s3_df.empty:
                    print("S3 데이터프레임에만 있는 행들:")
                    print(only_in_s3_df)

                    # Slack 메시지 전송 (S3에만 있는 데이터)
                    slack_message = format_for_slack(only_in_s3_df, 'S3')
                    send_slack_message(slack_message)
                    data_changed = True
                
                # 데이터가 변경된 경우에만 S3에 새로운 데이터 저장
                if data_changed == True:
                    csv_buffer = StringIO()
                    api_df.to_csv(csv_buffer, index=False)
                    new_data_csv = csv_buffer.getvalue().encode('utf-8')
                    
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=s3_key,
                        Body=new_data_csv,
                        ContentType='text/csv'
                    )
                    print("업데이트된 데이터를 S3에 저장했습니다.")
                else:
                    print("데이터 변경 없음. S3 업데이트 불필요.")
                        
            except s3_client.exceptions.NoSuchKey:
                print("S3에 기존 데이터가 없습니다. 모든 API 데이터가 새로운 데이터입니다.")
                
                # api_df를 바로 S3에 저장
                csv_buffer = StringIO()
                api_df.to_csv(csv_buffer, index=False)
                new_data_csv = csv_buffer.getvalue().encode('utf-8')
                
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=s3_key,
                    Body=new_data_csv,
                    ContentType='text/csv'
                )
                print("API 데이터를 S3에 새로 저장했습니다.")
                
                # 모든 API 데이터를 새로운 데이터로 Slack에 전송
                slack_message = format_for_slack(api_df, 'New')
                send_slack_message(slack_message)
        
        else:
            print("API에서 추출된 데이터가 없습니다.")
        
    except Exception as e:
        print(f"오류 발생: {str(e)}")

    return {
        'statusCode': 200,
        'body': json.dumps('처리 완료')
    }

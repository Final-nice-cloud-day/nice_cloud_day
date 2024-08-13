import json
import os
from io import StringIO
from typing import Any, Dict

import boto3
import pandas as pd
import pendulum
import requests


def create_url(current_date: str) -> tuple[str, dict]:
    url = "https://www.safetydata.go.kr"
    dataname = "/V2/api/DSSP-IF-00247"
    servicekey = "H0NCZD5N2HN4CZLW"
    payloads = {
        "serviceKey": servicekey,
        "returnType": "json",
        "pageNo": "1",
        "numOfRows": "1000",
        "crtDt": current_date
        }
    return url + dataname, payloads

def emr_function() -> Dict[str, Any]:
    current_date = pendulum.now().format("YYYYMMDD")
    year = pendulum.now().format("YYYY")
    month = pendulum.now().format("MM")
    day = pendulum.now().format("DD")

    response = requests.get(*create_url(current_date))

    res_ok = 200

    if response.status_code != res_ok:
        raise Exception(f"API 요청 실패: {response.status_code}")

    api_data = json.loads(response.text)
    emr_df = pd.DataFrame(api_data["body"])
    curr_df = emr_df.sort_values(by="SN")
    curr_df["MSG_CN"] = curr_df["MSG_CN"].str.replace("\r\n", " ", regex=True)

    s3 = boto3.client("s3")
    bucket_name = "team-okky-1-bucket"

    folder_path = f"emergency-disaster-character/{year}/{month}/{day}/"
    file_name = f"emergency_data_{current_date}.csv"
    full_path = folder_path + file_name

    try:
        response = s3.get_object(Bucket=bucket_name, Key=full_path)
        csv_data = response["Body"].read().decode("utf-8")
        pre_df = pd.read_csv(StringIO(csv_data), sep=";", quotechar='"')
        pre_df["MSG_CN"] = pre_df["MSG_CN"].str.replace("\r\n", " ", regex=True)
    except s3.exceptions.NoSuchKey:
        pre_df = pd.DataFrame()

    diff = len(curr_df) - len(pre_df)

    if diff == 0:
        csv_buffer = StringIO()
        curr_df.to_csv(csv_buffer, sep=";", quotechar='"', index=False)
        csv_data = csv_buffer.getvalue()

        s3.put_object(
            Bucket=bucket_name,
            Key="emergency-disaster-character/emergency_data/emergency_data.csv",
            Body=csv_data,
            ContentType="text/csv"
        )

        return {
            "statusCode": 200,
            "body": "신규 데이터 없음",
            "event" : 0
        }
    else:
        csv_buffer = StringIO()
        curr_df.to_csv(csv_buffer, sep=";", quotechar='"', index=False)
        csv_data = csv_buffer.getvalue()

        s3.put_object(
            Bucket=bucket_name,
            Key=full_path,
            Body=csv_data,
            ContentType="text/csv"
        )

        s3.put_object(
            Bucket=bucket_name,
            Key="emergency-disaster-character/emergency_data/emergency_data.csv",
            Body=csv_data,
            ContentType="text/csv"
        )

    new_df = curr_df.iloc[len(pre_df):, :]
    flag = 0
    # Slack 메시지 구성
    for index, row in new_df.iterrows():

        if "지진" in row["DST_SE_NM"] or "지진" in row["MSG_CN"]:
            flag = 1

        message = (
            "<긴급 재난 문자 알림!>\n\n"
            f"<지역> \n{row['RCPTN_RGN_NM']}\n\n"
            f"<발생 시간> \n{row['CRT_DT']}\n\n"
            f"<긴급단계> \n{row['EMRG_STEP_NM']}\n\n"
            f"<재해 구분명> \n{row['DST_SE_NM']}\n\n"
            f"<내용> \n{row['MSG_CN']}"
        )

        slack_webhook_url = os.getenv("slack_web_hook")
        payload = {
            "text": message
        }

        slack_response = requests.post(
            slack_webhook_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"}
        )

        if slack_response.status_code != res_ok:
            raise Exception(
                f"Slack 요청 실패: {slack_response.status_code}, "
                f"응답 내용:\n{slack_response.text}"
            )

    return {
        "statusCode": 200,
        "body": "Slack 알림 전송 성공!",
        "event": flag
    }

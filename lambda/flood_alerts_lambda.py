import io
import json
import os
import urllib.request

import boto3
import pandas as pd
import pendulum

# Initialize S3 client
s3_client = boto3.client("s3", region_name="ap-northeast-2")
s3_bucket_name = "team-okky-1-bucket"
s3_root_folder = "flood"

# Retrieve the API key from environment variables
api_key = os.environ["API_KEY"]

# Define the API URL templates
api_url_template = f"https://api.hrfco.go.kr/{api_key}/fldfct/list/{{}}.json"
latest_api_url = f"https://api.hrfco.go.kr/{api_key}/fldfct/list.json"

def fetch_data(url):
    try:
        with urllib.request.urlopen(url) as response:
            if response.status == 200:
                return json.loads(response.read().decode())
    except Exception as e:
        print(f"Error fetching data from {url}: {e}")
    return None

def get_data_for_date(edt):
    url = api_url_template.format(edt)
    return fetch_data(url)

def get_latest_data():
    return fetch_data(latest_api_url)

def get_latest_s3_data_prefix():
    response = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=s3_root_folder + "/", Delimiter="/")
    if "CommonPrefixes" in response:
        prefixes = [prefix["Prefix"].replace(s3_root_folder + "/", "").strip("/") for prefix in response["CommonPrefixes"]]
        if prefixes:
            return max(prefixes)
    return None

def accumulate_data(since_date):
    accumulated_data = []
    seen_pairs = set()
    current_date = pendulum.from_format(since_date, "YYYYMMDD")
    today = pendulum.now()

    while current_date <= today:
        edt = current_date.format("YYYYMMDD")
        data = get_data_for_date(edt)
        if data and data.get("code") != "990":
            for item in data.get("content", []):
                pair = (item.get("no"), item.get("sttnm"))
                if pair not in seen_pairs:
                    seen_pairs.add(pair)
                    accumulated_data.append(item)
        current_date = current_date.add(days=1)

    return accumulated_data

def save_data_to_s3(df, date, ancnm):
    base_dir = "/tmp/flood_forecast_data"
    os.makedirs(base_dir, exist_ok=True)

    date_dir = os.path.join(base_dir, date)
    os.makedirs(date_dir, exist_ok=True)

    file_name = f"{ancnm}.csv".replace(" ", "_").replace("/", "_")
    file_path = os.path.join(date_dir, file_name)
    df.to_csv(file_path, index=False)

    s3_key = f"{s3_root_folder}/{date}/{file_name}"
    s3_client.upload_file(file_path, s3_bucket_name, s3_key)
    print(f"Uploaded {file_path} to s3://{s3_bucket_name}/{s3_key}")

def save_data_by_date_and_ancnm(data):
    if not data:
        print("No data to save")
        return

    df = pd.DataFrame(data)
    if "ancdt" not in df.columns:
        print("Error: 'ancdt' column is missing from data")
        return

    for date, group in df.groupby(df["ancdt"].str[:8]):
        for ancnm, subgroup in group.groupby("ancnm"):
            save_data_to_s3(subgroup, date, ancnm)

def get_previous_alerts():
    response = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=s3_root_folder + "/", Delimiter="/")
    if "Contents" in response:
        latest_file = max(response["Contents"], key=lambda x: x["LastModified"])
        latest_file_key = latest_file["Key"]
        s3_object = s3_client.get_object(Bucket=s3_bucket_name, Key=latest_file_key)
        file_content = s3_object["Body"].read().decode("utf-8")
        df = pd.read_csv(io.StringIO(file_content))
        return df.to_dict("records")
    return []

def get_sent_alerts():
    try:
        s3_object = s3_client.get_object(Bucket=s3_bucket_name, Key=f"{s3_root_folder}/sent_alerts.json")
        file_content = s3_object["Body"].read().decode("utf-8")
        return json.loads(file_content)
    except s3_client.exceptions.NoSuchKey:
        return []

def save_sent_alerts(sent_alerts):
    s3_client.put_object(
        Bucket=s3_bucket_name,
        Key=f"{s3_root_folder}/sent_alerts.json",
        Body=json.dumps(sent_alerts, ensure_ascii=False).encode("utf-8")
    )

def send_slack_message(alert):
    slack_webhook_url = os.environ["SLACK_WEBHOOK_URL"]
    message = {
        "text": f"*{alert['kind']}*\n"
                f"발표 일시: {alert['ancdt']}\n"
                f"발표자: {alert['ancnm']}\n"
                f"예보 번호: {alert['no']}\n"
                f"지점: {alert['obsnm']}\n"
                f"강명: {alert['rvrnm']}\n"
                f"현재 일시: {alert['sttcurdt']}\n"
                f"현재 수위: {alert['sttcurhgt']}\n"
                f"현재 해발 수위: {alert['sttcursealvl']}\n"
                f"주의 지역: {alert['wrnaranm']}"
    }
    data = json.dumps(message, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(slack_webhook_url, data=data, headers={"Content-Type": "application/json"})
    response = urllib.request.urlopen(req)
    if response.status != 200:
        raise ValueError(f"Request to Slack returned an error {response.status}, the response is:\n{response.read().decode('utf-8')}")

def process_alerts(data):
    previous_alerts = get_previous_alerts() or []
    sent_alerts = get_sent_alerts() or []

    previous_alerts_set = {(alert["no"], alert["sttnm"], alert["ancdt"]) for alert in previous_alerts}
    sent_alerts_set = {(alert["no"], alert["sttnm"], alert["ancdt"]) for alert in sent_alerts}

    new_alerts = []
    for alert in data:
        if "ancdt" in alert and (alert["no"], alert["sttnm"], alert["ancdt"]) not in previous_alerts_set and (alert["no"], alert["sttnm"], alert["ancdt"]) not in sent_alerts_set:
            send_slack_message(alert)
            sent_alerts.append(alert)
            new_alerts.append(alert)

    if new_alerts:
        save_sent_alerts(sent_alerts)
        return "Data saved by date and ancnm, Slack messages sent"
    else:
        return "No new data to save and send message"

def lambda_handler(event, context):
    fetch_latest = event.get("fetch_latest", False)

    if fetch_latest:
        data = get_latest_data()
        if data is None or data.get("code") == "990":
            data = []
    else:
        latest_prefix = get_latest_s3_data_prefix()
        since_date = latest_prefix if latest_prefix else "20240101"
        data = accumulate_data(since_date)

    if data:
        print("Data fetched successfully. Processing...")
        save_data_by_date_and_ancnm(data)
        result = process_alerts(data)
    else:
        result = "No new data to save"

    return {
        "statusCode": 200,
        "body": result
    }

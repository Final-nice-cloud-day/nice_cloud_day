import csv
import os

import pandas as pd


# 현재 파일의 절대 경로를 기반으로 상대 경로를 절대 경로로 변환
def get_absolute_path(relative_path):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(dir_path, relative_path)

# 위도와 경도 변환 함수
def conversion(old):
    try:
        new = old.split("-")
        if len(new) != 3:
            raise ValueError("Invalid input format")
        degrees = int(new[0])
        minutes = int(new[1])
        seconds = int(new[2])
        return degrees + minutes / 60.0 + seconds / 3600.0
    except Exception as e:
        print(f"Error converting {old}: {e}")
        return None

# 데이터 로드
list_data = pd.read_csv(f"{get_absolute_path('../data/waterlevel/list.csv')}", header=0)
associate_data = pd.read_csv(f"{get_absolute_path('../data/waterlevel/associate_list.csv')}", header=0)

# 각 셀의 양쪽 공백 제거
list_data = list_data.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
associate_data = associate_data.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

# 두 데이터프레임 병합
merged_data = pd.merge(list_data, associate_data, left_on="obs_id", right_on="obs_id")

# attn_level, warn_level, danger_level, rel_river, first_at 열의 빈 값 제거 (NaN 또는 빈 문자열)
merged_data = merged_data.dropna(subset=["attn_level", "warn_level", "danger_level", "rel_river", "first_at"])
merged_data = merged_data[(merged_data["attn_level"] != "") & (merged_data["warn_level"] != "") & (merged_data["danger_level"] != "") & (merged_data["rel_river"] != "") & (merged_data["first_at"] != "")]

# 현재 시간
now = pd.Timestamp.today(tz="Asia/Seoul").strftime("%Y%m%d%H:%M:%S")
# 오늘 날짜
today = pd.Timestamp.today(tz="Asia/Seoul").replace(tzinfo=None)

# last_at 컬럼을 datetime 형식으로 변환
merged_data["last_at"] = pd.to_datetime(merged_data["last_at"], format="%Y%m%d%H:%M:%S")

# data_key, created_at, updated_at 컬럼 추가 및 updated_at 포맷 통일
merged_data["data_key"] = now
merged_data["created_at"] = merged_data["first_at"]
merged_data["updated_at"] = merged_data["last_at"].apply(lambda x: now if 0 <= (today - x).total_seconds() < 3600 else x.strftime("%Y%m%d%H:%M:%S"))
merged_data["last_at"] = merged_data["last_at"].dt.strftime("%Y%m%d%H:%M:%S")

# 위도와 경도를 수치형 데이터로 변환
merged_data["lat"] = merged_data["lat"].apply(conversion)
merged_data["lon"] = merged_data["lon"].apply(conversion)

# 결과 저장
output_file_path = f"{get_absolute_path('../data/info/water_level_info.csv')}"
merged_data.to_csv(output_file_path, index=False, quoting=csv.QUOTE_ALL)

# 'obs_id' 열 추출
obs_id_column = merged_data[["obs_id"]]

# 결과 저장 (유효한 obs_id로 업데이트)
output_file_path = f"{get_absolute_path('../data/waterlevel/extracted_id_list.csv')}"
obs_id_column.to_csv(output_file_path, index=False)

print(f"{merged_data.shape}, {obs_id_column.shape}")

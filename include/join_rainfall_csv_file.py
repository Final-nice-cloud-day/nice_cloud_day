import csv
import os

import pandas as pd


# 현재 파일의 절대 경로를 기반으로 상대 경로를 절대 경로로 변환
def get_absolute_path(relative_path: str) -> str:
    dir_path = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(dir_path, relative_path)

# 위도와 경도 변환 함수
def conversion(old: str) -> float:
    split_count = 3
    try:
        new = old.split("-")
        if len(new) != split_count:
            raise ValueError("Invalid input format")
        degrees = int(new[0])
        minutes = int(new[1])
        seconds = int(new[2])
        return degrees + minutes / 60.0 + seconds / 3600.0
    except Exception as e:
        print(f"Error converting {old}: {e}")
        return None

# 데이터 로드
list_data = pd.read_csv(f"{get_absolute_path('../data/rainfall/list.csv')}", header=0)
associate_data = pd.read_csv(
    f"{get_absolute_path('../data/rainfall/associate_list.csv')}", header=0
)

# 각 셀의 양쪽 공백 제거
list_data = list_data.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
associate_data = associate_data.apply(lambda x: x.str.strip() if x.dtype == "object"
                                        else x)

# first_at, last_at 컬럼 데이터 포맷 변환
associate_data["first_at"] = pd.to_datetime(associate_data["first_at"], format="%Y%m%d",
                                            errors="ignore")
associate_data["last_at"] = pd.to_datetime(associate_data["last_at"], format="%Y%m%d",
                                            errors="ignore")

# 두 데이터프레임 병합
merged_data = list_data.merge(associate_data, left_on="obs_id", right_on="obs_id")

# first_at 열의 빈 값 제거
merged_data = merged_data.dropna(subset=["first_at"])
merged_data = merged_data[merged_data["first_at"] != ""]

# 현재 시간
now = pd.Timestamp.today(tz="Asia/Seoul").strftime("%Y%m%d")
# 오늘 날짜
today = pd.Timestamp.today(tz="Asia/Seoul").replace(tzinfo=None)

# last_at 컬럼을 datetime 형식으로 변환
merged_data["last_at"] = pd.to_datetime(merged_data["last_at"], format="%Y%m%d")

# data_key, created_at, updated_at 컬럼 추가 및 updated_at 포맷 통일
ONE_DAY_TIME_CONSTANT = 60 * 60
merged_data["data_key"] = now
merged_data["created_at"] = merged_data["first_at"]
merged_data["updated_at"] = merged_data["last_at"].apply(lambda x: now if 0 <= (today - x).total_seconds() <= ONE_DAY_TIME_CONSTANT else x.strftime("%Y%m%d"))

# 위도와 경도를 수치형 데이터로 변환
merged_data["lat"] = merged_data["lat"].apply(conversion)
merged_data["lon"] = merged_data["lon"].apply(conversion)

# 결과 저장
output_file_path = f"{get_absolute_path('../data/info/rainfall_info.csv')}"
merged_data.to_csv(output_file_path, index=False, quoting=csv.QUOTE_ALL)

# 'obs_id' 열 추출
obs_id_column = merged_data[["obs_id"]]

# 결과 저장 (유효한 obs_id로 업데이트)
output_file_path = f"{get_absolute_path('../data/rainfall/extracted_id_list.csv')}"
obs_id_column.to_csv(output_file_path, index=False)

print(f"{merged_data.shape}, {obs_id_column.shape}")

# 최종프로젝트 보고서 - 오키도키 1조

---

# 프로젝트 개요

## 프로젝트 주제
대한민국 날씨 및 자연재해 데이터 기반 대시보드 및 시스템 구현

## 목표
- 전국 날씨를 볼 수 있는 대시보드 구현
- 여러 API에서 제공하는 날씨 비교 및 최근 N년 해당일자 날씨 정보 제공
- 특보 발생 현황 및 추이 확인 가능한 대시보드 구현
- 연간 자연재해 발생 횟수 및 자연재해 정보 제공 (홍수, 산불, 지진) 
- 당일 긴급재난문자, 홍수예보, 기상특보 발생 시 Slack 알림 구현
- Airflow dag 성공 여부 log → Slack 알림 구현

## 프로젝트 파이프라인
<details>
  
  <summary> 아키텍쳐 </summary>
  
  ![스크린샷 2024-08-06 13 48 13](https://github.com/user-attachments/assets/b2d2ce75-ec2f-448f-9daf-505e4eb8d8e2)

</details>

- Airflow를 통해 API에서 제공하는 데이터를 S3와 Redshift에 저장
- 저장된 데이터를 바탕으로 마트테이블 생성 및 대시보드 연결
- AWS Lambda를 이용한 실시간 데이터 처리 및 슬랙 알림 설정

<details>
  
  <summary> Data flow </summary>
  
  ![prj_1-데이터흐름도](https://github.com/user-attachments/assets/1a338a65-495d-4f5c-87dd-f258718ccead)

</details>

## 활용 기술 및 프레임워크

| 데이터 파이프라인 | Apache Airflow, AWS Lambda |
| --- | --- |
| 데이터 레이크 | AWS S3 |
| 데이터 웨어하우스 | AWS Redshift |
| 데이터 시각화 | Apache Superset, Tableau |
| 데이터 처리 | Python - requests, beautifulsoup4, pandas, AWS Athena |
| CI / CD | Git, GitHub Actions |
| 협업 툴 | AWS, GitHub, Slack, Notion, Gather |

---

# 프로젝트 세부 사항

## 사용 API
- [기상청 API 정보](https://apihub.kma.go.kr/)
  - 단기 예보 
  - 중기 예보
  - 기상 특보
  - 태풍 정보
  - 지진 정보
- [한강 홍수 통제소 OpenAPI ](https://www.hrfco.go.kr/web/openapiPage/reference.do)
  - 수위 관측소 제원 조회
- [국가 수자원 관리 종합 시스템](http://www.wamis.go.kr:8080/wamisweb/wl/w7.do)
- [재난안전데이터공유 플랫폼](https://www.safetydata.go.kr)
  - 긴급 재난 문자 
- [공공데이터 포털](https://www.data.go.kr)
  - [한국수자원공사_SPI 가뭄지수정보](https://www.data.go.kr/tcs/dss/selectApiDataDetailView.do?publicDataPk=15056637)
  - [산림청 국립산림과학원_산불위험예보정보](https://www.data.go.kr/tcs/dss/selectApiDataDetailView.do?publicDataPk=15084817)
- [산림청](https://www.forest.go.kr/kfsweb/kfi/kfs/frfr/selectFrfrStatsNow.do?mn=NKFS_06_09_01)
- [기상청_황사](https://www.weather.go.kr/w/dust/dust-obs-days.do)



## 프로젝트 작업 및 타임라인

| 주차별 |           작업 이름             |                                     마감일              |
|:------:|:--------------------------------------------------:|:---------------------------------:|
| 0주차  | 주제 선정 및 API 탐색                                   | 2024년 7월 1일 → 2024년 7월 12일 |
| 1주차  | raw_data 수집 & AWS 환경 구성                           | 2024년 7월 15일 → 2024년 7월 19일 |
| 2주차  | 대시보드 제작 & EC2 Airflow 이관                      | 2024년 7월 22일 → 2024년 7월 26일 |
| 3주차  | CI/CD 자동화 & 개인추가작업                             | 2024년 7월 29일 → 2024년 8월 2일 |
| 4주차  | 최종 작업 마무리 & 보고서 및 발표 준비                             | 2024년 8월 5일 → 2024년 8월 9일 |
| 5주차  | 자율 프로젝트 마무리                             | 2024년 8월 12일 → 2024년 8월 16일 |

---

# 팀원 및 역할


| 팀원 | 역할 |
| --- | --- |
| 곽도영 | 과거/ 현재 기상 특보 정보, AWS 환경 생성 |
| 남원우 | 연간 자연 재해, 산불 지수, 24년 지진 정보 |
| 손봉호 | 단기 예보, 긴급 재난 문자 알림, Airflow 모니터링 시도 |
| 전찬수 | 중기 예보, API 별 날씨 비교 , CI/CD git action , airflow log → sns 알림 |
| 최용구 | 자연 재해 - 홍수 예보, 하천, 강수량 / AWS Infra 구축 보조 / Airflow 서버 분산 시도 |


---

# 프로젝트 결과
## Superset
<details>
  
<summary> Tab1. 날씨 </summary>

![proj-dashboard-2024-08-06T05-13-12 404Z](https://github.com/user-attachments/assets/3a475e13-8eb5-405a-ab13-dc7fc6bf7e58)

</details>

<details>
  
<summary> Tab2. 자연재해 </summary>

![proj-dashboard-2024-08-06T08-22-10 762Z](https://github.com/user-attachments/assets/f013a6da-6c48-4721-b656-f79d6564e649)

</details>

## Tableau
<details>
</details>

---

# 개선점

# nice_cloud_day

## 프로젝트 개요
### 프로젝트 주제
- **대한민국 날씨 및 자연재해에 대한 데이터를 제공하는 파이프라인 구현 및 대시보드 작성**
### 목표
- 날씨 및 재해 정보 관련 데이터의 통계, 현황, 추이등의 대시보드 구현
- 실시간성을 띄는 데이터를 활용하여 SLACK 알림 구현
- Airflow Dag의 log 정보 SLACK을 통해 제공
### 사용 API
- 기상청 API 정보 : [기상청hub](https://apihub.kma.go.kr/)
  - 단기 예보 
  - 중기 예보
  - 기상 특보
  - 태풍 정보
  - 지진 정보
- 한강 홍수 통제소 OpenAPI : [한강호수](https://www.hrfco.go.kr/web/openapiPage/reference.do)
  - 수위 관측소 제원 조회
  -
  - 
- 국가 수자원 관리 종합 시스템 :[수위 관측소](http://www.wamis.go.kr:8080/wamisweb/wl/w7.do)

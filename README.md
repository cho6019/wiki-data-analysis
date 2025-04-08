# wiki-data-analysis

## 작업 과정
- 1. GCP GS에 위키피디아 검색 데이터 배치
  2. GCP 인스턴스 생성 후 SPARK 설치 및 MASTER, WORKER 노드 생성
  3. 인스턴스에 GS의 데이터 불러오기 및 처리 스크립트 작성
  4. 로컬 AIRFLOW를 이용한 SSH 명령 자동화 작업 DAGS 작성
  5. 로컬 SPARK 및 ZEPPLIN으로 얻어낸 데이터에 대한 분석 실행

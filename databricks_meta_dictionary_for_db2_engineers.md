# Databricks 데이터 엔지니어 메타 사전 (DB2 엔지니어 대상)

이 문서는 DB2 환경에서 근무하던 주니어 엔지니어들이 Databricks 레이크하우스(Lakehouse) 환경으로 원활하게 기술 전이를 할 수 있도록 돕기 위해 작성되었습니다. 기존 RDBMS(DB2)의 개념과 매칭하여 핵심 용어를 정리했습니다.

---

## 1. 아키텍처 및 데이터 모델링 (Architecture & Modeling)

| 용어 | 정의 | DB2 비교 및 이해 |
| :--- | :--- | :--- |
| **메달리온 아키텍처 (Medallion Architecture)** | 데이터 품질에 따라 Bronze, Silver, Gold 3단계로 관리하는 설계 패턴. | Staging(Bronze) -> ODS/EDW(Silver) -> Data Mart(Gold) 흐름과 유사. |
| **데이터 레이크하우스 (Data Lakehouse)** | 데이터 레이크의 유연성과 DW의 트랜잭션/성능을 결합한 아키텍처. | DB2의 정형 데이터 처리 능력에 비정형 데이터 저장 능력이 합쳐진 형태. |
| **관리형 vs 외부 테이블 (Managed vs External)** | 데이터 파일의 생명주기 관리 주체에 따른 분류. | Managed는 일반 테이블, External은 페더레이션 닉네임과 유사. |
| **파티셔닝 (Partitioning)** | 특정 컬럼 기준으로 데이터를 물리적 디렉토리로 분할 저장. | DB2의 Table Partitioning(Range)과 목적이 동일함. |
| **Z-오더링 (Z-Ordering)** | 다차원 클러스터링을 통해 연관 데이터를 인접하게 배치하는 기술. | DB2의 MDC(Multidimensional Clustering) 테이블과 가장 유사. |
| **리퀴드 클러스터링 (Liquid Clustering)** | 정적인 파티셔닝을 대체하는 동적 데이터 배치 기술. | 데이터 크기에 따라 자동으로 셀 크기를 조절하는 '지능형 MDC'. |
| **스키마 진화 (Schema Evolution)** | 데이터 추가 시 테이블 스키마를 자동으로 변경(Add Column)하는 기능. | DB2의 엄격한 Schema-on-Write 방식을 유연하게 자동화한 형태. |
| **구체화된 뷰 (Materialized Views)** | 쿼리 결과를 저장해두고 점진적으로 업데이트하는 뷰. | DB2의 MQT(Materialized Query Table)와 완벽히 일치하는 개념. |
| **테이블 클론 (Table Clones)** | Shallow(메타데이터만) 또는 Deep(물리 파일까지) 복제 기능. | Shallow는 스토리지 스냅샷, Deep은 CTAS 작업과 유사. |
| **OPTIMIZE & VACUUM** | 작은 파일 병합 및 불필요한 과거 버전 파일 삭제. | DB2의 REORG 작업 및 아카이브 로그 정리와 동일한 관리 작업. |
| **임시 뷰 (Temp View)** | 세션이나 클러스터 수명 동안만 유지되는 논리적 뷰. | DB2의 DGTT(Declared Global Temporary Table)와 유사. |
| **레이크하우스 페더레이션 (Federation)** | 외부 DB 데이터를 복사 없이 직접 쿼리하는 기능. | DB2의 핵심 기능인 Federation(Wrapper/Nickname)과 동일. |
| **SCD (Slowly Changing Dimension)** | 시간에 따라 변하는 차원 데이터의 이력을 관리하는 모델링. | DB2의 Temporal Table 및 이력 관리 테이블 설계 방식과 동일. |
| **자동 증가 컬럼 (Identity Columns)** | 레코드 삽입 시 순차적 고유 키 생성. | DB2의 `GENERATED ALWAYS AS IDENTITY`와 완벽히 일치. |

## 2. 데이터 저장소 및 포맷 (Storage & Formats)

| 용어 | 정의 | DB2 비교 및 이해 |
| :--- | :--- | :--- |
| **파켓 (Parquet File)** | 분석용 쿼리에 최적화된 컬럼 지향(Columnar) 저장 포맷. | Row 단위 저장인 DB2와 달리 열 단위로 압축 저장하여 집계 성능 극대화. |
| **델타 레이크 (Delta Lake)** | Parquet 위에 트랜잭션 로그를 얹어 ACID를 보장하는 스토리지 계층. | DB2의 Transaction Log(Active/Archive) 기능을 파일 레벨에서 구현. |
| **클라우드 오브젝트 스토리지** | S3, ADLS 등 무한 확장이 가능한 비공유형 저장소. | 용량을 할당해야 하는 Table Space와 달리 무제한 확장 가능한 가상 디스크. |

## 3. 컴퓨팅 및 처리 엔진 (Compute & Processing Engine)

| 용어 | 정의 | DB2 비교 및 이해 |
| :--- | :--- | :--- |
| **아파치 스파크 (Apache Spark)** | 메모리 기반의 대규모 분산 처리 엔진. | DB2의 쿼리 실행 엔진이 여러 대의 서버로 분산된 형태. |
| **클러스터 (Cluster)** | Driver(마스터)와 Worker(일꾼)로 구성된 연산 자원 집합. | DB2 DPF 환경의 Coordinator Node와 Data Node 구조와 유사. |
| **카탈리스트 옵티마이저** | 쿼리를 분석하여 최적의 실행 계획을 생성하는 엔진. | DB2의 SQL Optimizer와 완벽히 동일한 역할 수행. |
| **포톤 가속 (Photon Engine)** | C++ 기반의 벡터화된 차세대 쿼리 실행 엔진. | DB2 BLU Acceleration(컬럼 기반 벡터 처리)의 클라우드 버전. |
| **적응형 쿼리 실행 (AQE)** | 실행 도중 통계를 파악하여 실행 계획을 동적으로 재조정. | 실행 중 스스로 Access Path를 튜닝하는 '지능형 옵티마이저'. |
| **범용 vs 작업 클러스터** | 대화형 테스트용 vs 배치 전용 리소스 운영 모델. | 상시 구동 서버와 작업 시간에만 켜지는 서버의 차이. |
| **오토스케일링** | 부하에 따라 워커 노드 개수를 자동으로 증감하는 기능. | Peak 용량에 맞춰 서버를 사두지 않고 필요할 때만 확장하는 탄력성. |
| **SQL 웨어하우스 (SQL Warehouses)** | BI 조회 및 ANSI SQL 쿼리에 최적화된 컴퓨팅 리소스. | 분석 쿼리와 배치를 분리하기 위한 DB2의 WLM(Workload Manager) 역할. |
| **스파크 UI & 프로파일러** | 실행 계획 및 성능 병목 지점을 시각화하는 도구. | DB2의 Visual Explain, db2expln, db2pd를 합친 대시보드. |

## 4. 데이터 엔지니어링 및 파이프라인 (Pipelines & Ingestion)

| 용어 | 정의 | DB2 비교 및 이해 |
| :--- | :--- | :--- |
| **DLT (Delta Live Tables)** | 선언적 방식으로 구축하는 자동화된 데이터 파이프라인. | MQT + ETL 스케줄링 + 데이터 품질 체크가 통합된 고급 프레임워크. |
| **오토 로더 (Auto Loader)** | 새로운 파일 도착을 감지하여 증분 데이터만 로드하는 기능. | 파일 입고를 감지하여 자동으로 `LOAD`를 수행하는 지능형 로더. |
| **COPY INTO** | 파일 기반 데이터를 테이블로 고속 적재하는 SQL 명령어. | DB2의 고속 적재 도구인 `LOAD` 유틸리티와 목적/사용법 유사. |
| **CDC (APPLY CHANGES INTO)** | 소스 DB의 변경 내역을 델타 테이블에 병합하는 기술. | DB2의 Q Replication이나 InfoSphere CDC 솔루션과 동일. |
| **품질 제약조건 (Expectations)** | 파이프라인 내에서 데이터 품질을 검증하는 규칙. | DB2의 `CHECK CONSTRAINT`와 유사하나, 불량 데이터 격리 가능. |
| **구조적 스트리밍** | 실시간 데이터를 배치와 동일한 방식으로 처리하는 엔진. | MQ 등 스트림 데이터를 복잡한 로직 없이 SQL로 처리하는 방식. |
| **DAG (Directed Acyclic Graph)** | 태스크 간의 선후행 의존성을 나타낸 흐름도. | Control-M이나 TWS 같은 배치 스케줄러의 Job Tree와 동일. |
| **체크포인트 (Checkpointing)** | 장애 시 재시작 지점을 찾기 위해 상태를 기록하는 기능. | DB2의 LSN이나 배치 프로그램의 재시작(Restart) 로직 자동화. |
| **파일 도착 트리거** | 시간 기반이 아닌 파일 입고 시점에 작업을 기동하는 방식. | 쉘 스크립트의 Polling 없이 이벤트 기반으로 배치를 시작함. |
| **워터마크 (Watermarking)** | 실시간 처리 시 지연 도착 데이터를 허용하는 임계값. | 비즈니스 마감(Closing) 로직을 시스템 레벨에서 처리하는 개념. |
| **DABs (Asset Bundles)** | 코드와 설정을 묶어 버전 관리 및 배포하는 CI/CD 도구. | SQL/SP/스크립트를 묶어 운영 서버로 이관하는 배포 자동화 체계. |

## 5. 데이터 거버넌스 및 카탈로그 (Unity Catalog)

| 용어 | 정의 | DB2 비교 및 이해 |
| :--- | :--- | :--- |
| **유니티 카탈로그 (Unity Catalog)** | 전사적 데이터 권한 및 메타데이터 중앙 제어 솔루션. | DB2의 시스템 카탈로그와 권한 관리 체계의 클라우드 확장판. |
| **3계층 네임스페이스** | `Catalog.Schema.Table` 구조의 표준 명명 규칙. | DB2의 `Database.Schema.Table` 계층 구조와 완벽히 매칭. |
| **인포메이션 스키마** | 메타데이터 조회를 위한 표준 SQL 뷰 세트. | `SYSCAT.TABLES`, `SYSCAT.COLUMNS` 등 시스템 뷰와 동일 역할. |
| **데이터 리니지 (Lineage)** | 데이터의 생성 원천과 흐름 경로를 자동 시각화하는 기능. | 외부 메타데이터 관리 솔루션(InfoSphere 등)이 내장된 형태. |
| **외부 위치 (External Locations)** | 클라우드 저장소 경로에 대한 보안 자격 증명 관리. | 페더레이션 구성을 위한 Server, Wrapper, User Mapping 관리와 유사. |

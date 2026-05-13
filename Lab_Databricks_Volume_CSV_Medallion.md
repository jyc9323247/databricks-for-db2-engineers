# Lab: Databricks Volume CSV → Bronze → Silver → Gold 데이터 수집 파이프라인

> **버전:** 1.0 | **최종 수정:** 2026-05-13  
> **대상:** 일반 사용자 계정 (Workspace Admin 아님)  
> **클라우드:** AWS / Azure / GCP Databricks (UI 위주, 공통 절차)  
> **예상 소요 시간:** 약 90분

---

## 목차

1. [실습 개요](#1-실습-개요)
2. [사전 준비 사항](#2-사전-준비-사항)
3. [실습 데이터 준비](#3-실습-데이터-준비)
4. [Step 1 — 워크스페이스 환경 설정 (Admin 작업)](#step-1--워크스페이스-환경-설정-admin-작업)
5. [Step 2 — 실습 사용자 권한 확인](#step-2--실습-사용자-권한-확인)
6. [Step 3 — Catalog / Schema / Volume 생성](#step-3--catalog--schema--volume-생성)
7. [Step 4 — CSV 파일 Volume에 업로드](#step-4--csv-파일-volume에-업로드)
8. [Step 5 — Bronze 레이어: 원본 데이터 적재](#step-5--bronze-레이어-원본-데이터-적재)
9. [Step 6 — Silver 레이어: 정제 및 타입 변환](#step-6--silver-레이어-정제-및-타입-변환)
10. [Step 7 — Gold 레이어: 비즈니스 집계 테이블](#step-7--gold-레이어-비즈니스-집계-테이블)
11. [Step 8 — 데이터 검증 및 시각화](#step-8--데이터-검증-및-시각화)
12. [실습 정리 (Clean-up)](#실습-정리-clean-up)
13. [트러블슈팅](#트러블슈팅)
14. [참고 자료](#참고-자료)

---

## 1. 실습 개요

### 학습 목표

- Unity Catalog Volume에 CSV 파일을 업로드하고 읽는 방법을 익힌다.
- Medallion Architecture (Bronze → Silver → Gold)의 개념을 이해하고 직접 구현한다.
- `spark.read.csv()`를 사용한 가장 기본적인 데이터 수집 방식을 실습한다.
- 일반 사용자 계정으로 필요한 권한 구조를 이해한다.

### 아키텍처

```
┌─────────────┐     ┌──────────────────────────────────────────────────┐
│  CSV 파일   │     │          Unity Catalog (Managed Tables)          │
│ (로컬 PC)   │     │                                                  │
│             │     │  ┌──────────┐  ┌──────────┐  ┌──────────────┐   │
│ books.csv   │────▶│  │  Volume  │─▶│  Bronze  │─▶│    Silver    │   │
│ orders.csv  │     │  │ (landing)│  │ (raw)    │  │  (cleansed)  │   │
│ customers.csv│    │  └──────────┘  └──────────┘  └──────┬───────┘   │
└─────────────┘     │                                      │           │
                    │                              ┌───────▼───────┐   │
                    │                              │     Gold      │   │
                    │                              │ (aggregated)  │   │
                    │                              └───────────────┘   │
                    └──────────────────────────────────────────────────┘
```

### 이 Lab에서 사용하지 않는 것

- Auto Loader (`cloudFiles`)
- Delta Live Tables (DLT) / Lakeflow Declarative Pipelines
- Structured Streaming

> 이 Lab은 **가장 간단한 배치 읽기 (`spark.read`)** 만 사용합니다.

---

## 2. 사전 준비 사항

| 항목 | 요구 사항 |
|------|-----------|
| Databricks 워크스페이스 | Unity Catalog가 활성화된 워크스페이스 |
| 계정 유형 | 일반 사용자 계정 (Workspace Admin 계정은 Admin 초기 설정 시에만 필요) |
| 컴퓨트 | All-Purpose Cluster 또는 SQL Warehouse (실습 중 생성 가능) |
| 로컬 PC | CSV 파일 3개를 다운로드/저장할 수 있는 환경 |
| 브라우저 | Chrome, Edge 등 최신 브라우저 |

---

## 3. 실습 데이터 준비

온라인 서점 시나리오로 3개의 CSV 파일을 사용합니다. 아래 내용을 복사하여 각각의 파일로 저장하세요.

### 3-1. `books.csv` — 도서 마스터 (20건)

```csv
book_id,title,author,genre,price,published_date,isbn
B001,클린 코드,로버트 C. 마틴,기술,33000,2013-12-01,978-8966260959
B002,해리 포터와 마법사의 돌,J.K. 롤링,판타지,12800,2019-11-25,978-8983920980
B003,사피엔스,유발 하라리,인문,22000,2015-11-23,978-8934972464
B004,코스모스,칼 세이건,과학,16800,2006-12-20,978-8983711892
B005,데미안,헤르만 헤세,문학,8800,2009-01-20,978-8937460449
B006,이것이 자바다,신용권,기술,36000,2022-01-05,978-1165219754
B007,어린 왕자,앙투안 드 생텍쥐페리,문학,9800,2007-05-01,978-8932917245
B008,총 균 쇠,재레드 다이아몬드,인문,25000,2013-01-25,978-8970127248
B009,파이썬 머신러닝 완벽 가이드,권철민,기술,38000,2022-04-01,978-1165215756
B010,1984,조지 오웰,문학,10800,2003-06-20,978-8937460166
B011,데이터 중심 애플리케이션 설계,마틴 클레프만,기술,35000,2018-04-12,978-8960883666
B012,미움받을 용기,기시미 이치로,자기계발,14900,2014-11-17,978-8996991342
B013,나미야 잡화점의 기적,히가시노 게이고,문학,13800,2012-12-07,978-8925560311
B014,팩트풀니스,한스 로슬링,인문,19800,2019-03-04,978-8934983941
B015,리팩터링,마틴 파울러,기술,35000,2020-04-01,978-8966262113
B016,오브젝트,조영호,기술,38000,2019-06-17,978-8966262106
B017,아몬드,손원평,문학,13000,2017-03-31,978-8954651134
B018,돈의 심리학,모건 하우절,경영,18000,2021-01-08,978-1160540266
B019,Spark 완벽 가이드,빌 체임버스,기술,40000,2021-09-01,978-1162244723
B020,역행자,자청,자기계발,17500,2022-06-01,978-8901260716
```

### 3-2. `orders.csv` — 주문 내역 (30건)

```csv
order_id,customer_id,book_id,quantity,order_date,order_status
ORD-001,C001,B001,1,2025-01-05,completed
ORD-002,C002,B003,2,2025-01-07,completed
ORD-003,C003,B005,1,2025-01-10,completed
ORD-004,C001,B009,1,2025-01-12,completed
ORD-005,C004,B002,3,2025-01-15,cancelled
ORD-006,C005,B012,1,2025-01-18,completed
ORD-007,C002,B007,1,2025-01-20,completed
ORD-008,C006,B019,1,2025-01-22,completed
ORD-009,C003,B011,1,2025-01-25,completed
ORD-010,C007,B004,2,2025-01-28,completed
ORD-011,C008,B018,1,2025-02-01,completed
ORD-012,C004,B006,1,2025-02-03,pending
ORD-013,C009,B015,1,2025-02-05,completed
ORD-014,C001,B016,1,2025-02-08,completed
ORD-015,C010,B010,2,2025-02-10,completed
ORD-016,C005,B013,1,2025-02-12,completed
ORD-017,C002,B001,1,2025-02-15,completed
ORD-018,C006,B020,1,2025-02-18,completed
ORD-019,C007,B014,1,2025-02-20,completed
ORD-020,C003,B008,1,2025-02-22,completed
ORD-021,C008,B017,2,2025-02-25,completed
ORD-022,C009,B003,1,2025-03-01,cancelled
ORD-023,C010,B009,1,2025-03-03,completed
ORD-024,C001,B012,1,2025-03-05,completed
ORD-025,C004,B005,1,2025-03-08,pending
ORD-026,C005,B019,1,2025-03-10,completed
ORD-027,C006,B001,2,2025-03-12,completed
ORD-028,C002,B011,1,2025-03-15,completed
ORD-029,C007,B006,1,2025-03-18,completed
ORD-030,C008,B003,1,2025-03-20,completed
```

### 3-3. `customers.csv` — 고객 정보 (10건)

```csv
customer_id,name,email,city,membership_grade,registered_date
C001,김민수,minsu.kim@example.com,서울,Gold,2023-03-15
C002,이지은,jieun.lee@example.com,부산,Silver,2023-06-20
C003,박준혁,junhyuk.park@example.com,대전,Bronze,2024-01-10
C004,최서연,seoyeon.choi@example.com,인천,Gold,2023-05-22
C005,정하늘,haneul.jung@example.com,서울,Silver,2024-03-01
C006,한도윤,doyun.han@example.com,대구,Bronze,2024-06-15
C007,강유진,yujin.kang@example.com,광주,Silver,2023-09-10
C008,윤서준,seojun.yun@example.com,서울,Gold,2023-11-05
C009,임수빈,subin.lim@example.com,부산,Bronze,2024-08-20
C010,오예린,yerin.oh@example.com,제주,Silver,2024-02-14
```

> **참고:** 위 데이터는 실습용 가상 데이터입니다.  
> 실제 프로젝트에서는 Kaggle의 "Online Bookstore Dataset" 등 공개 데이터셋을 활용할 수 있습니다.  
> - https://www.kaggle.com/datasets/sbonelondhlazi/bookstore-dataset  
> - https://www.kaggle.com/datasets/benjnb/online-bookstore-dataset

---

## Step 1 — 워크스페이스 환경 설정 (Admin 작업)

> ⚠️ **이 Step은 Workspace Admin 또는 Metastore Admin이 수행합니다.**  
> 이미 설정이 완료된 환경이라면 [Step 2](#step-2--실습-사용자-권한-확인)로 건너뛰세요.

### 1-1. 실습 사용자 계정 생성 (또는 확인)

1. 워크스페이스 좌측 사이드바 → **Settings** → **Identity and access** → **Users**
2. 실습 사용자가 이미 등록되어 있는지 확인
3. 없으면 **Add user** → 이메일 주소 입력 → **Add**

### 1-2. 컴퓨트(Cluster) 접근 권한

실습 사용자가 클러스터를 사용할 수 있어야 합니다.

**옵션 A — 기존 All-Purpose Cluster 공유:**

1. **Compute** → 대상 클러스터 선택 → **Permissions**
2. 실습 사용자 추가 → **Can Attach To** 권한 부여

**옵션 B — 실습 사용자가 직접 클러스터 생성하도록 허용:**

1. **Settings** → **Compute** → Cluster creation policy 확인
2. **Unrestricted** 또는 적절한 Policy가 설정되어 있어야 함

> ℹ️ **Free Edition** 사용자는 기본적으로 workspace catalog과 default schema에 대한 권한이 자동 부여됩니다.

### 1-3. 실습용 Catalog에 사용자 권한 부여

실습에서 사용할 Catalog에 대해 다음 권한을 부여합니다.  
**Notebook 또는 SQL Editor에서 Admin 계정으로 실행:**

```sql
-- ============================================================
-- Admin 실행: 실습 사용자에게 Catalog/Schema 레벨 권한 부여
-- ============================================================

-- 방법 1: 워크스페이스 기본 Catalog 사용 시 (권장 — 심플)
-- Free Edition / Trial에서는 workspace catalog + default schema에
-- USE CATALOG, USE SCHEMA, CREATE TABLE, CREATE VOLUME 이 이미 부여됨.
-- → 이 경우 별도 GRANT 불필요.

-- 방법 2: 별도 Catalog를 만들어서 실습하는 경우
CREATE CATALOG IF NOT EXISTS lab_bookstore;

GRANT USE CATALOG ON CATALOG lab_bookstore TO `실습사용자@example.com`;
GRANT CREATE SCHEMA ON CATALOG lab_bookstore TO `실습사용자@example.com`;
```

> **💡 NOTE:** `USE CATALOG`은 Catalog 내 객체에 접근하기 위한 전제 조건입니다.  
> `CREATE SCHEMA`을 주면 실습 사용자가 직접 Schema를 생성할 수 있습니다.

---

## Step 2 — 실습 사용자 권한 확인

> 여기서부터는 **실습 사용자 (일반 계정)** 로 로그인하여 진행합니다.

Notebook을 새로 생성하고, 첫 번째 셀에서 현재 권한을 확인합니다.

```sql
-- 사용 가능한 Catalog 목록 확인
SHOW CATALOGS;
```

```sql
-- 실습 Catalog 접근 확인
USE CATALOG lab_bookstore;    -- 또는 워크스페이스 기본 Catalog명
SHOW SCHEMAS;
```

> 만약 `PERMISSION_DENIED` 에러가 발생하면 Admin에게 Step 1의 권한 부여를 요청하세요.

---

## Step 3 — Catalog / Schema / Volume 생성

실습 사용자가 직접 Schema와 Volume을 생성합니다.

```sql
-- ============================================================
-- 실습 사용자 실행
-- ============================================================

-- Catalog 선택 (환경에 맞게 수정)
USE CATALOG lab_bookstore;

-- Schema 생성 (Bronze/Silver/Gold 테이블을 모두 이 Schema 안에 생성)
CREATE SCHEMA IF NOT EXISTS bookstore
COMMENT '온라인 서점 데이터 파이프라인 실습';

USE SCHEMA bookstore;

-- Volume 생성 (CSV 파일 랜딩 영역)
CREATE VOLUME IF NOT EXISTS landing
COMMENT 'CSV 원본 파일 업로드용 Volume';
```

### 생성 확인

```sql
-- Schema 확인
DESCRIBE SCHEMA EXTENDED bookstore;

-- Volume 확인
SHOW VOLUMES;
```

> **핵심 개념:**  
> - **Volume** = 파일(CSV, JSON, 이미지 등 비정형 데이터)을 저장하는 공간  
> - **Table** = 정형 데이터를 저장하는 공간 (Delta Lake)  
> - Volume의 파일 경로: `/Volumes/<catalog>/<schema>/<volume>/파일명`

---

## Step 4 — CSV 파일 Volume에 업로드

### 방법 A — UI를 통한 업로드 (권장)

1. 좌측 사이드바 → **New** → **Add or upload data**
2. **Upload files to a volume** 클릭
3. **Destination volume** 에서 경로 선택:
   - Catalog: `lab_bookstore`
   - Schema: `bookstore`
   - Volume: `landing`
4. 준비한 3개 CSV 파일을 드래그 앤 드롭:
   - `books.csv`
   - `orders.csv`
   - `customers.csv`
5. 업로드 완료 확인

### 방법 B — Notebook에서 직접 생성

인터넷 접근이 가능한 환경이라면 Notebook 셀에서 직접 파일을 생성할 수도 있습니다.

```python
# books.csv 직접 생성 예시
books_data = """book_id,title,author,genre,price,published_date,isbn
B001,클린 코드,로버트 C. 마틴,기술,33000,2013-12-01,978-8966260959
B002,해리 포터와 마법사의 돌,J.K. 롤링,판타지,12800,2019-11-25,978-8983920980
B003,사피엔스,유발 하라리,인문,22000,2015-11-23,978-8934972464
B004,코스모스,칼 세이건,과학,16800,2006-12-20,978-8983711892
B005,데미안,헤르만 헤세,문학,8800,2009-01-20,978-8937460449
B006,이것이 자바다,신용권,기술,36000,2022-01-05,978-1165219754
B007,어린 왕자,앙투안 드 생텍쥐페리,문학,9800,2007-05-01,978-8932917245
B008,총 균 쇠,재레드 다이아몬드,인문,25000,2013-01-25,978-8970127248
B009,파이썬 머신러닝 완벽 가이드,권철민,기술,38000,2022-04-01,978-1165215756
B010,1984,조지 오웰,문학,10800,2003-06-20,978-8937460166
B011,데이터 중심 애플리케이션 설계,마틴 클레프만,기술,35000,2018-04-12,978-8960883666
B012,미움받을 용기,기시미 이치로,자기계발,14900,2014-11-17,978-8996991342
B013,나미야 잡화점의 기적,히가시노 게이고,문학,13800,2012-12-07,978-8925560311
B014,팩트풀니스,한스 로슬링,인문,19800,2019-03-04,978-8934983941
B015,리팩터링,마틴 파울러,기술,35000,2020-04-01,978-8966262113
B016,오브젝트,조영호,기술,38000,2019-06-17,978-8966262106
B017,아몬드,손원평,문학,13000,2017-03-31,978-8954651134
B018,돈의 심리학,모건 하우절,경영,18000,2021-01-08,978-1160540266
B019,Spark 완벽 가이드,빌 체임버스,기술,40000,2021-09-01,978-1162244723
B020,역행자,자청,자기계발,17500,2022-06-01,978-8901260716"""

volume_path = "/Volumes/lab_bookstore/bookstore/landing"

with open(f"{volume_path}/books.csv", "w", encoding="utf-8") as f:
    f.write(books_data)

print("books.csv 업로드 완료")
```

> `orders.csv`, `customers.csv`도 동일한 방식으로 생성합니다. (지면상 생략)

### 업로드 확인

```python
# Volume 내 파일 목록 확인
files = dbutils.fs.ls("/Volumes/lab_bookstore/bookstore/landing")
for f in files:
    print(f"{f.name:30s} {f.size:>10,} bytes")
```

**기대 결과:**

```
books.csv                            약 1.2 KB
orders.csv                           약 1.1 KB
customers.csv                        약 0.5 KB
```

---

## Step 5 — Bronze 레이어: 원본 데이터 적재

> **Bronze 원칙:** 원본 데이터를 **있는 그대로** Delta Table에 저장합니다.  
> 스키마 변환이나 데이터 정제를 하지 않습니다. 모든 컬럼을 STRING으로 읽습니다.

### 5-1. Bronze — books

```python
# ============================================================
# Bronze: books — 원본 CSV 그대로 적재
# ============================================================
from pyspark.sql.functions import current_timestamp, lit, input_file_name

bronze_books = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")  # 모든 컬럼을 STRING으로
    .option("encoding", "UTF-8")
    .load("/Volumes/lab_bookstore/bookstore/landing/books.csv")
)

# 메타데이터 컬럼 추가 (Bronze 관례)
bronze_books = (
    bronze_books
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", lit("books.csv"))
)

# Delta Table로 저장
(
    bronze_books.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("lab_bookstore.bookstore.bronze_books")
)

print(f"bronze_books 적재 완료: {bronze_books.count()} rows")
bronze_books.display()
```

### 5-2. Bronze — orders

```python
# ============================================================
# Bronze: orders
# ============================================================
bronze_orders = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .option("encoding", "UTF-8")
    .load("/Volumes/lab_bookstore/bookstore/landing/orders.csv")
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", lit("orders.csv"))
)

(
    bronze_orders.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("lab_bookstore.bookstore.bronze_orders")
)

print(f"bronze_orders 적재 완료: {bronze_orders.count()} rows")
bronze_orders.display()
```

### 5-3. Bronze — customers

```python
# ============================================================
# Bronze: customers
# ============================================================
bronze_customers = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .option("encoding", "UTF-8")
    .load("/Volumes/lab_bookstore/bookstore/landing/customers.csv")
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", lit("customers.csv"))
)

(
    bronze_customers.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("lab_bookstore.bookstore.bronze_customers")
)

print(f"bronze_customers 적재 완료: {bronze_customers.count()} rows")
bronze_customers.display()
```

### 5-4. Bronze 테이블 검증

```sql
-- Bronze 테이블 목록 확인
SHOW TABLES IN lab_bookstore.bookstore LIKE 'bronze_*';
```

```sql
-- 스키마 확인 — 모든 원본 컬럼이 STRING인지 확인
DESCRIBE TABLE lab_bookstore.bookstore.bronze_books;
```

---

## Step 6 — Silver 레이어: 정제 및 타입 변환

> **Silver 원칙:**  
> 1. 데이터 타입을 올바르게 변환 (STRING → INT, DATE, DOUBLE 등)  
> 2. null / 빈 문자열 처리  
> 3. 중복 제거  
> 4. 불필요한 공백 제거 (trim)

### 6-1. Silver — books

```python
# ============================================================
# Silver: books — 타입 변환 및 정제
# ============================================================
from pyspark.sql.functions import col, trim, to_date

silver_books = (
    spark.table("lab_bookstore.bookstore.bronze_books")
    .select(
        trim(col("book_id")).alias("book_id"),
        trim(col("title")).alias("title"),
        trim(col("author")).alias("author"),
        trim(col("genre")).alias("genre"),
        col("price").cast("int").alias("price"),
        to_date(col("published_date"), "yyyy-MM-dd").alias("published_date"),
        trim(col("isbn")).alias("isbn"),
        col("_ingested_at")
    )
    .dropDuplicates(["book_id"])         # 중복 제거
    .filter(col("book_id").isNotNull())  # null 제거
)

(
    silver_books.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("lab_bookstore.bookstore.silver_books")
)

print(f"silver_books 적재 완료: {silver_books.count()} rows")
silver_books.printSchema()
silver_books.display()
```

### 6-2. Silver — orders

```python
# ============================================================
# Silver: orders — 타입 변환 및 정제
# ============================================================
silver_orders = (
    spark.table("lab_bookstore.bookstore.bronze_orders")
    .select(
        trim(col("order_id")).alias("order_id"),
        trim(col("customer_id")).alias("customer_id"),
        trim(col("book_id")).alias("book_id"),
        col("quantity").cast("int").alias("quantity"),
        to_date(col("order_date"), "yyyy-MM-dd").alias("order_date"),
        trim(col("order_status")).alias("order_status"),
        col("_ingested_at")
    )
    .dropDuplicates(["order_id"])
    .filter(col("order_id").isNotNull())
)

(
    silver_orders.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("lab_bookstore.bookstore.silver_orders")
)

print(f"silver_orders 적재 완료: {silver_orders.count()} rows")
silver_orders.printSchema()
silver_orders.display()
```

### 6-3. Silver — customers

```python
# ============================================================
# Silver: customers — 타입 변환 및 정제
# ============================================================
silver_customers = (
    spark.table("lab_bookstore.bookstore.bronze_customers")
    .select(
        trim(col("customer_id")).alias("customer_id"),
        trim(col("name")).alias("name"),
        trim(col("email")).alias("email"),
        trim(col("city")).alias("city"),
        trim(col("membership_grade")).alias("membership_grade"),
        to_date(col("registered_date"), "yyyy-MM-dd").alias("registered_date"),
        col("_ingested_at")
    )
    .dropDuplicates(["customer_id"])
    .filter(col("customer_id").isNotNull())
)

(
    silver_customers.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("lab_bookstore.bookstore.silver_customers")
)

print(f"silver_customers 적재 완료: {silver_customers.count()} rows")
silver_customers.printSchema()
silver_customers.display()
```

### 6-4. Silver 테이블 검증

```sql
-- 타입이 올바르게 변환되었는지 확인
DESCRIBE TABLE lab_bookstore.bookstore.silver_books;
-- price → int, published_date → date 여야 함

DESCRIBE TABLE lab_bookstore.bookstore.silver_orders;
-- quantity → int, order_date → date 여야 함
```

---

## Step 7 — Gold 레이어: 비즈니스 집계 테이블

> **Gold 원칙:** 비즈니스 분석에 바로 사용 가능한 집계/조인 결과를 테이블로 저장합니다.

### 7-1. Gold — 장르별 매출 요약

```python
# ============================================================
# Gold: 장르별 매출 요약 (completed 주문만)
# ============================================================
from pyspark.sql.functions import sum as _sum, count, round as _round

gold_sales_by_genre = (
    spark.table("lab_bookstore.bookstore.silver_orders")
    .filter(col("order_status") == "completed")
    .join(
        spark.table("lab_bookstore.bookstore.silver_books"),
        on="book_id",
        how="inner"
    )
    .withColumn("order_amount", col("price") * col("quantity"))
    .groupBy("genre")
    .agg(
        count("order_id").alias("total_orders"),
        _sum("quantity").alias("total_quantity"),
        _sum("order_amount").alias("total_revenue"),
        _round(_sum("order_amount") / _sum("quantity"), 0).alias("avg_price_per_unit")
    )
    .orderBy(col("total_revenue").desc())
)

(
    gold_sales_by_genre.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("lab_bookstore.bookstore.gold_sales_by_genre")
)

print("gold_sales_by_genre 적재 완료")
gold_sales_by_genre.display()
```

### 7-2. Gold — 고객별 구매 요약

```python
# ============================================================
# Gold: 고객별 구매 요약
# ============================================================
gold_customer_summary = (
    spark.table("lab_bookstore.bookstore.silver_orders")
    .filter(col("order_status") == "completed")
    .join(
        spark.table("lab_bookstore.bookstore.silver_books"),
        on="book_id",
        how="inner"
    )
    .join(
        spark.table("lab_bookstore.bookstore.silver_customers"),
        on="customer_id",
        how="inner"
    )
    .withColumn("order_amount", col("price") * col("quantity"))
    .groupBy("customer_id", "name", "city", "membership_grade")
    .agg(
        count("order_id").alias("total_orders"),
        _sum("quantity").alias("total_books_purchased"),
        _sum("order_amount").alias("total_spent")
    )
    .orderBy(col("total_spent").desc())
)

(
    gold_customer_summary.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("lab_bookstore.bookstore.gold_customer_summary")
)

print("gold_customer_summary 적재 완료")
gold_customer_summary.display()
```

### 7-3. Gold — 월별 주문 트렌드

```python
# ============================================================
# Gold: 월별 주문 트렌드
# ============================================================
from pyspark.sql.functions import date_format

gold_monthly_trend = (
    spark.table("lab_bookstore.bookstore.silver_orders")
    .filter(col("order_status") == "completed")
    .join(
        spark.table("lab_bookstore.bookstore.silver_books"),
        on="book_id",
        how="inner"
    )
    .withColumn("order_amount", col("price") * col("quantity"))
    .withColumn("order_month", date_format("order_date", "yyyy-MM"))
    .groupBy("order_month")
    .agg(
        count("order_id").alias("order_count"),
        _sum("quantity").alias("total_quantity"),
        _sum("order_amount").alias("total_revenue")
    )
    .orderBy("order_month")
)

(
    gold_monthly_trend.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("lab_bookstore.bookstore.gold_monthly_trend")
)

print("gold_monthly_trend 적재 완료")
gold_monthly_trend.display()
```

---

## Step 8 — 데이터 검증 및 시각화

### 8-1. 전체 테이블 목록 확인

```sql
SHOW TABLES IN lab_bookstore.bookstore;
```

**기대 결과 (9개 테이블):**

| tableName |
|-----------|
| bronze_books |
| bronze_orders |
| bronze_customers |
| silver_books |
| silver_orders |
| silver_customers |
| gold_sales_by_genre |
| gold_customer_summary |
| gold_monthly_trend |

### 8-2. 데이터 건수 크로스 체크

```sql
SELECT 'bronze_books' as tbl, COUNT(*) as cnt FROM lab_bookstore.bookstore.bronze_books
UNION ALL
SELECT 'silver_books', COUNT(*) FROM lab_bookstore.bookstore.silver_books
UNION ALL
SELECT 'bronze_orders', COUNT(*) FROM lab_bookstore.bookstore.bronze_orders
UNION ALL
SELECT 'silver_orders', COUNT(*) FROM lab_bookstore.bookstore.silver_orders
UNION ALL
SELECT 'bronze_customers', COUNT(*) FROM lab_bookstore.bookstore.bronze_customers
UNION ALL
SELECT 'silver_customers', COUNT(*) FROM lab_bookstore.bookstore.silver_customers;
```

**기대 결과:**

| tbl | cnt |
|-----|-----|
| bronze_books | 20 |
| silver_books | 20 |
| bronze_orders | 30 |
| silver_orders | 30 |
| bronze_customers | 10 |
| silver_customers | 10 |

### 8-3. Gold 테이블 시각화 (Notebook 차트 기능 활용)

Gold 테이블 결과에서 `display()` 실행 후 Notebook 우측 상단의 **차트 아이콘 (+)** 을 클릭하면 시각화를 바로 생성할 수 있습니다.

**추천 시각화:**

| Gold 테이블 | 차트 유형 | X축 | Y축 |
|---|---|---|---|
| gold_sales_by_genre | Bar Chart | genre | total_revenue |
| gold_customer_summary | Bar Chart | name | total_spent |
| gold_monthly_trend | Line Chart | order_month | total_revenue |

---

## 실습 정리 (Clean-up)

실습 후 리소스를 정리합니다.

```sql
-- ⚠️ 아래 명령은 되돌릴 수 없습니다. 실습 완료 후에만 실행하세요.

-- 개별 테이블 삭제
DROP TABLE IF EXISTS lab_bookstore.bookstore.gold_monthly_trend;
DROP TABLE IF EXISTS lab_bookstore.bookstore.gold_customer_summary;
DROP TABLE IF EXISTS lab_bookstore.bookstore.gold_sales_by_genre;
DROP TABLE IF EXISTS lab_bookstore.bookstore.silver_customers;
DROP TABLE IF EXISTS lab_bookstore.bookstore.silver_orders;
DROP TABLE IF EXISTS lab_bookstore.bookstore.silver_books;
DROP TABLE IF EXISTS lab_bookstore.bookstore.bronze_customers;
DROP TABLE IF EXISTS lab_bookstore.bookstore.bronze_orders;
DROP TABLE IF EXISTS lab_bookstore.bookstore.bronze_books;

-- Volume 삭제 (내부 파일도 함께 삭제됨)
DROP VOLUME IF EXISTS lab_bookstore.bookstore.landing;

-- Schema 삭제
DROP SCHEMA IF EXISTS lab_bookstore.bookstore;

-- Catalog 삭제 (Admin만 가능할 수 있음 ⚠️ 확인 필요)
-- DROP CATALOG IF EXISTS lab_bookstore;
```

---

## 트러블슈팅

### Q1. `PERMISSION_DENIED` 에러가 발생합니다

**원인:** 현재 사용자에게 필요한 권한이 부여되지 않았습니다.

**해결:**
```sql
-- Admin이 실행 — 필요한 최소 권한
GRANT USE CATALOG ON CATALOG lab_bookstore TO `사용자@example.com`;
GRANT USE SCHEMA ON SCHEMA lab_bookstore.bookstore TO `사용자@example.com`;
GRANT CREATE TABLE ON SCHEMA lab_bookstore.bookstore TO `사용자@example.com`;
GRANT READ VOLUME ON VOLUME lab_bookstore.bookstore.landing TO `사용자@example.com`;
GRANT WRITE VOLUME ON VOLUME lab_bookstore.bookstore.landing TO `사용자@example.com`;
```

### Q2. CSV 파일 읽을 때 한글이 깨집니다

**해결:** `.option("encoding", "UTF-8")` 옵션이 있는지 확인하세요. 로컬에서 CSV 저장 시 UTF-8 인코딩으로 저장해야 합니다. (Excel에서 저장 시 "CSV UTF-8 (쉼표로 분리)" 선택)

### Q3. `inferSchema`를 true로 하면 안 되나요?

Bronze 레이어에서는 **false**를 권장합니다. 이유:
- 원본 데이터를 그대로 보존하는 것이 Bronze의 원칙
- inferSchema는 샘플링 기반이므로 대용량 데이터에서 잘못된 타입 추론 가능
- 타입 변환은 Silver에서 명시적으로 수행하는 것이 안전

### Q4. `saveAsTable` vs `save` 차이는?

| 항목 | `saveAsTable` | `save` |
|------|---------------|--------|
| 등록 위치 | Unity Catalog에 테이블로 등록 | 파일만 저장 (등록 안 됨) |
| 메타데이터 | Catalog Explorer에서 조회 가능 | 직접 경로를 알아야 접근 |
| 권장 | ✅ 이 Lab에서 사용 | 임시 파일 저장 시 |

### Q5. 클러스터가 없다고 나옵니다

1. 좌측 사이드바 → **Compute** → **Create compute**
2. Cluster mode: **Single Node** (실습에는 충분)
3. Databricks Runtime: 최신 LTS 버전 선택 (⚠️ 정확한 버전은 환경에 따라 다름)
4. **Create** 클릭 후 시작까지 약 3~5분 대기

---

## 참고 자료

| 주제 | 링크 |
|------|------|
| Unity Catalog Volumes 공식 문서 | https://docs.databricks.com/aws/en/volumes/volume-files |
| Volume에 파일 업로드 | https://docs.databricks.com/aws/en/ingestion/file-upload/upload-to-volume |
| Unity Catalog 권한 모델 | https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/ |
| Medallion Architecture | https://www.databricks.com/glossary/medallion-architecture |
| CSV 데이터 읽기 튜토리얼 | https://docs.databricks.com/aws/en/getting-started/import-visualize-data |

---

> **⚠️ 확인 필요 사항:**
> - Databricks Free Edition에서 `CREATE CATALOG` 명령이 허용되지 않을 수 있습니다. 이 경우 워크스페이스 기본 Catalog(`workspace` 또는 워크스페이스명과 동일한 이름)의 `default` Schema 또는 새 Schema를 사용하세요.
> - 각 클라우드(AWS/Azure/GCP)에서 Catalog Explorer의 UI 레이아웃이 약간 다를 수 있습니다.
> - `overwriteSchema` 옵션은 Databricks Runtime 버전에 따라 동작이 다를 수 있습니다. (DBR 11.x 이상 권장)
> - Volume에 대한 UI 업로드 기능은 파일당 최대 5GB까지 지원됩니다.

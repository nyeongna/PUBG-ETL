# PUBG Dataset ETL with AWS 

# 📖 Overview
This project is to practice the ETL process with Player Unknown Battle Ground(PUBG) dataset. Often times, dataset below 10,000 rows can easily be handled within a single server. However, when the size of the dataset is over 1,000,000 or even higher up to billions, the need for distributed computation is somewhat required. This project utilizes three PUBG-related dataset (two of them with 1,000,000 rows) from Kaggle. This ETL process loads the dataset into AWS S3 bucket, creates the AWS EMR cluster, loads and transforms the dataset within the EMR cluster-end by using pySpark, then write the transformed dataset back to AWS S3 in csv format. Then finally, it extracts the dataset in the S3 to final Fact and Dimension tables in AWS Redshift. All of the above series of steps are orchestrated by AirFlow. Structure of the Fact/Dimension tables are made based on the future analytical queries.

# ⚡︎ Data Source
- [PUBG - Aggregate](https://www.kaggle.com/skihikingkevin/pubg-match-deaths?select=aggregate) - match-related dataset <br>
- [PUBG - Death](https://www.kaggle.com/skihikingkevin/pubg-match-deaths?select=deaths) - kill/death-related dataset <br>
- [PUBG - Weapon](https://www.kaggle.com/aadhavvignesh/pubg-weapon-stats?select=pubg-weapon-stats.csv) - weapons-related dataset

# 🚩 Tech
- Python
- AirFlow
- Docker
- AWS S3
- AWS EMR
- AWS Redshift

# → How to Run
1. You need to have AWS CLI configuration ready (AWS credentials + EMR Credentiasl) ([for details](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html))
2. You need 🐳 docker & docker-compose
3. Run the following command in the terminal where you git clone the reposit <br>
```docker-compose -f docker-compose-LocalExecutor.yml up -d```
5. Add your "redshift' account info in the AirFlow Web UI (localhost:8080/admin -> Admin -> Connections)
6. Assign your S3 Bucket name to "BUCKET_NAME" variable in "/dags/spark_submit_airflow.py"
7. Assign your S3 Bucket name to "BUCKET_NAME" variable in "/dags/scripts/spark/spark-scipt.py"
8. Create the S3 bucket with the name you specified for "BUCKET_NAME"
9. Run the dag named "spark_submit_airflow"

# 📘 General Description
![image](https://user-images.githubusercontent.com/26275222/157262095-ef985cd1-29f7-4c8d-8e97-c3db0cbffa82.png)
Above is the total process of ETL process used in this project. All the workflows were controlled by AirFlow. Raw dataset is stored in AWS S3 bucket and all the data wrangling process is handled by AWS EMR cluster (mostly spark-related work). Then final Fact and Dimension tables are created in AWS Redshift, which supports fast query speed and compuatation due to columnar storage characteristic.

# 🗒 DAG and Tasks
![image](https://user-images.githubusercontent.com/26275222/157407898-47bfa5ec-30f4-4d29-84dc-bc819d59e893.png)
- <strong>start_data_pipeline</strong>: DummyOperator to indicate the successful run of the DAG
- <strong><script_to_s3, data_to_s3></strong>: Load raw data and spark script to S3
- <strong>create_emr_cluster</strong>: Create AWS EMR cluster for spark job
- <strong>add_steps</strong>: Submit a list of work EMR cluster needs to do
- <strong>watch_step</strong>: Check if the EMR cluster and the steps are successfully done
- <strong>terminate_emr_cluster</strong>: Terminate the created EMR cluster after job finished
- <strong>create_tables</strong>: Create Fact/Dimension tables in AWS Redshift
- <strong>load_XXX_table</strong>: Load the output csv file from EMR cluster in S3 to Redsfhit
- <strong>check_data_quality</strong>: Check if the data is successfully stored in Redshift table
- <strong>end_data_pipeline</strong>: DummyOperator to indicate the successful end of the DAG


# 📊 Fact/Dimension Tables
![image](https://user-images.githubusercontent.com/26275222/157388669-a460918c-4dff-4cbc-91cf-2c5deaf36141.png)
kill_log table acts as **FACT table**. Each record represents every kill log during the match and the details of the kill log and relevant players info are stored in other <strong>DIMENSION tables </strong>.
- Detailed information about the match itself (map, game_size, etc...) can be found by JOINING the fact table with "match" table with JOIN key of "match_id".
- "killer_id" and "victim_id" represents unique identifier for the player at specific match. It can be used as JOIN key with "player_id" column of "player" table.
- Detailed "timestamp" information can be retrieved by JOINING "kill_log" table with "time" table with JOIN key of "timestamp".
- Specific information regarding the weapon that was used in the kill log can be found in the "weapon" dimension table. It can be retrieved by JOINING the fact table with "weapon" table.

# 🙋‍♂️ Query Exmaple
## The Most Used Weapon by Map Query
```sql
SELECT m.map AS Map,
       kl.weapon AS Weapon,
       COUNT(*) AS Num
FROM pubg.kill_log AS kl
LEFT JOIN pubg.match AS m ON kl.match_id = m.match_id
LEFT JOIN pubg.time AS t ON kl.timestamp = t.timestamp
LEFT JOIN pubg.weapon AS w ON kl.weapon = w.weapon
WHERE m.map in ('ERANGEL', 'MIRAMAR')
GROUP BY m.map, kl.weapon
ORDER BY m.map, Num DESC
```
By JOINING Fact & Dimension tables, one can get the result of the **Most used Weapon by Map**. The result of the above code would be as follows <br>
<img src="https://user-images.githubusercontent.com/26275222/161499122-959332e6-0fd4-4e02-be7d-792091a1792b.png" width="500" height="200"/>
<img src="https://user-images.githubusercontent.com/26275222/161499171-047cc791-d11c-4824-a9c8-b2dbe8c38faa.png" width="500" height="200"/>

# ✔︎ Reasons for the Tech Stacks
- Often times when Data Engineering work is needed, seemless workflows from Extract to Transform to Load are necessary. These 3 steps can be treated as one single data engineering work and Airflow works as one of the best tools to orchestrate the 3 ETL steps.
- **Airflow** was chosen for orchestration because I was accustomed to working with Python and Airflow is one of the most popular Open Source pipeline framework recognized by many developers in Github. This hugh community enables **quick trouble-shooting**.
- Since AWS Services share the same data center, moving data within the AWS Services guarantees high speed and stability. Thus, AWS S3 was chosen for Storage.
- For data wrangling, Spark was used instead of Hadoop since Spark supports faster speed with the use of in-memory as intermediate data saving storage (replacing HDFS). For this Spark job, AWS EMR was used because it can be created and turned-off easily with Airflow and support Spark. It also supports easy data transfer from AWS S3.
- Lastly, AWS Redshift was used for storing the final Fact/Dimension table because it supports high data transfer speed from AWS S3 by using 'COPY COMMAND'. In spite of the fact that AWS Redshift is a columnar storage, it also supports PostgreSQL. Thus, it can be said AWS Redshift supports both the easy access and fast query speed.

### Why AWS?
- 먼저, 클라우드 서비스로만 데이터 파이프라인을 구성하려고 했습니다. AWS, 구글 클라우드, Azure 등의 옵션이 있었지만, Airflow와 마찬가지로 점유율과 커뮤니티의 크기 면에서 도움을 받을 길이 더 많아보여서 AWS를 선택하였습니다.
### Airflow
- 워크플로우 매니지먼트 플랫폼으로 Airflow, Oozie, Luigi 등등이 있었지만 파이썬을 사용하고 UI가 좀더 직관적이며 tasks 끼리의 dependcies를 쉽게 알아볼 수 있는데 트리나 DAG 형태로 나타내주는 Airflow를 선택하였습니다. 또한 관련 커뮤니티가 현시점에서 가장 커서 초보입장에서 트러블 슈팅에 좀 더 용이할것 같아서 Airflow를 선택하였습니다.
### AWS S3
- AWS S3 에는 raw data가 저장되는 곳 + AWS EMR 에서 spark를 이용한 data transformation 결과 테이블을 저장하는 용도로 사용하였습니다. AWS EMR을 이용한 결과물을 저장하는 용도로는 HDFS 도 사용될 수 있었지만,
  1. AWS EMR 에서 HDFS 저장 비용을 늘리고 싶지 않다는 점
  2. AWS 서비스들은 같은 data center를 공유하고 있으므로 S3를 HDFS 대용으로 이용하여도 네트워크 오버헤드가 크지 않고, 싼 가격으로 데이터를 저장할 수 있다는 점 <br>
  때문에 AWS S3 를 선택하게 되었습니다
  
### AWS EMR
- Data Transformation을 하는 과정에서 Airflow 서버에서 직접 sql문을 통해서 data wrangling을 할 수도 있었지만,
  1. raw data가 2GB에 해당하여 크다는 점
  2. Airflow operator를 통해서 AWS EMR을 자동 생성 및 종료를 지원하고 AWS 서비스 내에서의 (S3 -> EMR -> S3 -> Redshift) 데이터 이동이 간편하고 빠르다는 점
  3. data transformation 을 위해 잠깐 동안 AWS EMR을 이용하는 것은 비용이 그렇게 크지 않다는 점
  4. Spark를 지원해서 pySpark를 통한 빠른 data transformation이 가능하다는 점
  5. Spark는 intermediate output 저장 시 하드 대신 메모리를 사용한다는 점에서 Hadoop 대신 스파크를 사용 했습니다.<br>
  이러한 점 때문에 AWS EMR을 이용하여 data transformation 을 진행하였습니다
  
### AWS Redshift
- 데이터 웨어하우스로 AWS Aurora, RDS 등 사양한 서비스들이 사용될 수 있지만,
  1. Column based Redshift가 다른 DB 보다 더 빠른 성능을 보여준다는 점
  2. PostgreSQL 을 지원하여 S3 로부터 COPY COMMAND로 빠르게 데이터를 적재할 수 있다는 점
  3. Redshift가 OLAP 와 같은 analytical query 에서 더 뛰어난 성능을 보여준다는 점 <br>
  때문에 AWS Redshift를 데이터 웨어하우스로 사용하였습니다.
  
  
*****
*****
  
# 🤔 Struggle Points
### S3, Redshift 관련
- [S3, Redshift] Region 을 동일시하면, data transfer 속도가 빨라짐 (같은 데이터 센터 내에 있기 때문)
- COPY COMMAND 작성시, 옮기려는 파일(json, csv, parquet) 데이터의 헤더(HEADER)가 있는지 없는지 굉장히 중요하다. Redshift에 이미 Columns들을 만들었다면, **ignoreheader=1** 옵션을 꼭 넣어줘야함
  - ignoreheader=1 옵션을 추가했으므로, Redshift에서 레코드를 읽을때 컬럼명 정보 없이 값들만 순서대로 읽으므로, Redshift 컬럼명 정의할 때 **순서가 중요**
- COPY COMMAND 작성시, 옵션에 "TIMEFORMAT 'YYYY-MM-DD HH:MI:SS" 추가해줘야함
  - 옵션 안 적을시 Redshift에 "yyyy-MM-dd HH:mm:ss.0" 형식으로 default 로 저장
  
### AirFlow 관련
- Airflow 버전별(v1, v2)로 CustomOperator 라이브러리와 사용법이 다르므로 주의할 것. 현 프로젝트는 v1.1 기준
- 가끔 먹통이 될 때가 있는데, web server 리부팅하자

### pySpark 관련
- string으로 된 다양한 timestamp 포맷(yyy-MM-dd'T'HH:mm:ssZ) 모두 처리가능
- 'row_number()' 와 'window 함수' 조합으로 unique index column 추가 가능
- pySpark → S3 로 write 할 때 (df.write.csv("s3a://xxxxx", timestampFormat="....")
  - timestampFormat 인자를 지정하지 않으면 default 포맷으로 write 되므로 원하는 포맷이 있으면 꼭 명시해줘야함 timestampFormat = "yyyy-MM-dd HH:mm:ss"
- Unix timestamp(정수 13자리, 밀리초) 처리를 주의
  - to_timestamp(): [string 형식 → timestamp 형식] 변환
  - unix_timestamp(): [일반 timestamp 형식 → unix timestamp 형식] 변환
  - [Unix timestamp 관련 함수](https://jin03114.tistory.com/26?category=1025805)

### AWS EMR 관련
- Airflow EmrTerminateJobFlowOperator 를 써서 EMR Auto termination 명령을 내릴 때 **EMR version이 5.34.0 이상**이어야함

### 테이블 및 PostgreSQL 관련
- NUMERIC 타입은 Numeric(precision, scale) 인자를 가질 수 있는데, precision은 전체(소수점포함) 숫자 길이를 뜻하고 scales은 소수점자리를 뜻한다. 따라서 Numeric(5,2)는 (-999.99 ~ 999.99 까지 커버가능). **default scale 값이 0** 이기 때문에 생략하면 소수점 숫자를 표기할 수 없음!!!
- distkey, sortkey 추가해보고 
  
# 🏃 Improvement to be done
- Redshift table에 distribution style, sorting key 추가해서 쿼리 성능 검증해보기
  - dist/sort key는 2개 이상의 node로 구성된 cluster에서 효과가 나옴
  - why? distkey 자체가 각 node에 데이터를 고르게 분배해서 shuffling overhead를 줄이는 것이 목적인데 node가 1개면 어차피 한 node에서 JOIN이 일어나니 효과 X
- Redshift table에 BI Tool 연결해서 analytics 해보기
- Full refresh (DAG 돌릴때마다 모든 것을 전부 새로 ETL) 말고 Execution date 를 기준으로 backfilling 해보기

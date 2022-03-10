# PUBG - ETL

# 📖 Overview
This project is to practice the ETL process with Player Unknown Battle Ground(PUBG) dataset. Often times, dataset below 10,000 rows can easily be handled within a single server. However, when the size of the dataset is over 1,000,000 or even higher up to billions, the need for distributed computation is somewhat required. This project utilizes three PUBG-related dataset (two of them with 1,000,000 rows) from Kaggle. This ETL process loads the dataset into AWS S3 bucket, creates the AWS EMR cluster, loads and transforms the dataset within the EMR cluster-end by using pySpark, then write the transformed dataset back to AWS S3 in csv format. Then finally, it extracts the dataset in the S3 to final Fact and Dimension tables in AWS Redshift. All of above series of steps are orchestrated by AirFlow. Structure of the Fact/Dimension tables are made based on the future analytical queries.

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

# DAG and Tasks
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
kill_log table acts as <strong>FACT table </strong>. Each record represents every kill log during the match and the details of the kill log and relevant players info are stored in other <strong>DIMENSION tables </strong>.
- Detailed information about the match itself (map, game_size, etc...) can be found by JOINING the fact table with "match" table with JOIN key of "match_id".
- "killer_id" and "victim_id" represents unique identifier for the player at specific match. It can be used as JOIN key with "player_id" column of "player" table.
- Detailed "timestamp" information can be retrieved by JOINING "kill_log" table with "time" table with JOIN key of "timestamp".
- Specific information regarding the weapon that was used in the kill log can be found in the "weapon" dimension table. It can be retrieved by JOINING the fact table with "weapon" table.

# 🙋‍♂️ Query Exmaple
```sql
SELECT kl.weapon AS Weapon, m.map AS Map,
p1.player_name AS Killer, p1.team_placement AS Killer_placement, p1.player_kills AS Killer_kill, p1.player_dmg AS Killer_dmg,
p2.player_name AS Victim, p2.team_placement AS Killer_placement, p2.player_kills AS Vivctim_kill, p2.player_dmg AS Victim_dmg
FROM pubg.kill_log as kl
LEFT JOIN pubg.match as m ON kl.match_id = m.match_id
LEFT JOIN pubg.time as t ON kl.timestamp = t.timestamp
LEFT JOIN pubg.player as p1 ON kl.killer_id = p1.player_id
LEFT JOIN pubg.player as p2 ON kl.victim_id = p2.player_id
LEFT JOIN pubg.weapon as w ON kl.weapon = w.weapon
LIMIT 3
```
By JOINING Fact & Dimension tables, one can get the detilaed information regarding the kill log of the match. The result of the above code would be as follows
![image](https://user-images.githubusercontent.com/26275222/157410400-bf421080-f64b-41f7-ae1c-42b710b6cea0.png)

# ✔︎ Reasons for the Tech Stacks
- Often times when Data Engineering work is needed, seemless workflows from Extract to Transform to Load are necessary. These 3 steps can be treated as one single data engineering work and Airflow works as one of the best tools to orchestrate the 3 ETL steps.
- Since AWS Services share the same data center, moving data within the AWS Services guarantees high speed and stability. Thus, AWS S3 was chosen for Storage.
- For data wrangling, Spark was used instead of Hadoop since Spark supports faster speed with the use of in-memory as intermediate data saving storage (replacing HDFS). For this Spark job, AWS EMR was used because it can be created and turned-off easily with Airflow and support Spark. It also supports easy data transfer from AWS S3.
- Lastly, AWS Redshift was used for storing the final Fact/Dimension table because it supports high data transfer from AWS S3 by using 'COPY COMMAND'. In spite of the fact that AWS Redshift is a columnar storage, it also supports PostgreSQL. Thus, it can be said AWS Redshift supports both the easy access and fast query speed.

<br>
***
  
# 🤔 Struggle Points
  
## S3, Redshift 관련
- [S3, Redshift] Region 을 동일시하면, data transfer 속도가 빨라짐 (같은 데이터 센터 내에 있기 때문)
- COPY COMMAND 작성시, 옮기려는 파일(json, csv, parquet) 데이터의 헤더(HEADER)가 있는지 없는지 굉장히 중요하다. Redshift에 이미 Columns들을 만들었다면, **ignoreheader=1** 옵션을 꼭 넣어줘야함
  - ignoreheader=1 옵션을 추가했으므로, Redshift에서 레코드를 읽을때 컬럼명 정보 없이 값들만 순서대로 읽으므로, Redshift 컬럼명 정의할 때 **순서가 중요**
- COPY COMMAND 작성시, 옵션에 "TIMEFORMAT 'YYYY-MM-DD HH:MI:SS" 추가해줘야함
  - 옵션 안 적을시 Redshift에 "yyyy-MM-dd HH:mm:ss.0" 형식으로 default 로 저장
  
## AirFlow 관련
- Airflow 버전별(v1, v2)로 CustomOperator 라이브러리와 사용법이 다르므로 주의할 것. 현 프로젝트는 v1.1 기준
- 가끔 먹통이 될 때가 있는데, web server 리부팅하자

## pySpark 관련
- string으로 된 다양한 timestamp 포맷(yyy-MM-dd'T'HH:mm:ssZ) 모두 처리가능
- 'row_number()' 와 'window 함수' 조합으로 unique index column 추가 가능
- pySpark → S3 로 write 할 때 (df.write.csv("s3a://xxxxx", timestampFormat="....")
  - timestampFormat 인자를 지정하지 않으면 default 포맷으로 write 되므로 원하는 포맷이 있으면 꼭 명시해줘야함 timestampFormat = "yyyy-MM-dd HH:mm:ss"
- Unix timestamp(정수 13자리, 밀리초) 처리를 주의
  - to_timestamp(): [string 형식 → timestamp 형식] 변환
  - unix_timestamp(): [일반 timestamp 형식 → unix timestamp 형식] 변환
  - [Unix timestamp 관련 함수](https://jin03114.tistory.com/26?category=1025805)

## AWS EMR 관련
- Airflow EmrTerminateJobFlowOperator 를 써서 EMR Auto termination 명령을 내릴 때 **EMR version이 5.34.0 이상**이어야함

## 테이블 및 PostgreSQL 관련
- NUMERIC 타입은 Numeric(precision, scale) 인자를 가질 수 있는데, precision은 전체(소수점포함) 숫자 길이를 뜻하고 scales은 소수점자리를 뜻한다. 따라서 Numeric(5,2)는 (-999.99 ~ 999.99 까지 커버가능). **default scale 값이 0** 이기 때문에 생략하면 소수점 숫자를 표기할 수 없음!!!

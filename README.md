# PUBG - ETL

# 📖 Overview
This project is to practice the ETL process with Player Unknown Battle Ground(PUBG) dataset. Often times, dataset below 10,000 rows can easily be handled within a single server. However, when the size of the dataset is over 1,000,000 or even higher up to billions, the need for distributed computation is somewhat required. This project utilizes three PUBG-related dataset (two of them with 1,000,000 rows) from Kaggle. This ETL process loads the dataset into AWS S3 bucket, creates the AWS EMR cluster, loads and transforms the dataset within the EMR cluster-end by using pySpark, then write the transformed dataset back to AWS S3 in csv format. Then finally, it extracts the dataset in the S3 to final Fact and Dimension tables in AWS Redshift. All of above series of steps are orchestrated by AirFlow.

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
1. You need to have AWS CLI configuration ready ([for details](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html))
2. You need 🐳 docker & docker-compose
3. Run the following command in the terminal where you git clone the reposit 'docker-compose -f docker-compose-LocalExecutor.yml up -d'
4. Add your "redshift' account info in the AirFlow Web UI (localhost:8080/admin -> Admin -> Connections)
5. Assign your S3 Bucket name to "BUCKET_NAME" variable in "/dags/spark_submit_airflow.py"
6. Assign your S3 Bucket name to "BUCKET_NAME" variable in "/dags/scripts/spark/spark-scipt.py"
7. Create the S3 bucket with the name you specified for "BUCKET_NAME"
8. Run the dag named "spark_submit_airflow"

# 📘 General Description
![image](https://user-images.githubusercontent.com/26275222/157262095-ef985cd1-29f7-4c8d-8e97-c3db0cbffa82.png)
Above is the total process of ETL process used in this project. All the workflows were controlled by AirFlow. Raw dataset is stored in AWS S3 bucket and all the data wrangling process is handled by AWS EMR cluster (mostly spark-related work). Then final Fact and Dimension tables are created in AWS Redshift, which supports fast query speed and compuatation due to columnar storage characteristic.

# DAG and Tasks
![image](https://user-images.githubusercontent.com/26275222/157407898-47bfa5ec-30f4-4d29-84dc-bc819d59e893.png)
- start_data_pipeline: DummyOperator to indicate the successful run of the DAG
- <script_to_s3, data_to_s3>: Load raw data and spark script to S3
- create_emr_cluster: Create AWS EMR cluster for spark job
- add_steps: Submit a list of work EMR cluster needs to do
- watch_step: Check if the EMR cluster and the steps are successfully done
- terminate_emr_cluster: Terminate the created EMR cluster after job finished
- create_tables: Create Fact/Dimension tables in AWS Redshift
- load_XXX_table: Load the output csv file from EMR cluster in S3 to Redsfhit
- check_data_quality: Check if the data is successfully stored in Redshift table
- end_data_pipeline: DummyOperator to indicate the successful end of the DAG


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
![image](https://user-images.githubusercontent.com/26275222/157399373-993e2d90-655b-4e66-96af-f3d47d5ac115.png)

# ✔︎ Reasons for the Tech Stacks
- Often times when Data Engineering work is needed, seemless workflows from Extract to Transform to Load are necessary. These 3 steps can be treated as one single data engineering work and Airflow works as one of the best tools to orchestrate the 3 ETL steps.
- Since AWS Services share the same data center, moving data from AWS Service to other AWS Services guarantee high speed and stability. Thus, AWS S3 was chosen for Storage.
- For data wrangling, Spark was used instead of Hadoop since Spark supports faster speed with the use of in-memory as intermediate data saving storage (replacing HDFS). For this Spark job, AWS EMR was used because it can be created and turned-off easily with Airflow and support Spark. It also supports easy data transfer from AWS S3.
- Lastly, AWS Redshift was used for storing the final Fact/Dimension table because it supports high data transfer from AWS S3 by using 'COPY COMMAND'. In spite of the fact that AWS Redshift is a columnar storage, it also supports PostgreSQL. Thus, it can be said AWS Redshift supports both the easy access and fast query speed.



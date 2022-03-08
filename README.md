# PUBG - ETL

# 📖 Overview
This project is to practice the ETL process with Player Unknown Battle Ground(PUBG) dataset. Often times, dataset below 10,000 rows can easily be handled within a single server. However, when the size of the dataset is over 1,000,000 or even higher up to billions, the need for distributed computation is somewhat required. This project utilizes three PUBG-related dataset (two of them with 1,000,000 rows) from Kaggle. This ETL process loads the dataset into AWS S3 bucket, creates the AWS EMR cluster, loads and transforms the dataset within the EMR cluster-end by using pySpark, then write the transformed dataset back to AWS S3 in csv format. Then finally, it extracts the dataset in the S3 to final Fact and Dimension tables in AWS Redshift. All of above series of steps are orchestrated by AirFlow.

# ⚡︎ Data Source
[PUBG - Aggregate](https://www.kaggle.com/skihikingkevin/pubg-match-deaths?select=aggregate) - match-related dataset <br>
[PUBG - Death](https://www.kaggle.com/skihikingkevin/pubg-match-deaths?select=deaths) - kill/death-related dataset <br>
[PUBG - Weapon](https://www.kaggle.com/aadhavvignesh/pubg-weapon-stats?select=pubg-weapon-stats.csv) - weapons-related dataset

# 🚩 Tech
>- Python
>- AirFlow
>- Docker
>- AWS S3
>- AWS EMR
>- AWS Redshift

# General Description
Below is the total process of ETL process used in this project. All the workflows were controlled by AirFlow. Raw dataset is stored in AWS S3 bucket and all the data wrangling process is handled by AWS EMR cluster (mostly spark-related work). Then final Fact and Dimension tables are created in AWS Redshift, which supports fast query speed and compuatation due to columnar storage characteristic.
![image](https://user-images.githubusercontent.com/26275222/157262095-ef985cd1-29f7-4c8d-8e97-c3db0cbffa82.png)


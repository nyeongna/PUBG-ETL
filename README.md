# PUBG - ETL

# 📖 Overview
This project is to practice the ETL process with Player Unknown Battle Ground(PUBG) dataset. Often times, dataset below 10,000 rows can easily be handled within a single server. However, when the size of the dataset is over 1,000,000 or even higher up to billions, the need for distributed computation is somewhat required. This project utilizes three PUBG-related dataset (two of them with 1,000,000 rows) from Kaggle. This ETL process loads the dataset into AWS S3 bucket, creates the AWS EMR cluster, loads and transforms the dataset within the EMR cluster-end by using pySpark, then write the transformed dataset back to AWS S3 in csv format. Then finally, it extracts the dataset in the S3 to final Fact and Dimension tables in AWS Redshift. All of above series of steps are orchestrated by AirFlow.

# Data Source
[SITCA - Volume](https://www.sitca.org.tw/ROC/Industry/IN2610.aspx?pid=IN22603_03) <br>
[STICA - Open Interests](https://www.sitca.org.tw/ROC/Industry/IN2608.aspx?pid=IN22603_01)

# 🚩 Tech
>- Python
>- Pandas
>- BeautifulSoup
>- Openpyxl



# Code & Details
[SITCA Web Scraping - Get Volume and Open Interest](https://nbviewer.jupyter.org/gist/nyeongna/36bbba3da3007547e921015227e50185)

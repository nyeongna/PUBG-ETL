# pyspark
import argparse

from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import array_contains

def etl(spark):
    agg_csv = "s3a://pubg-etl-project/data/agg_small.csv"
    kill_csv = "s3a://pubg-etl-project/data/kill_small.csv"
    agg_df = spark.read.option("header", True).csv(agg_csv)
    kill_df = spark.read.option("header", True).csv(kill_csv)
    
    agg_df.write.csv("s3a://pubg-etl-project/clean_data/agg",mode='overwrite',header=True)
    kill_df.write.csv("s3a://pubg-etl-project/clean_data/kill",mode='overwrite',header=True)
    
    

if __name__ == "__main__":
    spark = SparkSession.builder.appName("pubg-etl").getOrCreate()
    etl(spark)

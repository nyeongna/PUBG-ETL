# pyspark
from pyspark.sql.functions import md5, concat, lit
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.functions import from_unixtime, to_timestamp, to_date, unix_timestamp
from pyspark.sql.functions import regexp_replace, monotonically_increasing_id
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql import SparkSession

# Assign "S3_BUCKET" with your S3 Bucket name
BUCKET_NAME = "pubg-etl-project"
def etl(spark):
    
    agg_csv = f"s3a://{BUCKET_NAME}/data/agg_small.csv"
    kill_csv = f"s3a://{BUCKET_NAME}/data/kill_small.csv"
    weapon_csv = f"s3a://{BUCKET_NAME}/data/weapon.csv"

    agg_df = spark.read.option("header", True).csv(agg_csv, inferSchema=True)
    kill_df = spark.read.option("header", True).csv(kill_csv, inferSchema=True)
    weapon_df = spark.read.option("header", True).csv(weapon_csv, inferSchema=True)
    
    '''
    Data Wrangling: kill_df
    '''
    # Check size of original kill_df
    print('original df size: ', kill_df.count())
    
    # Drop kill-log where 'killer_name': NaN(bluezone), 'victim_name': NaN
    kill_df = kill_df.na.drop(how='any', subset=['killer_name', 'victim_name'])
    
    # Drop kill-log where 'killer_name' & 'victim_name' are unknown
    kill_df = kill_df.filter(~kill_df.killer_name.contains('#unknown'))
    kill_df = kill_df.filter(~kill_df.victim_name.contains('#unknown'))
        
    # Test Duplicates
    dup_df = kill_df.groupBy('killer_name', 'victim_name', 'match_id').count().filter("count > 1")
    kill_df = kill_df.dropDuplicates(['killer_name', 'victim_name', 'match_id'])
    print('# of dup: ', dup_df.count())

    # Produce killer_id (killer_name + match_id)
    kill_df = kill_df.withColumn('killer_id', concat(kill_df.killer_name, kill_df.match_id))
    
    # Produce victim_id (victim_name + match_id)
    kill_df = kill_df.withColumn('victim_id', concat(kill_df.victim_name, kill_df.match_id))
    
    # Test view kill_df
    #kill_df.select(['time','killer_id', 'killer_name', 'match_id','victim_id','kill_log_id']).show(4)

    '''
    Data Wrangling: agg_df
    '''
    # Drop where 'player_name': NaN (Not Null value)
    agg_df = agg_df.na.drop(how='any', subset=['player_name'])
    
    # JOIN kill_df + agg_df
    kill_df.createOrReplaceTempView("kill")
    agg_df.createOrReplaceTempView("agg")
    kg_df = spark.sql('''
                        SELECT *
                        FROM kill
                        JOIN agg
                            ON kill.match_id = agg.match_id AND kill.killer_name = agg.player_name
                        ORDER BY kill.killer_name
    ''')
    # Get Timestamp (agg:date + kill:time)
    kg_df = kg_df.withColumn("date", to_timestamp(kg_df.date, "yyyy-MM-dd'T'HH:mm:ssZ"))
    kg_df = kg_df.withColumn("date", to_timestamp(unix_timestamp(kg_df.date) + kg_df.time))
    kg_df = kg_df.withColumnRenamed('date', 'timestamp')
    
    ''' ---------------------------------------Table Making-----------------'''
    # ---Time---Dimension
    # Create time df
    time_df = kg_df.select(['timestamp'])
    # Drop duplicates and NULL timestamp
    time_df = time_df.dropDuplicates(['timestamp'])
    time_df = time_df.na.drop(how='any', subset=['timestamp']) 
    # Create 'year', 'month', 'day', ....
    time_df = time_df.withColumn('year', year(time_df['timestamp']))
    time_df = time_df.withColumn('month', month(time_df['timestamp']))
    time_df = time_df.withColumn('day', dayofmonth(time_df['timestamp']))
    time_df = time_df.withColumn('week', weekofyear(time_df['timestamp']))
    time_df = time_df.withColumn('weekday', dayofweek(time_df['timestamp']))
    time_df = time_df.withColumn('hour', hour(time_df['timestamp']))
    # write time_df to S3
    # ---Time---Dimension END
    
    # ---match---Dimension
    # Create match df
    match_df = kg_df.select(['kill.match_id', 'map', 'match_mode', 'game_size'])
    match_df = match_df.withColumnRenamed('kill.match_id', 'match_id')
    # Drop duplicates match_id
    match_df = match_df.dropDuplicates(['match_id'])
    # ---match---Dimension END
    
    # ---Weapon---Dimension
    # Drop duplicates 'Weapon Name'
    weapon_df = weapon_df.dropDuplicates(['Weapon Name'])
    # Change column names according to the format
    weapon_df = weapon_df.withColumnRenamed('Weapon Name', 'weapon')
    weapon_df = weapon_df.withColumnRenamed('Weapon Type', 'weapon_type')
    weapon_df = weapon_df.withColumnRenamed('Bullet Type', 'bullet_type')
    weapon_df = weapon_df.withColumnRenamed('Damage', 'damage')
    weapon_df = weapon_df.withColumnRenamed('Magazine Capacity', 'megazine_capacity')
    weapon_df = weapon_df.withColumnRenamed('Range', 'range')
    weapon_df = weapon_df.withColumnRenamed('Bullet Speed', 'bullet_speed')
    weapon_df = weapon_df.withColumnRenamed('Rate of Fire', 'rate_of_fire')
    weapon_df = weapon_df.withColumnRenamed('Damage Per Second', 'dps')
    weapon_df = weapon_df.withColumnRenamed('Fire Mode', 'fire_mode')
    
    # Change some of the weapon names according to the format
    weapon_df = weapon_df.withColumn('weapon', regexp_replace('weapon','Kar98', 'Kar98k'))
    weapon_df = weapon_df.withColumn('weapon', regexp_replace('weapon','Mini14', 'Mini 14'))
    weapon_df = weapon_df.withColumn('weapon', regexp_replace('weapon','MK14', 'Mk14'))
    weapon_df = weapon_df.withColumn('weapon', regexp_replace('weapon','Thompson', 'Tommy Gun'))
    weapon_df = weapon_df.withColumn('weapon', regexp_replace('weapon','Uzi', 'Micro Uzi'))
    weapon_df = weapon_df.select(['weapon', 'weapon_type', 'bullet_type',
                                  'damage', 'megazine_capacity', 'range',
                                  'bullet_speed', 'rate_of_fire'])
    # ---Weapon---Dimension END
    
    # ---Player---Dimension
    # Creating player df
    player_df = agg_df.select(['match_id', 'player_name', 'party_size', 'player_assists',
                              'player_dbno', 'player_dist_ride', 'player_dist_walk', 'player_dmg',
                              'player_kills', 'player_survive_time', 'team_id', 'team_placement'])
    # Handle Null values on ['player_name' OR 'match_id']
    player_df = player_df.na.drop(how='any', subset=['player_name', 'match_id']) 
    
    # Drop duplicates player
    player_df = player_df.dropDuplicates(['player_name', 'match_id'])
    
    #player_df.groupBy('player_name', 'match_id').count().filter("count > 1").show()
    # Create PRIMARY KEY 'player_id' by concatenating 'player_name' and 'match_id'
    player_df = player_df.withColumn('player_id', concat(player_df.player_name, player_df.match_id))
    player_df = player_df.select(['player_id','match_id', 'player_name', 'party_size', 'player_assists',
                            'player_dbno', 'player_dist_ride', 'player_dist_walk', 'player_dmg',
                            'player_kills', 'player_survive_time', 'team_id', 'team_placement'])  
    print('# of dup in player_df ', player_df.select(['player_id', 'player_name', 'match_id', 'player_dmg']).count())
    # ---Player---Dimension END
    
    # ---Kill-log---Fact
    # Creating kill_log df
    kill_log_df = kg_df.select(['killer_id', 'victim_id', 'kill.match_id', 'timestamp',
                                'killed_by', 'killer_position_x', 'killer_position_y',
                                'victim_position_x', 'victim_position_y'])
    # Change 'killed_by' to 'weapon'
    kill_log_df = kill_log_df.withColumnRenamed('killed_by', 'weapon')
    
    # Add kill_log_id (Unique Index) to kill_log_df
    window = Window.orderBy(kill_log_df.killer_id)
    kill_log_df = kill_log_df.withColumn('kill_log_id', row_number().over(window))
    kill_log_df = kill_log_df.select(['kill_log_id','killer_id', 'victim_id', 'kill.match_id', 'timestamp',
                            'weapon', 'killer_position_x', 'killer_position_y',
                            'victim_position_x', 'victim_position_y'])
    # ---Kill-log---Fact END
    
    '''--------------------------------------------Final table schema and data testing '''
    match_df.printSchema()
    match_df.show(2)
    
    time_df.printSchema()
    time_df = time_df.orderBy(col('timestamp').desc())
    time_df.show(2)
    time_df.describe()
    
    player_df.printSchema()
    player_df.select(['player_id', 'player_name', 'match_id']).show(2)
    
    weapon_df.printSchema()
    weapon_df.select(['weapon', 'megazine_capacity']).show(2)
    
    kill_log_df.printSchema()
    kill_log_df.select(['kill_log_id', 'killer_id', 'victim_id', 'weapon', 'timestamp']).show(3)
    
    match_df.write.csv(f"s3a://{BUCKET_NAME}/clean_data/match",mode='overwrite',header=True)
    time_df.write.csv(f"s3a://{BUCKET_NAME}/clean_data/time",mode='overwrite',header=True,
                            timestampFormat = "yyyy-MM-dd HH:mm:ss")
    player_df.write.csv(f"s3a://{BUCKET_NAME}/clean_data/player",mode='overwrite',header=True)
    weapon_df.write.csv(f"s3a://{BUCKET_NAME}/clean_data/weapon",mode='overwrite',header=True)
    kill_log_df.write.csv(f"s3a://{BUCKET_NAME}/clean_data/kill_log",mode='overwrite',header=True, timestampFormat = "yyyy-MM-dd HH:mm:ss")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("pubg-etl").getOrCreate()
    etl(spark)

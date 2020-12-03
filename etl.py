import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import *
from pyspark.sql.functions import monotonically_increasing_id
from zipfile import ZipFile
import pandas as p
import pyspark.sql.functions as F


config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']



def create_spark_session():
    
    """
Initiate SPARK session to run the stages to process the individula tasks that attched to process the datasets
    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark
spark=create_spark_session()



def process_song_data(spark, input_data, output_data):
    """
create function that includes extraction for SONG-DATA dataset and exract the column to build the corresponding tables in the dimension model and writing these tables in parquet files for ease storage and processing
    """
    

    df_song= spark.read.json(input_data +"song_data/*/*/*/*.json")
    
    songs_table=df_song.select("song_id","title","artist_id","year","duration").distinct()
    songs_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet(os.path.join(output_data, 'songs'))


    artists_table=df_song.select("artist_id","artist_name", "artist_location", "artist_latitude", "artist_longitude").distinct()
    artists_table.write.mode('overwrite').parquet(os.path.join(output_data, 'artists'))

    

def process_log_data(spark, input_data, output_data):

    """
create function that includes extraction for LOG-DATA dataset and exract the column to build the corresponding tables in the dimension model and writing these tables in parquet files for ease storage and processing
also user defined function UDF to handle the casing for time column into readable format inorder to feed the TIME table 
df_song is included here again due to it is required  to be used for songplays table that is genereated by joining df_song with df_log
    """    
    df_song= spark.read.json(input_data +"song_data/*/*/*/*.json")
    df_Log = spark.read.json(input_data + "log_data/*/*/*.json")

   

    df_log_users = df_Log.filter("page = 'NextSong'").filter("userId != ''") 
    users_table=df_log_users.select("userId", "firstName" , "lastName" , "gender","level").distinct()
    users_table.write.mode('overwrite').parquet(os.path.join(output_data, 'users'))


    get_timestamp = udf(lambda epoch: datetime.fromtimestamp(epoch/1000),TimestampType()) 
    
    df = df_Log.filter("page = 'NextSong'").withColumn("ts",get_timestamp("ts"))
    
    time_table = df.selectExpr("ts","hour(ts) as hour","dayofmonth(ts) as day","weekofyear(ts) as week","month(ts) as month",\
    "year(ts) as year","dayofweek(ts) as weekday").distinct().orderBy("ts")
    
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(os.path.join(output_data, 'time'))

    
    
    
    df_log_action = df_Log.filter("page = 'NextSong'")
    
    songplays_table = df_log_action.join(df_song, (df_log_action.song == df_song.title) &\
    (df_log_action.artist == df_song.artist_name) & (df_log_action.length == df_song.duration), 'left_outer').\
    select(df_log_action.ts,col("userId").alias('user_id'),df_log_action.level,df_song.song_id,df_song.artist_id,\
    col("sessionId").alias("session_id"),df_log_action.location,col("useragent").alias("user_agent"))
    
    songplays_table=songplays_table.withColumn("ts",F.to_timestamp("ts"))

    songplays_table.withColumn("year",year('ts')).withColumn("month",month('ts')).write.mode("overwrite")\
    .partitionBy("year","month").parquet(os.path.join(output_data, 'songplays'))

    
    

def main():
    """
main function that includes input and output paths on S3 that required by the avove functions process_song_data and process_log_data
    """        
    
    spark = create_spark_session()
    input_data = conFig.get('IO','INPUT_DATA')
    output_data = conFig.get('IO','OUT_DATA')

    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
import os
import configparser
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl
from pyspark.sql.types import StringType as Str, IntegerType as Int, DateType as Dat, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function creates a new Spark session with the specified configuration or retrieves 
    the existing spark session and updates the configuration
    
    :return spark: Spark session
    """
    
    spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .getOrCreate()
    
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function loads the song_data from AWS S3 (input_data) and extracts the songs and artist tables.
    It then loads the processed data back to S3 (output_data)
    
    Parameters
    spark: Spark Session object
    input_data: Location (AWS S3 path) of song_data JSON files
    output_data: Location (AWS S3 path) where output data will be stored in parquet format 
    """
    
    # get filepath to song data file
    song_data = os.path.join(input_data + 'song_data/A/A/A/*.json')
            
    # read song data file
    df = spark.read.json(song_data)
    print("Read completed")
    
    # create temp view of song data for songplays table to join
    df.createOrReplaceTempView("song_data_view")
    
    # extract columns to create songs table
    song_fields = ["song_id", "title", "artist_id", "year", "duration"]
    songs_table = df.select(song_fields).dropDuplicates()

    print("Writing Songs table to S3 after processing")
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs/", mode="overwrite", partitionBy=["year","artist_id"])
    print("Write completed")
    
    # Extract columns to create artists table
    artists_fields = ["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude",
                      "artist_longitude as longitude"]
    artists_table = df.selectExpr(artists_fields).dropDuplicates()

    print("Writing Artists table to S3 after processing")
    # Write artists table to parquet files
    artists_table.write.parquet(output_data + "artists/", mode="overwrite")
    print("Write completed")
    

def process_log_data(spark, input_data, output_data):
    """
    This function loads the log_data from AWS S3 and extracts the songs and artist tables
    It then loads the processed data back to S3 (output_data)
    
    Parameters
    spark: Spark Session object
    input_data: Location (AWS S3 path) of songs metadata (song_data) JSON files
    output_data: Location (AWS S3 path) where dimensional tables will be stored in parquet format             
    """
    
    # ret filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    print("Reading log_data JSON files from S3")
    log_df = spark.read.json(log_data)
    print("Read completed")
    
    # Filter by actions for song plays
    log_df = log_df.filter(log_df.page == 'NextSong')

    # Extract columns for users table 
    users_fields = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table = log_df.selectExpr(users_fields).dropDuplicates()
    
    # write users table to parquet files
    print("Writing Users table to S3 after processing")  
    users_table.write.parquet(os.path.join(output_data, "users/") , mode="overwrite")
    print("Write completed")
    
#     # Create timestamp column from original timestamp column
#     get_timestamp = udf(lambda x: x / 1000, TimestampType())
#     log_df = log_df.withColumn("timestamp", get_timestamp(log_df.ts))
    
#      # create datetime column from original timestamp column
#     get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
#     log_df = log_df.withColumn("start_time", get_datetime(log_df.timestamp))
    
    # Extract columns to create time table
    # extract columns to create time table
    log_df = log_df.withColumn('start_time', (log_df['ts']/1000).cast('timestamp'))
    log_df = log_df.withColumn('weekday', date_format(log_df['start_time'], 'E'))
    log_df = log_df.withColumn('year', year(log_df['start_time']))
    log_df = log_df.withColumn('month', month(log_df['start_time']))
    log_df = log_df.withColumn('week', weekofyear(log_df['start_time']))
    log_df = log_df.withColumn('day', dayofmonth(log_df['start_time']))
    log_df = log_df.withColumn('hour', hour(log_df['start_time']))
    time_table = log_df.select('start_time', 'weekday', 'year', 'month',\
                           'week', 'day', 'hour').distinct()
    

    # write time table to parquet files partitioned by year and month
    print("Writing Time table to S3 after processing")  
    time_table.write.mode('overwrite').partitionBy('year', 'month')\
              .parquet(path=output_data + 'time')
    print("Write completed")
    
    # Read in song data to use for songplays table
    songs_df = spark.sql("SELECT * FROM song_data_view")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = log_df.join(songs_df, (log_df.song == songs_df.title)\
                                       & (log_df.artist == songs_df.artist_name)\
                                       & (log_df.length == songs_df.duration), "inner")\
                        .distinct()\
                        .select('start_time', 'userId', 'level', 'song_id',\
                                'artist_id', 'sessionId','location','userAgent',\
                                log_df['year'].alias('year'), log_df['month'].alias('month'))\
                        .withColumn("songplay_id", monotonically_increasing_id())


    # write songplays table to parquet files partitioned by year and month
    
    print("Writing Songplays table to S3 after processing") 
    
    songplays_table.write.mode("overwrite").partitionBy('year', 'month')\
                   .parquet(path=output_data + 'songplays')
    print("Write completed")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-udacity-obi/"
    
    print("\n")
    
    print("Processing song_data files")
    process_song_data(spark, input_data, output_data)
    print("Processing completed\n")
    
    print("Processing log_data files")
    process_log_data(spark, input_data, output_data)
    print("Processing completed\n")


if __name__ == "__main__":
    main()
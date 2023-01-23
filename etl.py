import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    function initiate spark session and return the application variable (spark)
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    - function takes 3 args spark session variable, source file path, and destination file path,
    - to get the file path data file, 
    - read songs data file,
    - extract columns to create songs table,
    - write songs table to parquet files partitioned by year and artist in the destination path (s3),
    - extract columns to create artists table,
    - and write artists table to parquet files.
    """
    
    # get filepath to song data file
    song_data = os.path.join(input_data + 'song_data/A/A/A/*.json')
    
    # read song data file
    df = spark.read.json(song_data)
    
    # create temp view for songs data
    df.createOrReplaceTempView('staging_songs_table')
    
    # extract columns to create songs table (song_id, title, artist_id, year, duration)
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
       
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year','artist_id').parquet(os.path.join(output_data,'songs'))
    
    # extract columns to create artists table(artist_id, name, location, lattitude, longitude)
    artists_table = spark.sql("select artist_id ,\
                               artist_name as name,\
                               artist_location as location,\
                               artist_latitude as lattitude,\
                               artist_longitude as longitude\
                              from staging_songs_table")
    
    # create temp view for artist table
    artists_table.createOrReplaceTempView('artists_table')
        
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(os.path.join(output_data,'artists'))
    
def process_log_data(spark, input_data, output_data):
    """
    - function takes 3 args spark session, input data file path, and output data file path,
    - to get filepath to log data file,
    - read log data file,
    - filter by actions for song plays (page = NextSong),
    - extract columns for users table,
    - write users table to parquet files in the destnation s3,
    - create timestamp and datetime columns from original timestamp column,
    - extract columns to create time table,
    - write time table to parquet files partitioned by year and month in the destnation s3,
    - and extract columns from joined song and log datasets to create songplays table
    """
    
    # get filepath to log data file
    log_data = input_data+"log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays 
    filtered_df = df.filter(df.page== 'NextSong').select('artist','firstName', 'gender', 'lastName', 'length', 'level', 'location', 'sessionId','song', 'ts', 'userAgent', 'userId').dropDuplicates()
    
    # extract columns for users table (user_id, first_name, last_name, gender, level)    
    user_table = filtered_df.select('userId', 'firstName', 'lastName', 'gender', 'level')

    # write users table to parquet files
    user_table.write.mode('overwrite').parquet(os.path.join(output_data,'users'))

    # create timestamp column from original timestamp column 
    get_timestamp = udf(lambda x:  datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = filtered_df.withColumn("timestamp" ,get_timestamp(filtered_df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # create temp view for log data
    df.createOrReplaceTempView('staging_events_table')
    
    # extract columns to create time table (start_time, hour, day, week, month, year, weekday)
    time_table = spark.sql("select distinct timestamp AS start_time, \
                            extract(hour from datetime) as hour,\
                            extract(day from datetime) as day,\
                            extract(week from datetime) as week,\
                            extract(month from datetime) as month,\
                            extract(year from datetime) as year,\
                            extract(dayofweek from timestamp) as weekday\
                            from staging_events_table")
    
    # create temp view for time table 
    time_table.createOrReplaceTempView('time_table')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(os.path.join(output_data,'time'))
    

    # read in song data to use for songplays table
    song_data = os.path.join(input_data + 'song_data/A/A/A/*.json')
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    songplays_table = spark.sql(" select e.timestamp as start_time,\
                                e.userId as user_id,\
                                e.level,\
                                s.song_id,\
                                s.artist_id,\
                                e.sessionId as session_id,\
                                e.location,\
                                e.userAgent as user_agent, \
                                t.year,\
                                t.month \
                                FROM staging_events_table e \
                                JOIN staging_songs_table s ON e.song = s.title AND e.length = s.duration \
                                JOIN artists_table a ON e.artist = a.name AND a.artist_id = s.artist_id \
                                JOIN time_table t ON e.timestamp = t.start_time\
                                ")

    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year','month').parquet(os.path.join(output_data,'songplays'))


def main():
    """
    main function declare 3 variables spark session, input data file path, output data file path, 
    to pass those as args for process_song_data function and process_log_data function. 
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://israas-bucket/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

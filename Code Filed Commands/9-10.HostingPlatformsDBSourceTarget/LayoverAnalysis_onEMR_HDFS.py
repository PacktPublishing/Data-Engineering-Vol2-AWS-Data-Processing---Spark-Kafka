from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (concat, expr, when, lit, col,
                                   extract, date_format, dense_rank, row_number, rank, count, unix_timestamp)

spark = SparkSession.builder.appName('LayoverAnalysis_onEMR_usingHDFS').getOrCreate()

df1 = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true')\
    .load('hdfs:///flight_data/')

df2 = df1.select('passenger_name', 'travel_date',
                 'departure_time', 'arrival_time','airplane_model')
df2.persist(StorageLevel.DISK_ONLY)

df3 = df2.withColumn('same_day_flag', dense_rank()
                     .over(Window.partitionBy('passenger_name').orderBy('travel_date')))

df4 = df3.groupby('passenger_name', 'travel_date').agg(count('same_day_flag').alias('dup_row_flag'))
df5 = df4.filter((df4.dup_row_flag >= lit(2)) & (df4.dup_row_flag < lit(3)))

df6 = df5.join(df2, (df2.passenger_name == df5.passenger_name) & (df2.travel_date == df5.travel_date), 'inner')\
        .drop('dup_row_flag', df5.passenger_name, df5.travel_date, df2.airplane_model)

df7 = df6.withColumn('dept_rank', dense_rank()
                     .over(Window.partitionBy('passenger_name','travel_date').orderBy('departure_time')))\
         .drop('airplane_model')
df7.persist(StorageLevel.DISK_ONLY)

df81 = df7.filter('dept_rank = 1').withColumnRenamed('departure_time','1st_departure_time')\
                                  .withColumnRenamed('arrival_time','1st_arrival_time')
df82 = df7.filter('dept_rank = 2').withColumnRenamed('departure_time','2nd_departure_time')\
                                  .withColumnRenamed('arrival_time','2nd_arrival_time')

df9 = df81.join(df82, (df81.passenger_name == df82.passenger_name) & (df81.travel_date == df82.travel_date), 'inner')\
          .drop(df81.passenger_name, df81.travel_date, 'dept_rank')

df10 = df9.withColumn('layover_duration_sec',
                      unix_timestamp(col('2nd_departure_time')) - unix_timestamp(col('1st_arrival_time')))\
        .drop('1st_arr_time_sec', '2nd_dep_time_sec')

df11 = df10.select(
        'passenger_name', 'travel_date',
        date_format('1st_departure_time', 'HH:mm:ss'),
        date_format('1st_arrival_time','HH:mm:ss'),
        date_format('2nd_departure_time', 'HH:mm:ss'),
        date_format('2nd_arrival_time', 'HH:mm:ss'),
        'layover_duration_sec')\
        .withColumn('layover_comment', expr("CASE WHEN layover_duration_sec < 3600 THEN 'VERY SHORT LAYOVER' "
             "WHEN layover_duration_sec > 21600 THEN 'VERY LONG LAYOVER'"
             "ELSE 'PROPER LAYOVER RANGE' END "))

df11.write.mode('overwrite').format('parquet').save('hdfs:///ouput_data/')

spark.stop()
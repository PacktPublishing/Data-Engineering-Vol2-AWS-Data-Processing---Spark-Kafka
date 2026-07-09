from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import dense_rank, col, max

spark = SparkSession.builder.appName('project4.2_TravelActivity').getOrCreate()

flight_df = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true')\
    .load('flight_data_csv/flight_data_1.csv')

df = flight_df.select('passenger_name','travel_date', 'passenger_country', 'distance' )\
               .filter('passenger_name = "Úrsula Vila"')\
               .orderBy(col('passenger_name').desc(), col('travel_date').asc())

df11 = df.withColumn('last_removed', dense_rank().over(Window.partitionBy('passenger_name').orderBy('travel_date')))
df_temp = df11.groupby('passenger_name').agg(max('last_removed').alias('last_removed_max'))
var_temp = df_temp.collect()
parm_temp = var_temp[0]['last_removed_max']
df1 = df11.filter(df11.last_removed < parm_temp).withColumnRenamed('last_removed', 'travel_sr_no')

df21 = df.withColumn('first_removed', dense_rank().over(Window.partitionBy('passenger_name').orderBy('travel_date')))
df22 = df21.filter('first_removed > 1').drop('first_removed')
df2 = df22.withColumn('travel_sr_no', dense_rank().over(Window.partitionBy('passenger_name').orderBy('travel_date')))

gap_df = df1.join(df2, (df1.passenger_name == df2.passenger_name) &
                       (df1.travel_sr_no == df2.travel_sr_no),
                  'inner').drop(df2.passenger_name, df2.passenger_country,
                                     df1.distance, df2.distance,
                                     df1.travel_sr_no, df2.travel_sr_no)

gap_df.show()


from pyspark.sql import SparkSession
from pyspark import SparkConf

#############################################################################################################
# Sort Merge Join
#############################################################################################################
# conf = SparkConf()
# conf.set('spark.sql.join.preferSortMergeJoin', 'true')

# spark = (SparkSession.builder.config(conf=conf)
#                      .appName('InternalJoins-SortMergeJoin_3')
#                      .master('')
#                      .getOrCreate())
#
# df1 = spark.read.format('parquet').load('fligt_data_parquet')
#
# df_flight = df1.select('flight_id','aircraft_id','flight_cost','origin_airport','destination_airport',
#                        'departure_time','arrival_time','passenger_name',
#                        'travel_date','airplane_model','tail_no','distance',
#                        'turbulance','temp_at_dept','fuel_consumed_litre','taxi_duration_mins')
#
# df_passenger = df1.select('passenger_name','frequent_flier','frequent_flier_no', 'travel_date',
#                           'passenger_country','passenger_dob','passenger_flight_class')
#
# df_joined = df_flight.join(df_passenger, df_flight.passenger_name == df_passenger.passenger_name, 'right')
# df_joined.show()

#############################################################################################################
# Broadcast Hash Join
#############################################################################################################
# conf = SparkConf()
# conf.set('spark.sql.join.preferSortMergeJoin', 'false')
# conf.set('spark.sql.autoBroadcastJoinThreshold', '524288000')
# conf.set('spark.sql.shuffle.partitions', '50')

# spark = (SparkSession.builder.config(conf=conf)
#                      .appName('InternalJoins-BroadcastHashJoin')
#                      .master('')
#                      .getOrCreate())

# df1 = spark.read.format('parquet').load('fligt_data_parquet')

# df_flight = df1.select('flight_id','aircraft_id','flight_cost','origin_airport','destination_airport',
#                        'departure_time','arrival_time','passenger_name','passenger_country',
#                        'travel_date','airplane_model','tail_no','distance',
#                        'turbulance','temp_at_dept','fuel_consumed_litre','taxi_duration_mins')

# df_passenger = df1.select('passenger_name','frequent_flier_no',
#                           'passenger_country').filter('passenger_country = "Spain"').limit(10)
# df_passenger.write.format('csv').save('test_csv')
#
# df_joined = df_flight.join(df_passenger, df_flight.passenger_name == df_passenger.passenger_name, 'left')\
#                      .drop(df_flight.passenger_name, df_flight.passenger_country)

# df_joined.show()


#############################################################################################################
# Broadcast Nested Loop Join
#############################################################################################################
# conf = SparkConf()
# conf.set('spark.sql.join.preferSortMergeJoin', 'false')
# conf.set('spark.sql.autoBroadcastJoinThreshold', '524288000')
# conf.set('spark.sql.shuffle.partitions', '20')

# spark = (SparkSession.builder.config(conf=conf)
#                      .appName('InternalJoins-BroadcastNestedLoopJoin')
#                      .master('')
#                      .getOrCreate())

# df1 = spark.read.format('parquet').load('fligt_data_parquet')

# df_flight = df1.select('flight_id','aircraft_id','flight_cost','origin_airport','destination_airport',
#                        'departure_time','passenger_name','passenger_country',
#                        'travel_date','airplane_model','tail_no','distance',
#                        'turbulance','temp_at_dept','fuel_consumed_litre','taxi_duration_mins')

# df_passenger = df1.select('passenger_name','frequent_flier_no', 'travel_date', 'arrival_time',
#                           'passenger_country').filter('passenger_country = "Spain"').limit(10)

# df_joined = df_flight.join(df_passenger, (df_flight.departure_time >= df_passenger.arrival_time), 'inner')\
#                      .drop(df_flight.passenger_name, df_flight.passenger_country, df_flight.travel_date)

# df_joined.show()


#############################################################################################################
# Shuffled Hash Join
#############################################################################################################
# conf = SparkConf()
# conf.set('spark.sql.join.preferSortMergeJoin', 'false')
# # conf.set('spark.sql.autoBroadcastJoinThreshold', '524288000')
# conf.set('spark.sql.shuffle.partitions', '20')
#
# spark = (SparkSession.builder.config(conf=conf)
#                      .appName('InternalJoins-ShuffledHashJoin_3')
#                      .master('')
#                      .getOrCreate())
#
# df1 = spark.read.format('parquet').load('fligt_data_parquet')
# df_flight = df1.select('flight_id','aircraft_id','flight_cost','origin_airport','destination_airport',
#                        'departure_time','passenger_name','passenger_country',
#                        'travel_date','airplane_model','tail_no','distance',
#                        'turbulance','temp_at_dept','fuel_consumed_litre','taxi_duration_mins')
#
# df_passenger = df1.select('passenger_name','frequent_flier_no', 'travel_date', 'arrival_time',
#                           'passenger_country')
#
# df_joined = df_flight.join(df_passenger,
#                            (df_flight.passenger_name == df_passenger.passenger_name) &
#                            (df_flight.departure_time < df_passenger.arrival_time),
#                            'right')\
#                      .drop(df_flight.passenger_name, df_flight.passenger_country, df_flight.travel_date)
#
# df_joined.show()

#############################################################################################################
# Shuffled Nested Loop Join
#############################################################################################################
# conf = SparkConf()
# conf.set('spark.sql.join.preferSortMergeJoin', 'true')
# conf.set('spark.sql.join.preferSortMergeJoin', 'false')
# conf.set('spark.sql.autoBroadcastJoinThreshold', '524288000')
# conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')
# conf.set('spark.driver.maxResultSize','4g')
# conf.set('spark.sql.shuffle.partitions', '10')

# spark = (SparkSession.builder.config(conf=conf)
#                      .appName('InternalJoins-ShuffledNestedLoopJoin')
#                      .master('spark://Mac.lan:7077')
#                      .getOrCreate())

# df1 = spark.read.format('parquet').load('fligt_data_parquet')

# df_flight = df1.select('flight_id','aircraft_id','flight_cost','origin_airport','destination_airport',
#                        'departure_time','passenger_name','passenger_country',
#                        'travel_date','airplane_model','tail_no','distance',
#                        'turbulance','temp_at_dept','fuel_consumed_litre','taxi_duration_mins')

# df_passenger = df1.select('passenger_name','frequent_flier_no', 'travel_date', 'arrival_time',
#                           'passenger_country')
#
# df_joined = df_flight.join(df_passenger,
#                            (df_flight.departure_time < df_passenger.arrival_time),
#                            'inner')\
#                      .drop(df_flight.passenger_name, df_flight.passenger_country, df_flight.travel_date)
#
# df_joined.show()


#############################################################################################################
# Spark Operators
#############################################################################################################
# from pyspark.sql.functions import count, dense_rank
#
# conf = SparkConf()
# spark = (SparkSession.builder.config(conf=conf)
#                      .appName('SparkOperators')
#                      .master('spark://Mac.lan:7077')
#                      .getOrCreate())
#
# df_flight = spark.read.format('parquet').load('fligt_data_parquet')
#
# df1 = df_flight.groupby('passenger_country').agg(count('passenger_country'))
# df1.show()
#
# df2 = df_flight.select('passenger_country').distinct().show()
#
# df3 = df_flight.select('flight_id','aircraft_id','flight_cost','origin_airport','destination_airport',
#                        'departure_time','passenger_name','passenger_country',
#                        'travel_date','airplane_model','tail_no','distance',
#                        'turbulance','temp_at_dept','fuel_consumed_litre','taxi_duration_mins')
#
# df4 = df_flight.select('passenger_name','frequent_flier_no', 'travel_date', 'arrival_time',
#                           'passenger_country').filter('passenger_country = "France"').limit(10)
#
# df_joined = df4.join(df3, (df3.passenger_name == df4.passenger_name),'inner')\
#                      .drop(df4.passenger_name, df4.passenger_country, df4.travel_date)
#
# df_joined.show(50)
#
# from pyspark.sql.functions import col
# from pyspark.sql import Window
#
# df5 = df_flight.select('passenger_name', 'passenger_country', 'travel_date')
#
# df6 = df5.withColumn('passenger_order',
#                      dense_rank().over(Window.partitionBy('passenger_country').orderBy(col('travel_date').asc())))\
#          .dropDuplicates(['passenger_country', 'travel_date', 'passenger_order'])
#
# df6.filter('passenger_order <=5').show()














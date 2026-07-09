from pyspark.sql import SparkSession
from pyspark import SparkConf

conf = SparkConf()
# conf.set("spark.sql.adaptive.enabled", "false")
conf.set("spark.sql.adaptive.enabled", "true")

#################################################################################################################
# AQE Coalesce Shuffle Partitions
#################################################################################################################

# conf.set('spark.sql.adaptive.coalescePartitions.enabled' , 'true')
# conf.set('spark.sql.adaptive.coalescePartitions.initialPartitionNum', '150')
# # conf.set('spark.sql.adaptive.advisoryPartitionSizeInBytes', '419430400')
#
# spark = (SparkSession.builder.config(conf=conf)
#                      .appName('AQECoalesceShufflePartitions_30').config(conf=conf)
#                      .master('spark://SoumyadeepDey.domain.name:7077')
#                      .getOrCreate())
#
# df_flight = spark.read.format('parquet').load('fligt_data_parquet')
#
# df1 = df_flight.select('flight_id','aircraft_id','flight_cost','origin_airport','destination_airport',
#                        'departure_time','passenger_name','passenger_country',
#                        'travel_date','airplane_model','tail_no','distance',
#                        'turbulance','temp_at_dept','fuel_consumed_litre','taxi_duration_mins')
#
# df2 = df_flight.select('passenger_name','frequent_flier_no', 'travel_date', 'arrival_time',
#                           'passenger_country')
#
# df_joined = df1.join(df2, (df1.passenger_name == df2.passenger_name) &
#                           (df1.passenger_country == df2.passenger_country),'inner')\
#                      .drop(df2.passenger_name, df2.passenger_country, df2.travel_date)
#
# df_joined.show()


#################################################################################################################
# AQE Skew Join Optimization
#################################################################################################################
# conf.set("spark.sql.adaptive.enabled", "false")
# conf.set("spark.sql.adaptive.enabled", "true")
#
# spark = (SparkSession.builder.config(conf=conf)
#                      .appName('AQESkewJoin_WITH')
#                      .master('spark://SoumyadeepDey.domain.name:7077')
#                      .getOrCreate())
#
# df_skewed_data = spark.read.format('parquet').load('skewed_data_parquet')
#
# df_flight = df_skewed_data.select('flight_id','aircraft_id','flight_cost','origin_airport','destination_airport',
#                        'departure_time','arrival_time','passenger_name', 'passenger_country',
#                        'travel_date','airplane_model','tail_no','distance',
#                        'turbulance','temp_at_dept','fuel_consumed_litre','taxi_duration_mins')
# df_passenger = df_skewed_data.select('passenger_name','frequent_flier','frequent_flier_no', 'travel_date',
#                           'passenger_country','passenger_dob','passenger_flight_class')
#
# df_passenger2 = df_skewed_data.select('passenger_name','frequent_flier','frequent_flier_no',
#                           'passenger_country','passenger_dob','passenger_flight_class')\
#                               .dropDuplicates(['passenger_country','passenger_name'])
#
# df_joined = df_flight.join(df_passenger2, (df_flight.passenger_country == df_passenger2.passenger_country),
#                            'inner').drop(df_passenger2.passenger_name , df_passenger2.passenger_country)
#
# df_joined.show()

#################################################################################################################
# Dynamic Partition Pruning
#################################################################################################################
# conf.set("spark.sql.adaptive.enabled", "true")
# conf.set('spark.sql.cbo.enabled', 'false')
# conf.set('spark.sql.cbo.optimizer.dynamicPartitionPruning', 'false')

# spark = (SparkSession.builder.config(conf=conf)
#                      .appName('DynamicPartitionPruning_false').config(conf=conf)
#                      .master('spark://SoumyadeepDey.domain.name:7077')
#                      .getOrCreate())

# df_flight_data = spark.read.format('parquet').load('fligt_data_parquet')
# df_flight_data_part = spark.read.format('parquet').load('flight_data_partitioned')

# df_flight_data.write.partitionBy('passenger_country').format('parquet').save('flight_data_partitioned')

# df_flight_data.where('passenger_country == "France"').show()
# df_flight_data_part.where('passenger_country == "France"').show()
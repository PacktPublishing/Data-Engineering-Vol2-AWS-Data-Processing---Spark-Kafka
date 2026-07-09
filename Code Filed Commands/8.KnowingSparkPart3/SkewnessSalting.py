from random import randint

from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StringType

conf = SparkConf()
conf.set("spark.sql.adaptive.enabled", "false")

#################################################################################################################
# Data Skewness
#################################################################################################################
# spark = (SparkSession.builder.config(conf=conf)
#                      .appName('DataSkewness')
#                      .master('spark://Mac.lan:7077')
#                      .getOrCreate())

#----------------------------------------------------------------------------------------------------------------
# Generate Skewed Data
#----------------------------------------------------------------------------------------------------------------
# df_flight = spark.read.format('parquet').load('fligt_data_parquet')
#
# df_USA = df_flight.where('passenger_country = "USA"')
# df_Spain = df_flight.where('passenger_country = "Spain"').limit(10000)
# df_France = df_flight.where('passenger_country = "France"').limit(1000000)
#
# df_USA.coalesce(1).write.mode('append').format('parquet').save('skewed_data_parquet')
# df_Spain.coalesce(1).write.mode('append').format('parquet').save('skewed_data_parquet')
# df_France.coalesce(1).write.mode('append').format('parquet').save('skewed_data_parquet')
#----------------------------------------------------------------------------------------------------------------

# df_skewed_data = spark.read.format('parquet').load('skewed_data_parquet')
#
# df_flight = df_skewed_data.select('flight_id','aircraft_id','flight_cost','origin_airport','destination_airport',
#                        'departure_time','arrival_time','passenger_name', 'passenger_country',
#                        'travel_date','airplane_model','tail_no','distance',
#                        'turbulance','temp_at_dept','fuel_consumed_litre','taxi_duration_mins')
# df_passenger = df_skewed_data.select('passenger_name','frequent_flier','frequent_flier_no', 'travel_date',
#                           'passenger_country','passenger_dob','passenger_flight_class')
#
# from pyspark.sql.functions import count
# df_skewed_data.groupBy('passenger_country').agg(count('flight_id').alias('flight_count')).show()
#
# df_passenger2 = df_skewed_data.select('passenger_name','frequent_flier','frequent_flier_no',
#                           'passenger_country','passenger_dob','passenger_flight_class')\
#                               .dropDuplicates(['passenger_country','passenger_name'])
#
# from pyspark.sql.functions import col
# # df_flight.groupby('passenger_country').count().orderBy(col('count').desc()).show()
#
# df_joined = df_flight.join(df_passenger2, (df_flight.passenger_country == df_passenger2.passenger_country),
#                            'inner').drop(df_passenger2.passenger_name , df_passenger2.passenger_country)
#
# df_joined.show()


#################################################################################################################
# Salting
#################################################################################################################
spark = (SparkSession.builder.config(conf=conf)
                     .appName('SparkSalting_FINAL')
                     .master('spark://Mac.lan:7077')
                     .getOrCreate())

df_skewed_data = spark.read.format('parquet').load('skewed_data_parquet')

df_flight = df_skewed_data.select('flight_id','aircraft_id','flight_cost','origin_airport','destination_airport',
                       'departure_time','arrival_time','passenger_name', 'passenger_country',
                       'travel_date','airplane_model','tail_no','distance',
                       'turbulance','temp_at_dept','fuel_consumed_litre','taxi_duration_mins')

df_passenger = df_skewed_data.select('passenger_name','frequent_flier','frequent_flier_no', 'travel_date',
                          'passenger_country','passenger_dob','passenger_flight_class')\
                             .dropDuplicates(['passenger_country','passenger_name'])

# No. of partitions = 30, each containing 500,000 rows
from pyspark.sql.functions import when, rand, concat, lit, sequence, explode

df_flight_salted = df_flight.withColumn('new_country_code_flight',
                    when(df_flight.passenger_country == "USA", concat(df_flight.passenger_country, lit('#'), (rand()*30).cast('int')))\
                    .when(df_flight.passenger_country == "France", concat(df_flight.passenger_country, lit('#'), (rand()*30).cast('int')))\
                    .otherwise(df_flight.passenger_country)
                    )

df_passenger_1 = df_passenger.withColumn('array_col', sequence(lit(1),lit(30)))
df_passenger_2 = df_passenger_1.withColumn('country_code', explode('array_col')).drop('array_col')
df_passenger_salted = df_passenger_2.withColumn('new_country_code_passenger',
                                           concat(df_passenger_2.passenger_country, lit('#'),df_passenger_2.country_code))\
                                .drop('country_code')

df_joined = df_flight_salted.join(df_passenger_salted,
                                  (df_passenger_salted.new_country_code_passenger == df_flight_salted.new_country_code_flight)
                                  & (df_passenger_salted.passenger_name == df_flight_salted.passenger_name),
                                  'inner')\
                            .drop(df_passenger_salted.passenger_country,
                                  df_passenger_salted.travel_date,
                                  'new_country_code_flight', 'new_country_code_passenger')

df_joined.show()















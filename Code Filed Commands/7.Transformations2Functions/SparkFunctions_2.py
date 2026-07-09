from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName('SparkWindowFunctions_Exercise1_FINAL')
        # .master("spark://SoumyadeepDey.domain.name:7077")
        .master("spark://Mac.lan:7077")
         .getOrCreate())

df1 = spark.read.format('csv').option('header','true').option('inferSchema','true')\
    .load('flight_data_csv/flight_data_1.csv')

###################################################################
# lit, expr, concat
###################################################################
from pyspark.sql.functions import lit, concat, expr, dense_rank

# var1 = "checking_history"
# flight_class = "economy"
# df1.select('travel_date', 'origin_airport','destination_airport','passenger_country', 'passenger_flight_class',
#             'departure_time','arrival_time', lit(var1))\
#     .withColumn('dep_arr_time', concat('departure_time',lit('#'),'arrival_time'))\
#     .filter(df1.passenger_flight_class == lit(flight_class)).show(5, truncate=False)

# df1.select('origin_airport','destination_airport','airplane_model',
#            expr("CASE "
#                 "WHEN turbulance > 6 then 'bad weather' "
#                 "WHEN turbulance > 3 and turbulance < 6 then 'moderate weather'"
#                 "ELSE 'good weather' "
#                 "END as weather_cond")).show(5)


###################################################################
# DATE functions
####################################################################
# from pyspark.sql.functions import col, date_format, extract, date_add, to_timestamp, year, unix_timestamp
# from pyspark.sql.types import TimestampType
#
# df2 = df1.select('travel_date', col('departure_time').cast('string'), col('arrival_time').cast('string'))
# df3 = df2.select('travel_date', col('departure_time').cast(TimestampType()), col('arrival_time').cast(TimestampType()))
# df4 = df3.select('travel_date', date_format('departure_time',"HH:mm:ss").alias('departure_time'),
#                                 date_format('arrival_time', "HH:mm:ss").alias('arrival_time'))

# df5 = df4.select('travel_date',
#                  extract(lit('year'), 'travel_date').alias('travel_year'),
#                  # year('travel_date').alias('year_func_output'))
#                  date_add('travel_date', 10).alias('travel_date_plus_10'),
#                  date_format('travel_date', 'dd/MM/yyyy').alias('formatted_travel_date'),
#                  to_timestamp('travel_date', 'yyyy-MM-dd').alias('formatted_travel_date_ts'))

# df6 = df4.select('travel_date', unix_timestamp('travel_date'))
# df6.show(5)


###################################################################
# when+otherwise
###################################################################
# from pyspark.sql.functions import when
#
# df2 = df1.select('airplane_model', 'turbulance', 'engine_performance', 'taxi_duration_mins')
# df3 = df2.withColumn('turbulance_indicator', when((df2.turbulance > 6) , "RED FLAG")\
#                                                     .when((df2.turbulance > 3) & (df2.turbulance <= 6), "ORANGE FLAG")\
#                                                     .when((df2.turbulance > 0) & (df2.turbulance <= 3), "YELLOW FLAG")\
#                                                     .otherwise("BLUE FLAG"))
# df3.show()


###################################################################
# WINDOW functions - IMPORTANT!!
###################################################################
from pyspark.sql.functions import col
from pyspark.sql import Window

# df2 = df1.select('passenger_name', 'passenger_country', 'travel_date')
# df3 = df2.orderBy(col('travel_date').asc()).dropDuplicates(['travel_date'])
# df3 = df2.withColumn('passenger_order',
#                      dense_rank().over(Window.partitionBy('passenger_country').orderBy(col('travel_date').asc())))\
#          .dropDuplicates(['passenger_country', 'travel_date', 'passenger_order'])
#
# df3.filter('passenger_order <=5').show()

#--------------------------------------------------------------------
# Identify the top 10 revenue-generating frequent fliers per country
#--------------------------------------------------------------------
# from pyspark.sql.functions import sum, col
#
# df2 = df1.select('passenger_name', 'passenger_country', 'flight_cost').where('frequent_flier = True')
# df3 = df2.groupby('passenger_name','passenger_country').agg(sum('flight_cost').alias('total_cost'))
# df4 = df3.withColumn('passenger_rank',
#                      dense_rank().over(Window.partitionBy('passenger_country')\
#                                        .orderBy(col('total_cost').desc())))
# df5 = df4.dropDuplicates(['passenger_rank','passenger_country']).where('passenger_rank <= 5')
# df6 = df5.orderBy('passenger_country')
# df6.show(30)







from pyspark.sql import SparkSession

#####################################################################################
# Convert CSV files to a single PARQUET file
#####################################################################################
# spark = SparkSession.builder.appName('InputPartitionsMaxPartitionBytes').getOrCreate()
# flight_df_csv = spark.read.format('csv')\
#                 .option('header','true')\
#                 .option('inferSchema', 'true')\
#                 .load('flight_data_csv')
#
# flight_df_csv.coalesce(1).write.format('parquet').save('flight_data_parquet')


#####################################################################################
# Input Partitions : Experiment with spark.sql.files.maxPartitionBytes
#####################################################################################
# spark.conf.set('spark.sql.files.maxPartitionBytes', '536870912')
# print(spark.conf.get('spark.sql.files.maxPartitionBytes'))
#
# df_flight = spark.read.format('csv')\
#                 .option('header','true')\
#                 .option('inferSchema', 'true')\
#                 .load('flight_data_csv')

# df_flight = spark.read.format('parquet')\
#                 .load('flight_data_parquet')

# print('Input 1 Partitions: ', df_flight.rdd.getNumPartitions())
#
# df1 = df_flight.select('departure_time','arrival_time','flight_id','aircraft_id','itinerary_no')\
#     .filter('passenger_country = "India"')
#
# print('Input 2 Partitions: ', df1.rdd.getNumPartitions())
# df1.write.format('parquet').save('test_file')


#####################################################################################
# Output Partitions :
# repartition, coalesce, partitionBy
#####################################################################################
# spark = SparkSession.builder.appName('OutputPartitionsRepartition').getOrCreate()
# df_flight = spark.read.format('csv')\
#                 .option('header','true')\
#                 .option('inferSchema', 'true')\
#                 .load('flight_data_csv')
#
# print('df_flight partitions :', df_flight.rdd.getNumPartitions())

# df_flight.repartition(100).write.format('csv').save('test_file_repartition')
# df_flight.repartition('passenger_country').write.format('csv').save('test_file_repartition')
# df_flight.repartition(10, 'passenger_country').write.format('csv').save('test_file_repartition')
# df_flight.coalesce(10).write.format('csv').save('test_file_coalesce')
# print(df_flight.select('passenger_country').distinct().count())
# df_flight.write.partitionBy('passenger_country').format('csv').save('test_file_partitionBy')


#####################################################################################
# Transformations that change partitions
#####################################################################################
# spark = SparkSession.builder.appName('ChangePartitionsSubtract').getOrCreate()
#
# flight_df = spark.read.format('parquet').load('flight_data_parquet')
# print('flight_df partitions :', flight_df.rdd.getNumPartitions())

# df1 = flight_df.select('flight_id','aircraft_id','origin_airport','destination_airport','engine_performance')
# print('df1 partitions :', df1.rdd.getNumPartitions())
# df1.show(5)

# df2 = flight_df.filter('aircraft_id = "AI161"')
# print('df2 partitions :', df2.rdd.getNumPartitions())

# df_csv_1 = spark.read.format('csv')\
#             .option('header', 'true')\
#             .option('inferSchema', 'true')\
#             .load('flight_data_csv/flight_data_schema_2.csv')
# print('df_csv_1 partitions :', df_csv_1.rdd.getNumPartitions())
#
# df_csv_2 = spark.read.format('csv')\
#             .option('header', 'true')\
#             .option('inferSchema', 'true')\
#             .load('flight_data_csv/flight_data_schema_6.csv')
# print('df_csv_2 partitions :', df_csv_2.rdd.getNumPartitions())

# df_final_union = df_csv_1.union(df_csv_2)
# print(df_final_union.rdd.getNumPartitions())

# df_final_subtract = df_csv_1.subtract(df_csv_2)
# print('df_final_subtract partitions :', df_final_subtract.rdd.getNumPartitions())
# df_final_subtract.show(5)

# df_final_intersect = df_csv_1.intersect(df_csv_2)
# print('df_final_intersect partitions :', df_final_intersect.rdd.getNumPartitions())
#
# df_final_sort = df_csv_1.sort('travel_date')
# print('df_final_sort partitions :', df_final_sort.rdd.getNumPartitions())


#####################################################################################
# Output Write Modes
#####################################################################################
# spark = SparkSession.builder.appName('WriteOutputMode').getOrCreate()
#
# df_csv_1 = spark.read.format('csv')\
#             .option('header', 'true')\
#             .option('inferSchema', 'true')\
#             .load('flight_data_csv/flight_data_schema_2.csv')
#
# df_csv_1.filter('aircraft_id = "AI161"')\
#     .write.partitionBy('passenger_country')\
#     .mode('Overwrite').format('parquet').save('test_file_parquet')
from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName('SparkWithRDBMSMySQL_Read').getOrCreate()

# df1 = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true')\
#     .load('s3://3aayaam-data-engg-vol-2/3aayaam_Spark/flight_data/flight_data_1.csv')

# jdbc_url = 'jdbc:mysql://<aurora-rds-endpoint>:3306/flight_db'

############################################################################################
# Write to RDBMS (MySQL) Target
############################################################################################

# df1.write.format('jdbc')\
#     .option("url", jdbc_url)\
#     .option("dbtable", "flight_data")\
#     .option("user", "admin")\
#     .option("password", "xxxx")\
#     .option("driver", "com.mysql.cj.jdbc.Driver")\
#     .option("numPartitions", "4")\
#     .option("batchSize", "10000")\
#     .mode("append")\
#     .save()

############################################################################################
# Read from RDBMS (MySQL) Source
############################################################################################

# df2 = spark.read.format('jdbc')\
#     .option("url", jdbc_url)\
#     .option("user", "admin")\
#     .option("password", "xxxx")\
#     .option("driver", "com.mysql.cj.jdbc.Driver")\
#     .option("dbtable", "(select passenger_name, itinerary_no, flight_id, departure_time, arrival_time, origin_airport, "
#                      "destination_airport from flight_data where passenger_country ='India') as t1")\
#     .option("partitionColumn", "itinerary_no")\
#     .option("numPartitions", "10")\
#     .option('lowerBound', '10002234')\
#     .option('upperBound', '200011000')\
#     .option("fetchSize", "10000")\
#     .option("pushDownPredicate", "false")\
#     .load()
#
# df2.write.mode('overwrite').format('parquet').save('s3://<bucket>/3aayaam_Spark/output/rdbms_read_output.parquet')
# spark.stop()


############################################################################################
# Read from Data Warehouse Source & Write to Data Warehouse Target - Redshift
############################################################################################

# spark = SparkSession.builder.appName('SparkWithRedshift').getOrCreate()
#
# jdbc_url = 'jdbc:redshift://<redshift-endpoint>:<port>/flight_db?user=admin&password=xxxx'
# iam_role = 'arn:aws:iam::125262686877:role/data-engg-on-aws-redshift-s3fullaccess-role'
#
# df1 = spark.read.format('io.github.spark_redshift_community.spark.redshift')\
#                 .option("url", jdbc_url)\
#                 .option("dbtable", "flight_data")\
#                 .option('aws_iam_role', iam_role)\
#                 .option('tempdir', 's3://3aayaam-data-engg-vol-2/3aayaam_Spark/output/temp/')\
#                 .load()
#
# from pyspark.sql.functions import col, extract, lit, sum
# df2 = df1.groupby(col('airplane_model').alias('airplane_company'),col('passenger_country').alias('country'),
#                   'origin_airport',extract(lit('YEAR'),'travel_date').alias('reporting_year'))\
#          .agg(sum('flight_cost').alias('total_revenue'))
#
# df2.write.format('io.github.spark_redshift_community.spark.redshift') \
#         .option("url", jdbc_url) \
#         .option("dbtable", "country_revenue") \
#         .option('aws_iam_role', iam_role)\
#         .option('tempdir', 's3://3aayaam-data-engg-vol-2/3aayaam_Spark/output/temp/')\
#         .mode('append')\
#         .save()
#
# spark.stop()

############################################################################################
# Parquet optimization features
############################################################################################

spark = SparkSession.builder.appName('ParquetFeatures').getOrCreate()
spark.conf.set('spark.sql.parquet.filterPushdown','true')
spark.conf.set('spark.sql.parquet.recordLevelFilter.enabled', 'true')
spark.conf.set('spark.sql.parquet.aggregatePushdown', 'true')

df1 = spark.read.format('parquet').load('flight_data_1.parquet')

# df1.filter('passenger_country = "USA"').explain()
df1.select('passenger_country','temp_at_dept').groupby('passenger_country').min().explain()















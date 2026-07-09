# from pyspark.sql import SparkSession
#
# spark = (SparkSession.builder.master('spark://SoumyadeepDey.domain.name:7077')
#          .config('spark.sql.warehouse.dir','/Users/soumyadeepdey/DeepHDD/3aayaamFolder/3aayaamDataEnggV2/spark_sql_warehouse')
#          .enableHiveSupport()
#          .appName('DataframePermanentTable').getOrCreate())
         # .appName('ReadDataFrameTable').getOrCreate())

# df1 = (spark.read.format('csv')
#        .option('header','true')
#        .option('inferSchema', 'true')
#        .load('flight_data_1.csv'))

# df1.createOrReplaceTempView('temp_flight_data')
# spark.sql('select * from temp_flight_data where passenger_country = "Spain"').show()


# (df1.write.format('parquet')
#  .option('path','/Users/soumyadeepdey/DeepHDD/3aayaamFolder/3aayaamDataEnggV2/spark_sql_warehouse/flight_data_table')
#  .saveAsTable('flight_data_table'))

# spark.sql('select * from flight_data_table').show()
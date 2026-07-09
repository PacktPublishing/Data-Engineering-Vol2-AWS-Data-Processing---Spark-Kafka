from pyspark.sql import SparkSession
from pyspark import StorageLevel
from pyspark.sql.functions import col, avg, sum, max, min

spark = SparkSession.builder.appName('Project3.1_FlightEfficiency')\
         .master("spark://SoumyadeepDey.domain.name:7077")\
         .getOrCreate()

flight_data_df = spark.read.format('csv')\
                 .option('inferSchema', 'true')\
                 .option('header', 'true')\
                 .load('flight_data_csv')

df1 = flight_data_df.select('airplane_model','distance','fuel_consumed_litre',
                            'avg_flight_speed_kmps','turbulance','engine_performance' )

df2 = df1.groupby('airplane_model').agg(avg('fuel_consumed_litre').alias('avg_fuel_consumption'),
                                        avg('avg_flight_speed_kmps').alias('avg_speed'),
                                        avg('engine_performance').alias('avg_engine_performance'))

df3 = df1.select('airplane_model', 'distance', 'turbulance', 'fuel_consumed_litre','engine_performance')
# df4 = df3.groupby('airplane_model', 'distance').agg(avg('fuel_consumed_litre').alias('avg_dist_fuel_consumption'))
#
# df41 = df2.join(df4, df4.airplane_model == df2.airplane_model, 'inner')\
#        .drop(df2.airplane_model, df2.avg_speed, df2.avg_engine_performance)
#
# df42 = df41.selectExpr('airplane_model', 'distance', 'avg_fuel_consumption' ,
#                        'avg_dist_fuel_consumption as fuel_consumption_distance_wise',
#                        'case when fuel_consumption_distance_wise > avg_fuel_consumption then "YES" else "NO" end as impact_of_distance')
# df42.show(100, truncate=False)

df5 = df3.groupby('airplane_model', 'turbulance')\
    .agg(avg('engine_performance').alias('avg_turb_engine_performance'))

df51 = df2.join(df5, df5.airplane_model == df2.airplane_model, 'inner').distinct()\
       .drop(df2.airplane_model, df2.avg_speed, df2.avg_fuel_consumption)

df52 = df51.selectExpr('airplane_model', 'turbulance', 'avg_engine_performance as engine_perf_wo_turbulence',
                       'avg_turb_engine_performance as engine_perf_w_turbulence',
                       'case when avg_engine_performance < avg_turb_engine_performance then "NO" '
                       'else "YES" end as impact_of_turbulence')

df52.show(100)
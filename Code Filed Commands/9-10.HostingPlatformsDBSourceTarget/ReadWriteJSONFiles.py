from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('ReadWriteJSONFiles').getOrCreate()

df1 = spark.read.format('json').load('flight_data_2.json')

'''
root
 |-- aircraft_ids: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- itinerary: array (nullable = true)
 |    |-- element: long (containsNull = true)
 |-- passenger_detail: struct (nullable = true)
 |    |-- address: string (nullable = true)
 |    |-- country: string (nullable = true)
 |-- passenger_id: long (nullable = true)
 |-- tickets: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- travel_detail: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- arr_time: string (nullable = true)
 |    |    |-- dep_time: string (nullable = true)
 |    |    |-- flight_id: string (nullable = true)
'''
from pyspark.sql.functions import explode,col

df2 = df1.select(explode('aircraft_ids').alias('aircraft_id'), explode('itinerary').alias('itinerary_id'),
                 col('passenger_detail.address').alias('address'), col('passenger_detail.country').alias('country'),
                 'passenger_id',
                 explode('tickets').alias('ticket_id'),
                 explode('travel_detail').alias('travel_detail_json')
                )
df3 = df2.select('aircraft_id', 'itinerary_id', 'address', 'country', 'passenger_id', 'ticket_id',
                 col('travel_detail_json.arr_time').alias('arrival_time'),
                 col('travel_detail_json.dep_time').alias('departure_time'),
                 col('travel_detail_json.flight_id').alias('flight_id')
                 ).dropDuplicates()

df3.write.format('parquet').save('s3://3aayaam-data-engg-vol-2/3aayaam_Spark/output/EMR_step-3_json_to_parquet_flattened.parquet')
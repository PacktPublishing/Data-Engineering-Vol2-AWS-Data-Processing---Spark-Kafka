from pyspark.sql import SparkSession

spark = (SparkSession.builder.master('')
         .config('')
         .enableHiveSupport()
         .appName('SparkIcebergOperation').getOrCreate())

#########################################################################################
### Configuration for Jupyter Notebook
## %%configure -f
## {
## "conf":{
##      "spark.sql.catalog.my_catalog" : "org.apache.iceberg.spark.SparkCatalog",
##      "spark.sql.catalog.my_catalog.type" : "glue",
##      "spark.sql.catalog.my_catalog.warehouse" : "s3://<bucket>/<folder>/",
##      "spark.sql.defaultCatalog" : "my_catalog",
##      "spark.sql.extensions" : "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
##     }
## }
#########################################################################################


spark.sql(
    'create table 3aayaam_iceberg_db.flight_data ('
    'departure_time timestamp,'
    'arrival_time timestamp,'
    'flight_id varchar(100),'
    'aircraft_id varchar(100),'
    'itinerary_no int,'
    'ticket_no varchar(100),'
    'flight_cost decimal(10,2),'
    'origin_airport varchar(100),'
    'destination_airport varchar(100),'
    'frequent_flier varchar(10),'
    'travel_date date,'
    'airplane_model varchar(100),'
    'frequent_flier_no varchar(100),'
    'passenger_name varchar(100),'
    'passenger_country varchar(100),'
    'tail_no varchar(100),'
    'distance int,'
    'turbulance int,'
    'temp_at_dept decimal(6,2),'
    'fuel_consumed_litre decimal(10,2),'
    'taxi_duration_mins decimal(6,2),'
    'avg_flight_speed_kmps decimal(10,2),'
    'engine_performance int,'
    'passenger_dob date,'
    'passenger_flight_class varchar(100)'
    ') using iceberg '
    'location "s3://<bucket>/<folder>" '
    'tblproperties ("write.delete.format.default"="orc","write.delete.mode"="copy-on-write", "write.update.mode"="merge-on-read")'
)

df1 = spark.read.format('csv').option('header', 'True').option('inferSchema','true').load('s3://<bucket>/<folder>/flight_data_1.csv')

df1.writeTo('3aayaam_iceberg_db.flight_data').append()

spark.sql('select * from 3aayaam_iceberg_db.flight_data').show(50)

spark.sql('delete from 3aayaam_iceberg_db.flight_data where ticket_no= "VG-77479-ZD-913"')
spark.sql('delete from 3aayaam_iceberg_db.flight_data where ticket_no= "CM-52082-HH-280"')
spark.sql('delete from 3aayaam_iceberg_db.flight_data where ticket_no= "YO-16886-KB-614"')
spark.sql('delete from 3aayaam_iceberg_db.flight_data where ticket_no= "YV-50923-WW-751"')
spark.sql('delete from 3aayaam_iceberg_db.flight_data where ticket_no= "LK-34004-QA-395"')

spark.sql('update 3aayaam_iceberg_db.flight_data set aircraft_id = "3AAYAAMIND" where itinerary_no= 152976430')
spark.sql('update 3aayaam_iceberg_db.flight_data set aircraft_id = "3AAYAAMIND" where itinerary_no= 150418396')
spark.sql('update 3aayaam_iceberg_db.flight_data set aircraft_id = "3AAYAAMIND" where itinerary_no= 159769531')
spark.sql('update 3aayaam_iceberg_db.flight_data set aircraft_id = "3AAYAAMIND" where itinerary_no= 151052883')
spark.sql('update 3aayaam_iceberg_db.flight_data set aircraft_id = "3AAYAAMIND" where itinerary_no= 153727132')
spark.sql('update 3aayaam_iceberg_db.flight_data set aircraft_id = "3AAYAAMIND" where itinerary_no= 150318187')
spark.sql('update 3aayaam_iceberg_db.flight_data set aircraft_id = "3AAYAAMIND" where itinerary_no= 156092896')
spark.sql('update 3aayaam_iceberg_db.flight_data set aircraft_id = "3AAYAAMIND" where itinerary_no= 157271554')
spark.sql('update 3aayaam_iceberg_db.flight_data set aircraft_id = "3AAYAAMIND" where itinerary_no= 156034494')
spark.sql('update 3aayaam_iceberg_db.flight_data set aircraft_id = "3AAYAAMIND" where itinerary_no= 156875330')
spark.sql('update 3aayaam_iceberg_db.flight_data set aircraft_id = "3AAYAAMIND" where itinerary_no= 159439013')
spark.sql('update 3aayaam_iceberg_db.flight_data set aircraft_id = "3AAYAAMIND" where itinerary_no= 154901928')

spark.sql('call my_catalog.system.rewrite_data_files(table => "3aayaam_iceberg_db.flight_data", strategy => "binpack", options => map("min-file-size-bytes", "10000000", "remove-dangling-deletes", "true"))').show()

spark.sql('call my_catalog.system.rewrite_manifests("3aayaam_iceberg_db.flight_data")').show()

spark.sql('call my_catalog.system.remove_orphan_files("3aayaam_iceberg_db.flight_data")').show()

spark.sql('call my_catalog.system.expire_snapshots("3aayaam_iceberg_db.flight_data", TIMESTAMP "2025-08-03 11:40:00.000", 1)').show()


%%configure -f
{
"conf":{
     "spark.sql.catalog.my_catalog" : "org.apache.iceberg.spark.SparkCatalog",
     "spark.sql.catalog.my_catalog.type" : "glue",
     "spark.sql.catalog.my_catalog.warehouse" : "s3://3aayaam-data-engg-vol-2/3aayaam_spark_iceberg/iceberg_catalog_warehouse/",
     "spark.sql.defaultCatalog" : "my_catalog",
     "spark.sql.extensions" : "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
     "spark.driver.cores" : "2",
     "spark.driver.memory" : "4g",
     "spark.executor.memory" : "2g",
     "spark.executor.cores" : "2",
     "spark.num.executors" : "2"
}
}

spark.sql(
    'create table 3aayaam_iceberg_db.flight_data_partition_by_country ('
    'departure_time timestamp,'
    'arrival_time timestamp,'
    'flight_id varchar(100),'
    'aircraft_id varchar(100),'
    'itinerary_no int,'
    'ticket_no varchar(100),'
    'flight_cost decimal(10,2),'
    'origin_airport varchar(100),'
    'destination_airport varchar(100),'
    'frequent_flier varchar(10),'
    'travel_date date,'
    'airplane_model varchar(100),'
    'frequent_flier_no varchar(100),'
    'passenger_name varchar(100),'
    'passenger_country varchar(100),'
    'tail_no varchar(100),'
    'distance int,'
    'turbulance int,'
    'temp_at_dept decimal(6,2),'
    'fuel_consumed_litre decimal(10,2),'
    'taxi_duration_mins decimal(6,2),'
    'avg_flight_speed_kmps decimal(10,2),'
    'engine_performance int,'
    'passenger_dob date,'
    'passenger_flight_class varchar(100)'
    ') using iceberg '
    'partitioned by (identity(passenger_country)) '
    'location "s3://<bucket>/<folder>" '
    'tblproperties ("write.delete.format.default"="orc","write.delete.mode"="copy-on-write", "write.update.mode"="merge-on-read")'
)


spark.sql(
    'create table 3aayaam_iceberg_db.flight_data_partition_by_travel_date ('
    'departure_time timestamp,'
    'arrival_time timestamp,'
    'flight_id varchar(100),'
    'aircraft_id varchar(100),'
    'itinerary_no int,'
    'ticket_no varchar(100),'
    'flight_cost decimal(10,2),'
    'origin_airport varchar(100),'
    'destination_airport varchar(100),'
    'frequent_flier varchar(10),'
    'travel_date date,'
    'airplane_model varchar(100),'
    'frequent_flier_no varchar(100),'
    'passenger_name varchar(100),'
    'passenger_country varchar(100),'
    'tail_no varchar(100),'
    'distance int,'
    'turbulance int,'
    'temp_at_dept decimal(6,2),'
    'fuel_consumed_litre decimal(10,2),'
    'taxi_duration_mins decimal(6,2),'
    'avg_flight_speed_kmps decimal(10,2),'
    'engine_performance int,'
    'passenger_dob date,'
    'passenger_flight_class varchar(100)'
    ') using iceberg '
    'partitioned by (month(travel_date)) '
    'location "s3://<bucket>/<folder>" '
    'tblproperties ("write.delete.format.default"="orc","write.delete.mode"="copy-on-write", "write.update.mode"="merge-on-read")'
)

spark.sql(
    'create table 3aayaam_iceberg_db.flight_data_partition_by_flight_id ('
    'departure_time timestamp,'
    'arrival_time timestamp,'
    'flight_id varchar(100),'
    'aircraft_id varchar(100),'
    'itinerary_no int,'
    'ticket_no varchar(100),'
    'flight_cost decimal(10,2),'
    'origin_airport varchar(100),'
    'destination_airport varchar(100),'
    'frequent_flier varchar(10),'
    'travel_date date,'
    'airplane_model varchar(100),'
    'frequent_flier_no varchar(100),'
    'passenger_name varchar(100),'
    'passenger_country varchar(100),'
    'tail_no varchar(100),'
    'distance int,'
    'turbulance int,'
    'temp_at_dept decimal(6,2),'
    'fuel_consumed_litre decimal(10,2),'
    'taxi_duration_mins decimal(6,2),'
    'avg_flight_speed_kmps decimal(10,2),'
    'engine_performance int,'
    'passenger_dob date,'
    'passenger_flight_class varchar(100)'
    ') using iceberg '
    'partitioned by (bucket(4, flight_id)) '
    'location "s3://<bucket>/<folder>" '
    'tblproperties ("write.delete.format.default"="orc","write.delete.mode"="copy-on-write", "write.update.mode"="merge-on-read")'
)
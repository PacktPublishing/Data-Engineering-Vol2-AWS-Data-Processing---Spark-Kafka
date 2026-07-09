# from pyspark.sql import SparkSession
#
# spark = (SparkSession.builder.master('spark://SoumyadeepDey.domain.name:7077')
#          .config('spark.sql.warehouse.dir','/Users/soumyadeepdey/DeepHDD/3aayaamFolder/3aayaamDataEnggV2/spark_sql_warehouse')
#          .enableHiveSupport()
#          .appName('SparkSQLWindowFunction').getOrCreate())

# spark.sql('create database spark_db')
# spark.sql('create database 3aayaam_db_1 '
#           'location "/Users/soumyadeepdey/DeepHDD/3aayaamFolder/3aayaamDataEnggV2/spark_sql_warehouse/3aayaam_db_1"')
# spark.sql('create database 3aayaam_db_2 '
#           'location "/Users/soumyadeepdey/DeepHDD/3aayaamFolder/3aayaamDataEnggV2/3aayaam_spark_dbs/3aayaam_db_2"')

# spark.sql('describe database extended spark_db').show(truncate=False)
# spark.sql('describe database extended 3aayaam_db_1').show(truncate=False)
# spark.sql('describe database extended 3aayaam_db_2').show(truncate=False)

# spark.sql('drop database spark_db')
# spark.sql('drop database 3aayaam_db_2')

# spark.sql(
#     'create table 3aayaam_db_1.flight_data ('
#     'departure_time varchar(100),'
#     'arrival_time varchar(100),'
#     'flight_id varchar(100),'
#     'aircraft_id varchar(100),'
#     'itinerary_no varchar(100),'
#     'ticket_no varchar(100),'
#     'flight_cost varchar(100),'
#     'origin_airport varchar(100),'
#     'destination_airport varchar(100),'
#     'frequent_flier varchar(100),'
#     'travel_date varchar(100),'
#     'airplane_model varchar(100),'
#     'frequent_flier_no varchar(100),'
#     'passenger_name varchar(100),'
#     'passenger_country varchar(100),'
#     'tail_no varchar(100),'
#     'distance varchar(100),'
#     'turbulance varchar(100),'
#     'temp_at_dept varchar(100),'
#     'fuel_consumed_litre varchar(100),'
#     'taxi_duration_mins varchar(100),'
#     'avg_flight_speed_kmps varchar(100),'
#     'engine_performance varchar(100),'
#     'passenger_dob varchar(100),'
#     'passenger_flight_class varchar(100)'
#     ') using csv'
# )

# spark.sql('LOAD DATA local inpath "/Users/soumyadeepdey/DeepHDD/3aayaamFolder/3aayaamDataEnggV2/13.SparkSQL/flight_data_1.csv" '
#           'into table 3aayaam_db_1.flight_data')

# spark.sql('select * from 3aayaam_db_1.flight_data').show()

# spark.sql(
#     'create table 3aayaam_db_1.flight_data_2 ('
#     'departure_time varchar(100),'
#     'arrival_time varchar(100),'
#     'flight_id varchar(100),'
#     'aircraft_id varchar(100),'
#     'itinerary_no varchar(100),'
#     'ticket_no varchar(100),'
#     'flight_cost varchar(100),'
#     'origin_airport varchar(100),'
#     'destination_airport varchar(100),'
#     'frequent_flier varchar(100),'
#     'travel_date varchar(100),'
#     'airplane_model varchar(100),'
#     'frequent_flier_no varchar(100),'
#     'passenger_name varchar(100),'
#     'passenger_country varchar(100),'
#     'tail_no varchar(100),'
#     'distance varchar(100),'
#     'turbulance varchar(100),'
#     'temp_at_dept varchar(100),'
#     'fuel_consumed_litre varchar(100),'
#     'taxi_duration_mins varchar(100),'
#     'avg_flight_speed_kmps varchar(100),'
#     'engine_performance varchar(100),'
#     'passenger_dob varchar(100),'
#     'passenger_flight_class varchar(100)'
#     ') using csv '
#     'location "/Users/soumyadeepdey/DeepHDD/3aayaamFolder/3aayaamDataEnggV2/13.SparkSQL/flight_data_1.csv"'
# )

# spark.sql('select passenger_country, flight_cost from 3aayaam_db_1.flight_data_2 limit 5').show()

# spark.sql('drop table 3aayaam_db_1.flight_data')

# spark.sql('show databases').show()
# spark.sql('use 3aayaam_db_1')
# spark.sql('show tables').show()

# spark.sql('drop table 3aayaam_db_1.flight_data_csv')

# spark.sql(
#     'create table 3aayaam_db_1.flight_data_csv ('
#     'departure_time timestamp,'
#     'arrival_time timestamp,'
#     'flight_id varchar(100),'
#     'aircraft_id varchar(100),'
#     'itinerary_no int,'
#     'ticket_no varchar(100),'
#     'flight_cost decimal(10,2),'
#     'origin_airport varchar(100),'
#     'destination_airport varchar(100),'
#     'frequent_flier varchar(10),'
#     'travel_date date,'
#     'airplane_model varchar(100),'
#     'frequent_flier_no varchar(100),'
#     'passenger_name varchar(100),'
#     'passenger_country varchar(100),'
#     'tail_no varchar(100),'
#     'distance int,'
#     'turbulance int,'
#     'temp_at_dept decimal(6,2),'
#     'fuel_consumed_litre decimal(10,2),'
#     'taxi_duration_mins decimal(6,2),'
#     'avg_flight_speed_kmps decimal(10,2),'
#     'engine_performance int,'
#     'passenger_dob date,'
#     'passenger_flight_class varchar(100)'
#     ') using csv '
#     'location "/Users/soumyadeepdey/DeepHDD/3aayaamFolder/3aayaamDataEnggV2/13.SparkSQL/flight_data_1.csv"'
#     'options ("header" = "true")'
# )
#
# spark.sql('select * from 3aayaam_db_1.flight_data_csv limit 5').show()

# spark.sql(
#     'create table 3aayaam_db_1.flight_data_orc ('
#     'departure_time timestamp,'
#     'arrival_time timestamp,'
#     'flight_id varchar(100),'
#     'aircraft_id varchar(100),'
#     'itinerary_no int,'
#     'ticket_no varchar(100),'
#     'flight_cost decimal(10,2),'
#     'origin_airport varchar(100),'
#     'destination_airport varchar(100),'
#     'frequent_flier varchar(10),'
#     'travel_date date,'
#     'airplane_model varchar(100),'
#     'frequent_flier_no varchar(100),'
#     'passenger_name varchar(100),'
#     'passenger_country varchar(100),'
#     'tail_no varchar(100),'
#     'distance int,'
#     'turbulance int,'
#     'temp_at_dept decimal(6,2),'
#     'fuel_consumed_litre decimal(10,2),'
#     'taxi_duration_mins decimal(6,2),'
#     'avg_flight_speed_kmps decimal(10,2),'
#     'engine_performance int,'
#     'passenger_dob date,'
#     'passenger_flight_class varchar(100)'
#     ') using orc '
#     'location "/Users/soumyadeepdey/DeepHDD/3aayaamFolder/3aayaamDataEnggV2/spark_sql_warehouse/3aayaam_db_1/flight_data_orc"'
# )

# spark.sql('insert into 3aayaam_db_1.flight_data_orc select * from 3aayaam_db_1.flight_data_csv')

# spark.sql('select * from 3aayaam_db_1.flight_data_orc limit 10').show()

# spark.sql('select passenger_country, passenger_name, distance, flight_cost, '
#           'dense_rank() over(partition by passenger_country order by flight_cost desc) as flight_cost_rank '
#           'from 3aayaam_db_1.flight_data_orc').show()








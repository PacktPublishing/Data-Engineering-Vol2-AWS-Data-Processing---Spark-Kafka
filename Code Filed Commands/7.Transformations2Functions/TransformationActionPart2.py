from pyspark.sql import SparkSession
from pyspark import SparkConf

conf = SparkConf()
conf.set("spark.executor.cores", "2")
conf.set("spark.executor.memory", "2g")

spark = SparkSession.builder\
        .config(conf=conf)\
        .master("spark://SoumyadeepDey.domain.name:7077")\
        .appName('TransformationActionPart2_DfIterator')\
        .getOrCreate()

df1 = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true')\
    .load('flight_data_csv/flight_data_1.csv')

###################################################################
# collect() Transformation
###################################################################
# df2 = df1.filter('passenger_country = "USA"').limit(10)
#
# usa_records = df2.limit(10).collect()
# print(type(usa_records))
# print(usa_records)

###################################################################
# foreach, foreachPartition Transformations
###################################################################
# def verify_taxi_duration(rows):
#     print(type(rows))
#     for row in rows:
#         print(row)
#         print('---------------------------------------------------------')
#         print('HERE IS THE TAXI DURATION: !!!!!', row.taxi_duration_mins)
#         print('---------------------------------------------------------')
    # print(rows)
    # taxi_duration_mins = rows.taxi_duration_mins
    #
    # if taxi_duration_mins > 200:
    #     print("-------------------------------------------")
    #     print("IMPORTANT NOTE !!!! very high taxi duration")
    #     print("-------------------------------------------")

# df2 = df1.filter('passenger_country = "USA"').limit(10)
# df2.foreach(verify_taxi_duration)
# df2.foreachPartition(verify_taxi_duration)

###################################################################
# Dataframe Iterator
###################################################################
# df2 = df1.filter('passenger_country = "USA"')
# itr_df2 = df2.toLocalIterator()
#
# for record in itr_df2:
#     print(record)


















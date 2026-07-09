from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, TimestampType

spark = SparkSession.builder.appName('DataFrameReaderWriter').getOrCreate()

#--------------------------------------------------------------------------------------
# Read from CSV file and write to PARQUET file using DataFrame reader & writer
#--------------------------------------------------------------------------------------
# df1 = spark.read.format('csv').load('orders.csv')
# df1.write.mode('overwrite').format('csv').save('orders.parquet')


#--------------------------------------------------------------------------------------
# Schema - header, inferSchema, define schema manually
#--------------------------------------------------------------------------------------
# df1 = spark.read.format('csv').load('orders.csv')
# df1 = spark.read.format('csv').option('header','true').load('orders.csv')
# df1 = spark.read.format('csv').option('header','true').option('inferSchema','true').load('orders.csv')

from pyspark.sql.types import IntegerType, VarcharType, StringType, DecimalType

# csv_schema = StructType([
#     StructField("id", StringType(), True),
#     StructField("cust_no", StringType(), True),
#     StructField("seller", StringType(), True),
#     StructField("date", DateType(), True),
#     StructField("price", DecimalType(), True),
#     StructField('method', StringType()),
# ])
#
# df1 = spark.read.schema(csv_schema).format('csv').load('orders.csv')
#
# df1.printSchema()


#--------------------------------------------------------------------------------------
# Split Size for small files
#--------------------------------------------------------------------------------------
print(spark.sparkContext.defaultParallelism)

df1 = spark.read.format('csv').load('customer_2.csv')
print(df1.rdd.getNumPartitions())













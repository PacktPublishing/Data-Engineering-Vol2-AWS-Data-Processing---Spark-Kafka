from pyspark.sql import SparkSession
from pyspark.context import SparkContext

spark = SparkSession.builder.appName('CreateDriverSparkSession').getOrCreate()

# spark = SparkSession(SparkContext())
# spark = spark.builder
# spark = spark.appName('CreateDriverSparkSession')
# spark = spark.getOrCreate()

print(spark)


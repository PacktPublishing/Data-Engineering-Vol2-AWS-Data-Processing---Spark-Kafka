from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName('BasicTransformationAction').getOrCreate()

# df_cust = spark.read.format('csv').option('header','true').\
#     option('inferSchema','true').load('customer.csv')

# df_cust.printSchema()
# df_cust.show(50)
# print(df_cust.columns)
# df2 = df_cust.select('customer_id','customer_name',"email")
# df2 = df_cust.select(df_cust.customer_id,df_cust.customer_name,'email')
# df_cust.show(2)
# df2.show(2)
# df3 = df_cust.where("country = 'Malta'")
# df3 = df_cust.where(df_cust.country == 'Malta')
# df3 = df_cust.filter("country = 'Malta'")
# df3.show(2)
# df4 = df_cust.select(df_cust.country).distinct()
# df4 = df_cust.distinct('country')
# df4.show()

# df_orders = spark.read.format('csv').option('header','true').\
#     option('inferSchema','true').load('orders.csv')

# df1 = df_orders.columns
# print(df1)
# df2 = df_orders.count()
# print(df2)
# df31.show(10)
# df3 = df_orders.select('customer_id')

##########################################################
# One-by-one or step-by-step transformation
##########################################################
# df31 = df_orders.filter('order_total_price > 30000')
# df3 = df31.select('customer_id')
# df4 = df3.distinct()

# df4.printSchema()
# df4.show(5)

# df5 = df_orders.select('seller_id')
# df6 = df5.distinct()
# df6.show()


##########################################################
# One-by-one and chain transformation
##########################################################
# df_orders = spark.read.format('csv').option('header','true').\
#     option('inferSchema','true').load('orders.csv')

# df1 = df_orders.select('order_id','customer_id','order_total_price','payment_method')
# df2 = df1.where('payment_method = "Visa"')
# df3 = df2.filter('order_total_price > 35000.00')
# df3.show(10)

# df11 = df_orders.select('order_id','customer_id','order_total_price','payment_method').where('payment_method = "Visa"')\
#     .filter('order_total_price > 35000.00')
# df11.show()


##########################################################
# Few more transformations:
# df.describe(['col1','col2']), df.limit(n), df.sort("col")
# df.drop("col"), df.dropDuplicates(['col1','col2'])
# df.dropna()
##########################################################
# spark = SparkSession.builder.appName('BasicTransformationAction').getOrCreate()

# df_cust = spark.read.format('csv').option('header', 'true')\
#     .option('inferSchema', 'true').load('customer.csv')

# df1 = df_cust.describe(['country', 'state','customer_id'])
# df1.show()

# df_cust.limit(10).show()

# df2 = df_cust.select('customer_id','dob','email').sort('dob','email', ascending=[False,True])
# df2.show(10,truncate=False)

# df_loan = spark.read.format('csv').option('header', 'true')\
#     .option('inferSchema', 'true').load('finance_loan_data.csv')

# df1 = df_loan.drop('loan_id', 'end_date', 'loan_type')
# df1.printSchema()
# df1.show(10)

# print(df_loan.count())
# print(df_loan.select('customer_id').distinct().count())

# df2 = df_loan.dropDuplicates(['customer_id'])
# print(df2.count())


##########################################################
# Action & Lazy Loading
##########################################################
# spark = SparkSession.builder.appName('SparkActionLazyLoading').getOrCreate()
#
# df_loan = spark.read.format('csv').option('header', 'true')\
#     .option('inferSchema', 'true').load('finance_loan_data.csv')
#
# df1 = df_loan.drop('loan_id','start_date','end_date')
# df2 = df1.sort("principal_amount", ascending=False)
# df3 = df2.filter('loan_type = "Education"').limit(100)
# df4 = df2.filter('loan_type = "Vehicle"').limit(100)
# df5 = df2.filter('loan_type = "Housing"').limit(100)
#
# df3.write.format('parquet').save('loan_education_data.parquet')
# df4.write.format('parquet').save('loan_vehicle_data.parquet')
# df5.write.format('parquet').save('loan_housing_data.parquet')
#
# df3.show(5)
# df4.show(5)
# df5.show(5)


##########################################################
# Write customer file in PARQUET & AVRO format
##########################################################
# spark_avro = SparkSession.builder.appName('')\
#               .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.5")\
#               .getOrCreate()
#
# df_cust = spark_avro.read.format('csv').option('header', 'true')\
#     .option('inferSchema', 'true').load('customer.csv')
#
#
# df1 = df_cust.coalesce(1)
# df1.write.format('parquet').save('customer.parquet')
#
# df2 = df_cust.coalesce(1)
# df2.write.format('avro').save('customer.avro')


##########################################################
# load() - Transformation or Action
##########################################################
# spark_csv1 = SparkSession.builder.appName('load-TransformationOrActionCSV1').getOrCreate()
# df11 = spark_csv1.read.format('csv').load('customer.csv')
# df11.show(2)
#
# spark_csv2 = SparkSession.builder.appName('load-TransformationOrActionCSV2').getOrCreate()
# df12 = spark_csv2.read.format('csv').option('header','true').load('customer.csv')
# df12.show(2)
#
# spark_csv3 = SparkSession.builder.appName('load-TransformationOrActionCSV3').getOrCreate()
# df13 = spark_csv3.read.format('csv').option('header','true').option('inferSchema','true').load('customer.csv')
# df13.show(2)
#
# spark_csv4 = SparkSession.builder.appName('load-TransformationOrActionCSV4').getOrCreate()
# df14 = spark_csv4.read.format('csv').option('inferSchema','true').load('customer.csv')
# df14.show(2)

# spark_json = SparkSession.builder.appName('load-TransformationOrActionJSON').getOrCreate()
# df2 = spark_json.read.format('json').option('inferSchema','true')\
#     .load('orders.json')
# df2.printSchema()
# df2.show()

# spark_parquet = SparkSession.builder.appName('load-TransformationOrActionPARQ').getOrCreate()
# df3 = spark_parquet.read.format('parquet').load('customer.parquet')
# df3.printSchema()
# df3.show()

# spark_avro = SparkSession.builder.appName('load-TransformationOrActionAVRO')\
#             .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.5")\
#             .getOrCreate()
# df4 = spark_avro.read.format('avro').load('customer.avro')
# df4.printSchema()
# df4.show()


##########################################################
# Performance comparison:
# Different file formats as source and target
##########################################################
spark = SparkSession.builder.appName('perf-test2').getOrCreate()

df_cust_csv = spark.read.format('csv').option('header','true')\
    .option('inferSchema','true')\
    .load('customer.csv')

df_cust_parquet = spark.read.format('parquet')\
    .load('customer.parquet')

#-----------------------------------------------------------------------------------------
# 1. Read --> Write
#-----------------------------------------------------------------------------------------
# df_cust_csv.write.format('csv').save('cust_perf11.csv')
# df_cust_csv.write.format('parquet').save('cust_perf12.parquet')
#
# df_cust_parquet.write.format('csv').save('cust_perf13.csv')
# df_cust_parquet.write.format('parquet').save('cust_perf14.parquet')

#-----------------------------------------------------------------------------------------
# 2. Read --> Select --> Filter --> Sort --> Write
#-----------------------------------------------------------------------------------------
# df21 = df_cust_csv.select('customer_id','customer_name','email','phone','credit_card_no','credit_card_type')\
#     .filter('credit_card_type = "MasterCard"')\
#     .sort('customer_id')
# df21.write.format('csv').save('cust_perf21.csv')
# df21.write.format('parquet').save('cust_perf21.parquet')
#
# df22 = df_cust_parquet.select('customer_id','customer_name','email','phone','credit_card_no','credit_card_type') \
#     .filter('credit_card_type = "MasterCard"') \
#     .sort('customer_id')
# df22.write.format('csv').save('cust_perf22.csv')
# df22.write.format('parquet').save('cust_perf22.parquet')

#-----------------------------------------------------------------------------------------
# 3. Read --> Select --> Distinct --> Show
#-----------------------------------------------------------------------------------------
# df31 = df_cust_csv.select('country','credit_card_type')\
#     .distinct()
# df31.show()
#
# df32 = df_cust_parquet.select('country','credit_card_type')\
#     .distinct()
# df32.show()

#-----------------------------------------------------------------------------------------
# 4. Read --> Sort --> Select --> Show
#-----------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------
# 5. Read --> Select --> Sort --> Drop --> Show
#-----------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------
# 6. Read --> Count --> Show
#-----------------------------------------------------------------------------------------

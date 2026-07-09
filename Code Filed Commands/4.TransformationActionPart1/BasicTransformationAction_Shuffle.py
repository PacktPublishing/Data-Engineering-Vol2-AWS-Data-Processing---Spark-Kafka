from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col

# spark = SparkSession.builder.appName('SparkShuffleInJoin').getOrCreate()
# var1 = spark.conf.get('spark.sql.shuffle.partitions')
# print("spark.sql.shuffle.partitions : ", var1)

#-------------------------------------------------------------
# Shuffle in groupBy()
#-------------------------------------------------------------
# df_loan = spark.read.format('csv')\
#     .option('header','true')\
#     .option('inferSchema', 'true')\
#     .load('finance_loan_data')
#
# df11 = df_loan.select('loan_type','principal_amount')
# print('Input Partitions :', df11.rdd.getNumPartitions())
#
# df21 = df11.groupby('loan_type').sum("principal_amount")
# print('Shuffle Partitions :', df21.rdd.getNumPartitions())
#
# df21.show()
# df21.write.format('csv').save('loan_data_shuffle_groupby')

#-------------------------------------------------------------
# Shuffle in join()
#-------------------------------------------------------------
# spark = SparkSession.builder.appName('ExperimentSparkShufflePartitions_2').getOrCreate()

# spark.conf.set('spark.sql.shuffle.partitions','2')
# print(spark.conf.get('spark.sql.shuffle.partitions'))

# df_loan = spark.read.format('csv')\
#     .option('header','true')\
#     .option('inferSchema', 'true')\
#     .load('finance_loan_data')

# df_cust = spark.read.format('csv').option('header','true').\
#     option('inferSchema','true').load('finance_customer_data')

# df12 = df_loan.select('loan_id','customer_id','loan_type','principal_amount', 'interest_rate','start_date')
# print(df12.rdd.getNumPartitions())

# df22 = df_cust.select('customer_id', 'first_name','last_name','email')
# print(df22.rdd.getNumPartitions())

# df_join = df12.join(df22, df12.customer_id == df22.customer_id, 'inner').drop(df22.customer_id)
# print(df_join.rdd.getNumPartitions())

# df_join.write.format('csv').save('loan_customer_join_data')


#-------------------------------------------------------------
# Shuffle in orderBy()
#-------------------------------------------------------------


#-------------------------------------------------------------
# Shuffle in distinct()
#-------------------------------------------------------------



##########################################################
# Performance comparison:
# Different file formats as source and target
# Application, Job and Stage details
##########################################################

#-----------------------------------------------------------------------------------------
# 1. Read --> Write
#-----------------------------------------------------------------------------------------
# spark = SparkSession.builder.appName('csv_to_parquet_to_csv').getOrCreate()
#
# df_cust_csv = spark.read.format('csv').option('header','true')\
#     .option('inferSchema','true')\
#     .load('finance_customer_data')
#
# df_cust_parquet = spark.read.format('parquet')\
#     .load('customer.parquet')
#
# df_cust_csv.write.format('csv').save('csv_to_csv.csv')
# df_cust_csv.write.format('parquet').save('csv_to_parquet.parquet')
#
# df_cust_parquet.write.format('csv').save('parquet_to_csv.csv')
# df_cust_parquet.write.format('parquet').save('parquet_to_parquet.parquet')

#-----------------------------------------------------------------------------------------
# 2. Read --> Select --> Filter --> Sort --> Write
#-----------------------------------------------------------------------------------------


#-----------------------------------------------------------------------------------------
# 3. Read --> Select --> Distinct --> Write
#-----------------------------------------------------------------------------------------

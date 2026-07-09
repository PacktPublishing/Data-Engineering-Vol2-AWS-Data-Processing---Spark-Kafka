from pyspark.sql import SparkSession
from pyspark import SparkConf

#############################################################################################################
# Explain Plan 1
#############################################################################################################
# from pyspark.sql.functions import count
# conf = SparkConf()
# conf.set("spark.sql.adaptive.enabled", "false")
#
# spark = (SparkSession.builder.config(conf=conf)
#                      .appName('SparkExplainPlan_1')
#                      .master('spark://Mac.lan:7077')
#                      .getOrCreate())
#
# df_flight = spark.read.format('parquet').load('fligt_data_parquet')
#
# df1 = df_flight.groupby('flight_id').agg(count('aircraft_id'))
# df1.explain(mode="codegen")
# df1.show()


#############################################################################################################
# Explain Plan 2
#############################################################################################################
from pyspark.sql.functions import count

# conf = SparkConf()
# conf.set("spark.sql.adaptive.enabled", "false")
#
# spark = (SparkSession.builder.config(conf=conf)
#                      .appName('SparkExplainPlan_2')
#                      .master('spark://Mac.lan:7077')
#                      .getOrCreate())
#
# df_flight = spark.read.format('parquet').load('fligt_data_parquet')
#
# df1 = df_flight.select('passenger_country').distinct()
# df1.explain()
# df1.show()

#############################################################################################################
# Explain Plan 3
#############################################################################################################
from pyspark.sql.functions import count

# conf = SparkConf()
# conf.set("spark.sql.adaptive.enabled", "false")
# conf.set("spark.sql.join.preferSortMergeJoin", "false")
#
# spark = (SparkSession.builder.config(conf=conf)
#                      .appName('SparkExplainPlan_3')
#                      .master('spark://Mac.lan:7077')
#                      .getOrCreate())
#
# df_account = spark.read.format('parquet').load('finance/finance_account_parquet')
# df_cust = spark.read.format('parquet').load('finance/finance_customer_parquet')
# df_loan = spark.read.format('parquet').load('finance/finance_loan_parquet')
#
# df_join = df_account.join(df_cust, df_account.customer_id == df_cust.customer_id, 'inner')
# df_join.explain()
# df_join.show()


#############################################################################################################
# Explain Plan 4
#############################################################################################################
from pyspark.sql.functions import count, col, dense_rank
from pyspark.sql import Window

# conf = SparkConf()
# conf.set("spark.sql.adaptive.enabled", "false")
# conf.set("spark.sql.join.preferSortMergeJoin", "false")
#
# spark = (SparkSession.builder.config(conf=conf)
#                      .appName('SparkExplainPlan_4')
#                      .master('spark://Mac.lan:7077')
#                      .getOrCreate())
#
# df_loan = spark.read.format('parquet').load('finance/finance_loan_parquet')
# df1 = df_loan.withColumn('loan_rank', dense_rank().over(Window.partitionBy('loan_type').orderBy(col('interest_rate').desc())))
#
# df1.explain()
# df1.show(30)

#############################################################################################################
# Explain Plan 5
#############################################################################################################
from pyspark.sql.functions import count

# conf = SparkConf()
# spark = (SparkSession.builder.config(conf=conf)
#                      .appName('SparkExplainPlan_1')
#                      .master('spark://Mac.lan:7077')
#                      .getOrCreate())
#
# df_flight = spark.read.format('parquet').load('fligt_data_parquet')
#
# df1 = df_flight.select('flight_id','aircraft_id','flight_cost','origin_airport','destination_airport',
#                        'departure_time','passenger_name','passenger_country',
#                        'travel_date','airplane_model','tail_no','distance',
#                        'turbulance','temp_at_dept','fuel_consumed_litre','taxi_duration_mins')
#
# df2 = df_flight.select('passenger_name','frequent_flier_no', 'travel_date', 'arrival_time',
#                           'passenger_country').filter('passenger_country = "France"').limit(10)
#
# df_joined = df1.join(df2, (df1.passenger_name == df2.passenger_name),'inner')\
#                      .drop(df2.passenger_name, df2.passenger_country, df2.travel_date)
#
# df_joined.explain()
# df_joined.explain(extended=True)
# df_joined.explain(mode="codegen")
# df_joined.explain(mode="cost")
# df_joined.explain(mode="formatted")


#############################################################################################################
# Explain Plan - Exercise
# Problem Statement:
# Find the loan details of top 3 countries (no. of customers wise)
# Loan details should add customer account information:
# i) Total no. of Accounts
# ii) Total Balance of all accounts
#############################################################################################################
# from pyspark.sql.functions import count, dense_rank, sum, col
#
# conf = SparkConf()
# conf.set("spark.sql.adaptive.enabled", "false")
#
# spark = (SparkSession.builder.config(conf=conf)
#                      .appName('SparkExplainPlan_2')
#                      .master('spark://Mac.lan:7077')
#                      .getOrCreate())
#
# df_account = spark.read.format('parquet').load('finance/finance_account_parquet')
# df_cust = spark.read.format('parquet').load('finance/finance_customer_parquet')
# df_loan = spark.read.format('parquet').load('finance/finance_loan_parquet')
#
# df_a1 = df_account.select('account_id','customer_id','balance')
# df_c1 = df_cust.select('customer_id','first_name','last_name','country')
# df_l1 = df_loan.select('loan_id','customer_id','loan_type','principal_amount','interest_rate','term_month')
#
# df_a2 = df_a1.groupby('customer_id').agg(count('account_id').alias('total_accounts_count'),
#                                          sum('balance').alias('total_balance'))
#
# df_c2 = df_c1.groupby('country').agg(count('customer_id').alias('cust_per_country'))
# df_c3 = df_c2.orderBy(col('cust_per_country').desc()).limit(5)
# df_c4 = df_c1.join(df_c3, df_c1.country == df_c3.country, 'inner').drop('cust_per_country')
#
# df_joined1 = df_a2.join(df_c4, df_a2.customer_id == df_c4.customer_id, 'inner')\
#                   .drop(df_c4.customer_id)
# df_joined2 = df_joined1.join(df_l1, df_joined1.customer_id == df_l1.customer_id)\
#                        .drop(df_l1.customer_id)
#
# df_joined2.explain()
# df_joined2.explain(extended=True)
# df_joined2.explain(mode="codegen")
# df_joined2.explain(mode="cost")
# df_joined2.explain(mode="formatted")
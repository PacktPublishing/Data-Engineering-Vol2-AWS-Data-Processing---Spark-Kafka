from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('BasicTransformationAction_2').getOrCreate()

# df_ledg = spark.read.format('csv')\
#     .option('header','true')\
#     .option('inferSchema', 'true')\
#     .load('finance_ledger_data.csv')


###########################################################################
# agg({"<col-name>" : "<aggr-function>"}). min, max, avg, count, sum
###########################################################################
# from pyspark.sql.functions import max, min, avg, sum

# df_acct = spark.read.format('csv')\
#     .option('header','true')\
#     .option('inferSchema', 'true')\
#     .load('finance_account_data.csv')

# df11 = df_acct.select('balance', 'acc_opening_date')
# # df12 = df11.agg({"balance" : "max"})
# df12 = df11.agg(min("balance").alias("min_balance"),
#                 max("balance").alias("max_balance"),
#                 avg("balance").alias("avg_balance"),
#                 max("acc_opening_date").alias("max_acc_opening_date"),
#                 min("acc_opening_date").alias("min_acc_opening_date"),)
# df12.show()


###########################################################################
# df.groupBy("<col-name>").agg()/max()/min()/sum()/avg()
###########################################################################
# df_tran = spark.read.format('csv')\
#     .option('header','true')\
#     .option('inferSchema', 'true')\
#     .load('finance_transaction_data.csv')
#
# from pyspark.sql.functions import sum, max, min, avg
# df21 = df_tran.select('tran_status','currency_code','amount')
# df22 = df21.groupby('tran_status','currency_code').agg(sum('amount').alias('total_amt'),
#                                        avg('amount').alias('avg_amt'),
#                                        max('amount').alias('max_amt'),
#                                        min(df21.amount).alias('min_amt'))
# df22.show()
#

###########################################################################
# df1.join(df2, '<col1-name>', 'join-type')
###########################################################################
# df_tran = spark.read.format('csv')\
#     .option('header','true')\
#     .option('inferSchema', 'true')\
#     .load('finance_transaction_data.csv')
#
# df_cust = spark.read.format('csv')\
#     .option('header','true')\
#     .option('inferSchema', 'true')\
#     .load('finance_customer_data.csv')
#
# df_loan = spark.read.format('csv')\
#     .option('header','true')\
#     .option('inferSchema', 'true')\
#     .load('finance_loan_data.csv')
#
# df_acct = spark.read.format('csv')\
#     .option('header','true')\
#     .option('inferSchema', 'true')\
#     .load('finance_account_data.csv')

# dfc1 = df_cust.select(df_cust.customer_id,df_cust.first_name,df_cust.last_name,df_cust.country)
# dfa1 = df_acct.select(df_acct.account_id,df_acct.customer_id,df_acct.balance)
# dfl1 = df_loan.select(df_loan.loan_id,df_loan.customer_id,df_loan.loan_type,df_loan.principal_amount)

# df_cal_join = dfc1.join(dfa1, dfc1.customer_id == dfa1.customer_id, 'inner').drop(dfa1.customer_id)\
#     .join(dfl1, dfc1.customer_id == dfl1.customer_id).drop(dfl1.customer_id)
#
# df_cal_join.show()

#---------------------------------------------------
# Join using multiple columns
#---------------------------------------------------
# dft1 = df_tran.select(df_tran.transaction_id,df_tran.src_account_id,df_tran.dest_account_id,
#                       df_tran.amount,df_tran.transaction_date)
# dfa2 = df_acct.select(df_acct.account_id,df_acct.customer_id,df_acct.balance,df_acct.acc_opening_date)
#
# df_join_at = dft1.join(dfa2,
#                        (dft1.src_account_id == dfa2.account_id) & (dft1.transaction_date == dfa2.acc_opening_date),
#                        'inner')

# df_join_at.show()


###########################################################################
# 'col' function
###########################################################################
from pyspark.sql.functions import col

# df_cust = spark.read.format('csv')\
#     .option('header','true')\
#     .option('inferSchema', 'true')\
#     .load('finance_customer_data.csv')

# df_cust.select(df_cust.phone_number.alias("home_phone")).show(5)
# df_cust.select(col('phone_number').alias('home_phone')).show(5)

# df_cust.filter(col('customer_id').between(10000,10050)).show()
# df_cust.filter(df_cust.customer_id.between(10000, 10010)).show()

# df_cust.printSchema()
# df_cust.select(col('customer_id').cast("string"), 'customer_id', col('created_at').cast('date'), 'created_at').show(5)
# df_cust.select(df_cust.customer_id.cast("int")).show()

# df_cust.filter(col('address_line_1').contains('Park')).show(5)
# df_cust.filter(df_cust.country.startswith('Uni')).show(5)
# df_cust.filter(col('country').endswith('dia')).show(5)
# df_cust.filter(df_cust.country.like('%di%')).show()
# df_cust.select(col('email').substr(0,3)).show(5)

# dfc1 = df_cust.select(col('customer_id').alias('customer_no'),
#                       col('created_at').cast('date'),
#                       col('updated_at').cast('date'),
#                       'country')
# dfc2 = dfc1.filter(dfc1.country.contains('ndia')).filter(col('customer_no').between(10000,200000))
# dfc2.show(truncate=False)


###########################################################################
# df.orderBy(col("<column>").desc/asc())
###########################################################################
from pyspark.sql.functions import col

# df_loan = spark.read.format('csv')\
#     .option('header','true')\
#     .option('inferSchema', 'true')\
#     .load('finance_loan_data.csv')

# df_loan.orderBy(col('start_date').desc()).limit(20).show()
# df_loan.orderBy(col('start_date').desc(), col('principal_amount').desc(), col('end_date').asc()).limit(50).show(50)


###########################################################################
# df.selectExpr("<col-with-runtime-functions>")
###########################################################################
# df_loan.selectExpr("(principal_amount * interest_rate)/100 as total_int",
#                "case when  loan_type == 'Business' then 'YES' else 'NO' end as busines_loan_or_not"
#                ).show()


###########################################################################
# df1.subtract(df2), df1.union(df2), df1.unionAll(df2), df1.intersect(df2)
###########################################################################
# df1 = df_loan.filter(col('loan_id').between(1000000,1000020))
# df1 = df_loan.filter(col('loan_id').between(1000000,1000020)).select('loan_id','customer_id')
# df2 = df_loan.filter(df_loan.loan_id.between(1000010,1000030))
# df2 = df_loan.filter(df_loan.loan_id.between(1000010,1000030)).select('loan_id','customer_id')
# df1.show()
# df2.show()
# df1.subtract(df2).show()
# df2.subtract(df1).show()
# df1.union(df2).show()
# df1.unionAll(df2).show()
# df1.intersect(df2).show()
# df2.intersect(df1).show()


###########################################################################
# df.withColumn(), df.withColumns()
###########################################################################
# df_loan = spark.read.format('csv')\
#     .option('header','true')\
#     .option('inferSchema', 'true')\
#     .load('finance_loan_data.csv')
#
# df1 = df_loan.select('loan_id','customer_id','loan_type','principal_amount','interest_rate','term_month').\
#        withColumn('interest_payable', col('principal_amount') * col('interest_rate')/100).\
#        withColumn('tot_int_rate', col('interest_rate')* col('term_month')).drop('interest_rate','term_month')
# df1.show()




















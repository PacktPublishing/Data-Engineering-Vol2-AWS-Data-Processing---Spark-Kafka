from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Project2_Finance_WriteFile').getOrCreate()

df_acct = spark.read.format('csv')\
    .option('header','true')\
    .option('inferSchema', 'true')\
    .load('finance_account_data')

df_cust = spark.read.format('csv')\
    .option('header','true')\
    .option('inferSchema', 'true')\
    .load('finance_customer_data')

df_ledg = spark.read.format('csv')\
    .option('header','true')\
    .option('inferSchema', 'true')\
    .load('finance_ledger_data')

df_loan = spark.read.format('csv')\
    .option('header','true')\
    .option('inferSchema', 'true')\
    .load('finance_loan_data')

df_tran = spark.read.format('csv')\
    .option('header','true')\
    .option('inferSchema', 'true')\
    .load('finance_transaction_data')

#########################################################################
# Perform all the operations using PARQUET and AVRO file also.
# Convert the input CSV file to PARQUET/AVRO and then write code for
# the reports.
#########################################################################

#########################################################################
# 1. Generate report for customers with their names, acc id,
#    balance, phone and email.
#########################################################################


#########################################################################
# 2. Need top 10 countries with highest customers and account balances.
#########################################################################


#########################################################################
# 3. Find out the top 25 cross-country transactions based on amount.
#########################################################################


#########################################################################
# 4. Find ledger details (type, amount, entry date) and their
#    corresponding account and customer details.
#########################################################################
df41 = df_ledg.select('transaction_id','account_id','ledger_type','amount','entry_date')
# print('df41 (df_ledg) :', df41.rdd.getNumPartitions())

df42 = df_acct.select('account_id','customer_id','balance')
# print('df42 (df_acct) :',df42.rdd.getNumPartitions())

df43 = df_cust.select('customer_id','first_name','last_name','email')
# print('df43 (df_cust) :',df43.rdd.getNumPartitions())

dfj_led_acc = df41.join(df42, df41.account_id == df42.account_id, 'inner').drop(df41.account_id)
# print('dfj_led_acc (1st join) :', dfj_led_acc.rdd.getNumPartitions())

dfj_led_acc_cus = dfj_led_acc.join(df43, df43.customer_id == dfj_led_acc.customer_id, 'inner').drop(dfj_led_acc.customer_id)
# print('dfj_led_acc_cus (2nd join) :', dfj_led_acc_cus.rdd.getNumPartitions())

dfj_led_acc_cus.write.format('parquet').save('led_acc_cus.parquet')


#########################################################################
# 5. Customer details who have the following:
#    a) Highest loan amount
#    b) Highest tenure of loan
#########################################################################
# Assignment Hint: aggr & max


#########################################################################
# 6. Which type of loan is the highest requirement.
#########################################################################
# Assignment Hint: sum, groupBy & limit or max


#########################################################################
# 7. Need the following details: Top 50 cross-country transactions
#    with their ledger entry and customer details.
#########################################################################
# Assignment Hint: sum, groupBy, join, limit








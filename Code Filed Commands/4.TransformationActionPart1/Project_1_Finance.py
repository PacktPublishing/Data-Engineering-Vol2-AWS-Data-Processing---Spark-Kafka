from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Project1_Finance').getOrCreate()

df_acct = spark.read.format('csv')\
    .option('header','true')\
    .option('inferSchema', 'true')\
    .load('finance_account_data.csv')

df_cust = spark.read.format('csv')\
    .option('header','true')\
    .option('inferSchema', 'true')\
    .load('finance_customer_data.csv')

df_ledg = spark.read.format('csv')\
    .option('header','true')\
    .option('inferSchema', 'true')\
    .load('finance_ledger_data.csv')

df_loan = spark.read.format('csv')\
    .option('header','true')\
    .option('inferSchema', 'true')\
    .load('finance_loan_data.csv')

df_tran = spark.read.format('csv')\
    .option('header','true')\
    .option('inferSchema', 'true')\
    .load('finance_transaction_data.csv')

#########################################################################
# 1. Customer Id and balance with more than $1B in acc.
#########################################################################
# df11 = df_acct.select('customer_id', 'balance')
# df11.printSchema()
# df12 = df11.where(df11.balance > 1000000000.00)
# df12.show(5,truncate=False)


#########################################################################
# 2. Customers (name, email, phone, zip code) from country France.
#########################################################################
# df21 = df_cust.select('first_name','last_name','email','phone_number','country','postal_code')
# df22 = df21.filter('country = "France"')
# df22.show(5)


#########################################################################
# 3. List of all unique countries.
#########################################################################
# df_cust.select('country').distinct().show(5)


#########################################################################
# 4. Transaction details of amount more than $2000.00.
#    Currency and Status are not needed.
#########################################################################
df41 = df_tran.drop('currency_code','tran_status').where(df_tran.amount > 2000)
df41.show(5)


#########################################################################
# 5. Which is the highest loan that customers have taken.
#########################################################################
# Assignment Hint: where & count


#########################################################################
# 6. Top 5 Business Loans and its customers.
#########################################################################
# Assignment Hint: limit


#########################################################################
# 7. Customers with 10 lowest balances.
#########################################################################
# Assignment Hint: sort & limit


#########################################################################
# 8. Need to analyze interest rate based on loan start date and tenure.
#########################################################################
# Assignment Hint: describe


#########################################################################
# 9. What is the highest ledger type entry.
#########################################################################
# Assignment Hint: count & limit


########################################################################
# 10. What is the total pending (“TranStatus.P”) transaction.
#########################################################################
# Assignment Hint: where & count





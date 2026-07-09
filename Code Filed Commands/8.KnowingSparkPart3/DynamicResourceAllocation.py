from pyspark.sql import SparkSession
from pyspark import SparkConf

conf = SparkConf()
conf.set('spark.executor.cores', '1')
conf.set('spark.dynamicAllocation.enabled','true')
conf.set('spark.dynamicAllocation.initialExecutors','2')
conf.set('spark.dynamicAllocation.maxExecutors', '5')
conf.set('spark.dynamicAllocation.schedulerBacklogTimeout', '1s')
conf.set('spark.dynamicAllocation.sustainedSchedulerBacklogTimeout', '1s')
conf.set('spark.dynamicAllocation.executorIdleTimeout', '15s')

spark = (SparkSession.builder.config(conf=conf)
                     .appName('DRA_4').config(conf=conf)
                     .master('spark://SoumyadeepDey.domain.name:7077')
                     .getOrCreate())

from pyspark.sql.functions import count, col, sum

df_account = spark.read.format('csv').option('header','true').option('inferSchema','true').load('finance_account_data')
df_cust = spark.read.format('csv').option('header','true').option('inferSchema','true').load('finance_customer_data')
df_loan = spark.read.format('csv').option('header','true').option('inferSchema','true').load('finance_loan_data')

df_a1 = df_account.select('account_id','customer_id','balance')
df_c1 = df_cust.select('customer_id','first_name','last_name','country')
df_l1 = df_loan.select('loan_id','customer_id','loan_type','principal_amount','interest_rate','term_month')



df_a2 = df_a1.groupby('customer_id').agg(count('account_id').alias('total_accounts_count'),
                                         sum('balance').alias('total_balance'))

df_c2 = df_c1.groupby('country').agg(count('customer_id').alias('cust_per_country'))
df_c3 = df_c2.orderBy(col('cust_per_country').desc()).limit(5)
df_c4 = df_c1.join(df_c3, df_c1.country == df_c3.country, 'inner').drop('cust_per_country')

df_joined1 = df_a2.join(df_c4, df_a2.customer_id == df_c4.customer_id, 'inner')\
                  .drop(df_c4.customer_id)
df_joined2 = df_joined1.join(df_l1, df_joined1.customer_id == df_l1.customer_id)\
                       .drop(df_l1.customer_id)

df_joined2.show()
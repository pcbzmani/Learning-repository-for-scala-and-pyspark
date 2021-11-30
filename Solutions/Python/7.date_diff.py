from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col,datediff,row_number,lit,when,first
from pyspark.sql.types import IntegerType

#Spark session
spark = SparkSession.builder\
        .master("local")\
        .appName("datediff")\
        .config('spark.ui.port', '4040')\
        .getOrCreate()

#Read file
df = spark.read\
        .format('csv')\
        .option('sep','|')\
        .option('inferSchema',True)\
        .option('header',True)\
        .load('/content/account_date.csv')


#Window function for first record to current record
w_up_cr = Window\
       .partitionBy('account_number')\
       .orderBy('date')\
       .rowsBetween(Window.unboundedPreceding, Window.currentRow)

#Window function for previous record to current record
w_op_cr = Window\
            .partitionBy('account_number')\
            .orderBy('date')\
            .rowsBetween(-1,Window.currentRow)

#Derive first date and previous date
df = df.withColumn('first_date',first(df['date']).over(w_uf))\
       .withColumn('previous_date',first(df['date']).over(w_up_cr))

window_dt = Window\
            .partitionBy('Account_number')\
            .orderBy('Date')

df = df.withColumn('constant',lit('1'))\
        .withColumn('first_dt_der',row_number().over(window_dt))

df = df.withColumn('date_diff_first',datediff('date','first_date'))\
.withColumn('date_diff_previous',datediff('date','previous_date'))\
.withColumn('first_dt',when(df['first_dt_der'] == df['constant'],lit('1'))\
            .otherwise(lit('0')))

df = df.select('account_number','date','date_diff_first','date_diff_previous','first_dt')

df.show()

# +--------------+----------+---------------+------------------+--------+
# |account_number|      date|date_diff_first|date_diff_previous|first_dt|
# +--------------+----------+---------------+------------------+--------+
# |           123|2016-01-01|              0|                 0|       1|
# |           123|2017-01-01|            366|               366|       0|
# |           123|2018-01-01|            731|               365|       0|
# |           123|2020-01-01|           1461|               730|       0|
# |           456|2019-01-01|              0|                 0|       1|
# |           456|2021-01-01|            731|               731|       0|
# |           896|2021-01-01|              0|                 0|       1|
# +--------------+----------+---------------+------------------+--------+


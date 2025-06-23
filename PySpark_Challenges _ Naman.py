# Databricks notebook source
from pyspark.sql import SparkSession 

# COMMAND ----------

spark = SparkSession.builder.appName('Challenges').getOrCreate()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType, IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenge 1: Total Spend Per Customer
# MAGIC
# MAGIC Calculate total amount spent by each customer.
# MAGIC

# COMMAND ----------

from pyspark.sql import Row

data = [
    Row(customer_id=1, amount=250),
    Row(customer_id=2, amount=450),
    Row(customer_id=1, amount=100),
    Row(customer_id=3, amount=300),
    Row(customer_id=2, amount=150)
]
df = spark.createDataFrame(data)
df.show()

# COMMAND ----------

df.groupBy("customer_id").agg(F.sum("amount")).show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenge 2: Highest Transaction Per Day
# MAGIC
# MAGIC Find the highest transaction amount for each day.
# MAGIC

# COMMAND ----------

data = [
    Row(date='2023-01-01', amount=100),
    Row(date='2023-01-01', amount=300),
    Row(date='2023-01-02', amount=150),
    Row(date='2023-01-02', amount=200)
]
df = spark.createDataFrame(data)
df.show()

# COMMAND ----------

df.groupBy('date').agg(F.max('amount')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenge 3: Fill Missing Cities With Default
# MAGIC
# MAGIC Replace null city values with 'Unknown'.
# MAGIC

# COMMAND ----------

data = [
    Row(customer_id=1, city='Dallas'),
    Row(customer_id=2, city=None),
    Row(customer_id=3, city='Austin'),
    Row(customer_id=4, city=None)
]
df = spark.createDataFrame(data)
df.show()

# COMMAND ----------

df.fillna(value='Unknown', subset=['city']).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenge 4: Compute Running Total by Customer
# MAGIC
# MAGIC Use a window function to compute cumulative sum of purchases per customer.
# MAGIC

# COMMAND ----------

data = [
    Row(customer_id=1, date='2023-01-01', amount=100),
    Row(customer_id=1, date='2023-01-02', amount=200),
    Row(customer_id=2, date='2023-01-01', amount=300),
    Row(customer_id=2, date='2023-01-02', amount=400)
]
df = spark.createDataFrame(data)
df.show()

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F
w = Window.partitionBy('customer_id')
cum_sum = df.withColumn('amount_cum', F.sum('amount').over(w))
cum_sum.show()
                                        

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenge 5: Average Sales Per Product
# MAGIC
# MAGIC Find average amount per product.
# MAGIC

# COMMAND ----------



# COMMAND ----------

data = [
    Row(product='A', amount=100),
    Row(product='B', amount=200),
    Row(product='A', amount=300),
    Row(product='B', amount=400)
]
df = spark.createDataFrame(data)
df.show()

# COMMAND ----------

df.groupBy('product').agg(F.avg('amount')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenge 6: Extract Year From Date
# MAGIC
# MAGIC Add a column to extract year from given date.
# MAGIC

# COMMAND ----------

data = [
    Row(customer='John', transaction_date='2022-11-01'),
    Row(customer='Alice', transaction_date='2023-01-01')
]
df = spark.createDataFrame(data)
df.show()


# COMMAND ----------

df_withYear = df.withColumn('year',year(df['transaction_date'])).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenge 7: Join Product and Sales Data
# MAGIC
# MAGIC Join two DataFrames on product_id to get product names with amounts.
# MAGIC

# COMMAND ----------

products = [
    Row(product_id=1, product_name='Phone'),
    Row(product_id=2, product_name='Tablet')
]
sales = [
    Row(product_id=1, amount=500),
    Row(product_id=2, amount=800),
    Row(product_id=1, amount=200)
]
df_products = spark.createDataFrame(products)
df_sales = spark.createDataFrame(sales)
df_products.show()
df_sales.show()

# COMMAND ----------

df_products.join(df_sales, on='product_id', how='inner').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenge 8: Split Tags Into Rows
# MAGIC
# MAGIC Given a list of comma-separated tags, explode them into individual rows.
# MAGIC

# COMMAND ----------

data = [
    Row(id=1, tags='tech,news'),
    Row(id=2, tags='sports,music'),
    Row(id=3, tags='food')
]
df = spark.createDataFrame(data)
df.show()


# COMMAND ----------


from pyspark.sql.functions import explode, split
df.withColumn('tags',explode(split(df['tags'],','))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenge 9: Top-N Records Per Group
# MAGIC
# MAGIC For each category, return top 2 records based on score.
# MAGIC

# COMMAND ----------

data = [
    Row(category='A', name='x', score=80),
    Row(category='A', name='y', score=90),
    Row(category='A', name='z', score=70),
    Row(category='B', name='p', score=60),
    Row(category='B', name='q', score=85)
]
df = spark.createDataFrame(data)
df.show()

# COMMAND ----------

w = Window.partitionBy('category').orderBy(F.desc('score'))
df_ranked = df.withColumn("rank", F.row_number().over(w))
top_2 = df_ranked.filter(F.col('rank') <= 2)
top_2 = top_2.drop('rank')
top_2.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Challenge 10: Null Safe Join
# MAGIC
# MAGIC Join two datasets where join key might have nulls, handle using null-safe join.
# MAGIC

# COMMAND ----------

data1 = [
    Row(id=1, name='John'),
    Row(id=None, name='Mike'),
    Row(id=2, name='Alice')
]
data2 = [
    Row(id=1, salary=5000),
    Row(id=None, salary=3000)
]
df1 = spark.createDataFrame(data1)
df2 = spark.createDataFrame(data2)
df1.show()
df2.show()


# COMMAND ----------

df1.join(df2, df1['id'].eqNullSafe(df2['id']),'outer').show()
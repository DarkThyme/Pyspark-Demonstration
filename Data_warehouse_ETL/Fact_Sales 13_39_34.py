# Databricks notebook source
from pyspark.sql import SparkSession 

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType, IntegerType

# COMMAND ----------

spark = SparkSession.builder.appName('FactSales2Dimensionality').getOrCreate()

# COMMAND ----------

df = spark.read.format("delta").load("/delta/fact_sales_stage1")

# COMMAND ----------

windowed_avg = df.groupBy("ProductCategory").agg(F.mean("UnitsSold").alias("avg_units"))

# COMMAND ----------

df = df.join(windowed_avg, on="ProductCategory", how="left") \
       .withColumn("UnitsSold", F.coalesce("UnitsSold", "avg_units")) \
       .withColumn("UnitsSold", F.round(F.col("UnitsSold"), 2)) \
       .drop("avg_units")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC We filled null UnitsSold with mean per ProductCategory

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("/delta/fact_sales_final")

# COMMAND ----------

median_values = df.filter("UnitPrice != -1") \
    .groupBy("ProductCategory") \
    .agg(F.expr('percentile_approx(UnitPrice, 0.5)').alias("median_price"))

# COMMAND ----------

# MAGIC %md
# MAGIC We filled null UnitPrice with median per ProductCategory

# COMMAND ----------

df = df.join(median_values, on="ProductCategory", how="left") \
       .withColumn("UnitPrice",F.round(F.when((F.col("UnitPrice") == -1) | F.col("UnitPrice").isNull(), F.col("median_price")).otherwise(F.col("UnitPrice")),2)) \
       .drop("median_price")

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("/delta/fact_sales_stage3")

# COMMAND ----------

df = spark.read.format("delta").load("/delta/fact_sales_stage3")

# COMMAND ----------

discount_median = df.groupBy("ProductCategory").agg(F.expr("percentile_approx(Discount, 0.5)").alias("median_discount"))

# COMMAND ----------

# MAGIC %md
# MAGIC We use median to replace null in discount sections, however, we will get 0s after rounding to 2 decimel places as they are in the order of 0,5,10,15... 
# MAGIC

# COMMAND ----------

df = df.join(discount_median, on="ProductCategory", how="left") \
       .withColumn("Discount", F.round(F.coalesce("Discount", "median_discount"), 2)) \
       .drop("median_discount")

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("/delta/fact_sales_final")
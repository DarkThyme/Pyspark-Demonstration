# Databricks notebook source
from pyspark.sql import SparkSession 

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import sha2, concat_ws, upper, col, expr, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType, IntegerType

# COMMAND ----------

spark = SparkSession.builder.appName('FactSales2Dimensionality').getOrCreate()

# COMMAND ----------

df_raw = spark.read.format("delta").load("/delta/fact_sales_final")

# COMMAND ----------

display(df_raw)

# COMMAND ----------

df_cleaned = spark.read.format("delta").load("/delta/df_fact_cleaned")

# COMMAND ----------

display(df_cleaned)

# COMMAND ----------

# MAGIC %md
# MAGIC Selecting only columns that are common in both

# COMMAND ----------

fact_columns = ["UnitsSold", "UnitPrice", "Discount", "SaleDate"]
df_raw_selected = df_raw.select(fact_columns)
df_cleaned_selected = df_cleaned.select(fact_columns)

# COMMAND ----------

df_raw_selected = df_raw_selected.withColumn("row_id", monotonically_increasing_id())
df_cleaned_selected = df_cleaned_selected.withColumn("row_id", monotonically_increasing_id())

# COMMAND ----------

df_compare = df_raw_selected.alias("raw").join(
    df_cleaned_selected.alias("cleaned"),
    on="row_id",
    how="inner"
)

df_mismatch = df_compare.filter(
    (col("raw.UnitsSold") != col("cleaned.UnitsSold")) |
    (col("raw.UnitPrice") != col("cleaned.UnitPrice")) |
    (col("raw.Discount") != col("cleaned.Discount")) |
    (col("raw.SaleDate") != col("cleaned.SaleDate"))
)

df_mismatch.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Seems there are no mismatch, lets print it out for usability.

# COMMAND ----------

print("Total Rows Compared:", df_compare.count())
print("Mismatched Rows:", df_mismatch.count())
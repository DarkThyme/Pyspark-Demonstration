# Databricks notebook source
from pyspark.sql import SparkSession 

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType, IntegerType

# COMMAND ----------

from itertools import chain

# COMMAND ----------

import datetime
from datetime import timedelta

# COMMAND ----------

spark = SparkSession.builder.appName('FactSales2Dimensionality').getOrCreate()

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/")

# COMMAND ----------

df = spark.read.csv("/FileStore/tables/fact_sales_data_v2.csv", header=True, inferSchema=True)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC We will replace null or improper values with either mean or median of the product name or category in case of UnitPrice. In case of UnitsSold we will use mean of UnitsSold in the name or category type. In case of discount, we will use median of discount in the name or category type.

# COMMAND ----------

#Compute the most frequent ProductName per (ProductCategory)
product_mode_df = (
    df.filter(F.col("ProductName").isNotNull())
      .groupBy("ProductCategory", "ProductName")
      .agg(F.count("*").alias("count"))
)

# COMMAND ----------

display(product_mode_df)

# COMMAND ----------

#Assign a Rank by count within each (ProductCategory)
window_spec = Window.partitionBy("ProductCategory").orderBy(F.desc("count"))

# COMMAND ----------

most_common_names = (
    product_mode_df
    .withColumn("rank", F.row_number().over(window_spec))
    .filter(F.col("rank") == 1)
    .select("ProductCategory", F.col("ProductName").alias("MostCommonProductName"))
)

# COMMAND ----------

display(most_common_names)

# COMMAND ----------

# MAGIC %md
# MAGIC We can see that the most common ProductName for each ProductCategory may not be intuitive. For example:
# MAGIC
# MAGIC "Clothing" → "Desk"
# MAGIC
# MAGIC "Electronics" → "Desk"
# MAGIC
# MAGIC So we will hardcode the entry and not go with the statistically obtained values (Reason being our dataset is very small so this method apperas more true)

# COMMAND ----------

impute_mapping = {
    "Clothing": "T-shirt",
    "Electronics": "Smartphone",
    "Furniture": "Desk"
}

# COMMAND ----------

df_imputed = df.withColumn(
    "ProductName",
    F.when(
        F.col("ProductName").isNull(),
        F.coalesce(
            F.create_map([F.lit(x) for x in chain(*impute_mapping.items())])[F.col("ProductCategory")],
            F.lit("Unknown_Product")
        )
    ).otherwise(F.col("ProductName"))
)

# COMMAND ----------

display(df_imputed)

# COMMAND ----------

df_imputed.write.format("delta").mode("overwrite").save("/delta/fact_sales_stage1")
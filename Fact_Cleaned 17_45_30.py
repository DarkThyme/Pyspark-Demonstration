# Databricks notebook source
from pyspark.sql import SparkSession 

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import sha2, concat_ws, upper, col, expr
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType, IntegerType

# COMMAND ----------

spark = SparkSession.builder.appName('FactSales2Dimensionality').getOrCreate()

# COMMAND ----------

df = spark.read.format("delta").load("/delta/fact_sales_final")

# COMMAND ----------

display(df)

# COMMAND ----------

df1 = spark.read.format("delta").load("/delta/dim_product")


# COMMAND ----------

display(df1)

# COMMAND ----------

df2 = spark.read.format("delta").load("/delta/dim_store_final")

# COMMAND ----------

display(df2)

# COMMAND ----------

df3 = spark.read.format("delta").load("/delta/dim_employee_final")

# COMMAND ----------

display(df3)

# COMMAND ----------

#Start with base fact table
df_fact = df

#1. Join with Product Dimension to get DIM_ProductId
df_fact = df_fact.join(
    df1.select("ProductCategory", "ProductName", "Brand", "DIM_ProductId"),
    on=["ProductCategory", "ProductName", "Brand"],
    how="left"
)

#2. Join with Store Dimension to get DIM_StoreId
df_fact = df_fact.join(
    df2.select("StoreRegion", "StoreName", "StoreType", "DIM_StoreId"),
    on=["StoreRegion", "StoreName", "StoreType"],
    how="left"
)

#3. Join with Employee Dimension to get DIM_EmployeeId
df_fact = df_fact.join(
    df3.select("SalesRep", "Department", "EmployeeRole", "DIM_EmployeeId"),
    on=["SalesRep", "Department", "EmployeeRole"],
    how="left"
)

# COMMAND ----------

display(df_fact)

# COMMAND ----------

df_fact.schema

# COMMAND ----------

df_fact.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/delta/df_fact")

# COMMAND ----------

df_fact_cleaned = df_fact.select(
    "DIM_ProductId",
    "DIM_StoreId",
    "DIM_EmployeeId",
    "UnitsSold",
    "UnitPrice",
    "Discount",
    "SaleDate"
)

# COMMAND ----------

display(df_fact_cleaned)

# COMMAND ----------

df_fact_cleaned.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/delta/df_fact_cleaned")


# COMMAND ----------

df_fact_cleaned = df_fact_cleaned.withColumn("NetRevenue",(col("UnitsSold") * col("UnitPrice") * (1 - col("Discount"))))

# COMMAND ----------

display(df_fact_cleaned)

# COMMAND ----------

df_fact_cleaned.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/delta/df_fact_cleaned")


# COMMAND ----------

invalid_products = df_fact_cleaned.join(
    df1.select("DIM_ProductId"),
    on="DIM_ProductId",
    how="left_anti"
)

print("Invalid DIM_ProductId rows in FACT:")
invalid_products.show()

# COMMAND ----------

invalid_stores = df_fact_cleaned.join(
    df2.select("DIM_StoreId"),
    on="DIM_StoreId",
    how="left_anti"
)

print("Invalid DIM_StoreId rows in FACT:")
invalid_stores.show()

# COMMAND ----------

invalid_employees = df_fact_cleaned.join(
    df3.select("DIM_EmployeeId"),
    on="DIM_EmployeeId",
    how="left_anti"
)

print("Invalid DIM_EmployeeId rows in FACT:")
invalid_employees.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Every table showed empty columns and only header which shows all surrogate keys in df_fact_cleaned match correctly with their respective dimension tables.
# MAGIC This is because :
# MAGIC ||left_anti join is empty	 Passed	 Every DIM_*Id in FACT exists in the respective DIM table||
# MAGIC ||No missing keys	Passed	successfully replaced natural keys with surrogate keys||
# MAGIC ||Data integrity preserved	Confirmed	Foreign key relationships are correctly established||
# MAGIC
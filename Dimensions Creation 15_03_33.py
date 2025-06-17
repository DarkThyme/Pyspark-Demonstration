# Databricks notebook source
from pyspark.sql import SparkSession 

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import sha2, concat_ws, upper
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType, IntegerType

# COMMAND ----------

spark = SparkSession.builder.appName('FactSales2Dimensionality').getOrCreate()

# COMMAND ----------

df = spark.read.format("delta").load("/delta/fact_sales_final")

# COMMAND ----------

df.show()

# COMMAND ----------

dim_product = df.select("ProductCategory", "ProductName", "Brand").distinct()

dim_product = dim_product.withColumn("DIM_ProductId",sha2(concat_ws("||", "ProductCategory", "ProductName", "Brand"), 256))

default_product = spark.createDataFrame([("N/A", "N/A", "N/A")], ["ProductCategory", "ProductName", "Brand"])
default_product = default_product.withColumn("DIM_ProductId",
    sha2(concat_ws("||", "ProductCategory", "ProductName", "Brand"), 256))

dim_product = dim_product.union(default_product)

# COMMAND ----------

display(dim_product)

# COMMAND ----------

dim_store = df.select("StoreRegion", "StoreName", "StoreType").distinct()
dim_store = dim_store.withColumn("DIM_StoreId",sha2(concat_ws("||", "StoreRegion", "StoreName", "StoreType"), 256))
default_store = spark.createDataFrame([("N/A", "N/A", "N/A")],["StoreRegion", "StoreName", "StoreType"]).withColumn("DIM_StoreId",
    sha2(concat_ws("||", "StoreRegion", "StoreName", "StoreType"), 256))
dim_store_final = dim_store.union(default_store)

# COMMAND ----------

display(dim_store_final)

# COMMAND ----------

dim_employee = df.select("SalesRep", "Department", "EmployeeRole").distinct()
dim_employee = dim_employee.withColumn("DIM_EmployeeId",sha2(concat_ws("||", "SalesRep", "Department", "EmployeeRole"), 256))
default_employee = spark.createDataFrame([("N/A", "N/A", "N/A")],["SalesRep", "Department", "EmployeeRole"]).withColumn("DIM_EmployeeId",
    sha2(concat_ws("||", "SalesRep", "Department", "EmployeeRole"), 256))
dim_employee_final = dim_employee.union(default_employee)

# COMMAND ----------

display(dim_employee_final)

# COMMAND ----------


dim_product.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/delta/dim_product")

dim_store_final.write \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .save("/delta/dim_store_final")

dim_employee_final.write \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .save("/delta/dim_employee_final")
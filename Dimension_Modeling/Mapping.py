# Databricks notebook source
from pyspark.sql import SparkSession 
spark = SparkSession.builder.appName('Mapping').getOrCreate()

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/")

# COMMAND ----------

df_Custom_Mapping_DIM = spark.read.csv("/FileStore/tables/Custom_Mapping_DIM.csv", header=True, inferSchema=True)
df_Custom_Mapping_DIM.show()

# COMMAND ----------

df_Dim_CustomerType = spark.read.csv("/FileStore/tables/Dim_CustomerType.csv", header=True, inferSchema=True)
df_Dim_CustomerType.show()

# COMMAND ----------

df_Dim_ProductCategory = spark.read.csv("/FileStore/tables/Dim_ProductCategory.csv", header=True, inferSchema=True)
df_Dim_ProductCategory.show()

# COMMAND ----------

df_Dim_StoreRegion = spark.read.csv("/FileStore/tables/Dim_StoreRegion.csv", header=True, inferSchema=True)
df_Dim_StoreRegion.show()

# COMMAND ----------

df_Fact_Transactions = spark.read.csv("/FileStore/tables/Fact_Transactions.csv", header=True, inferSchema=True)
df_Fact_Transactions.show()

# COMMAND ----------

from pyspark.sql.functions import sha2, concat_ws, col
df_Custom_Mapping_DIM = df_Custom_Mapping_DIM.withColumn("DIM_CustomLabelID",
                                                  sha2(concat_ws("||", col("ProductCategory"), col("StoreRegion"), col("CustomerType")), 256))

# COMMAND ----------

from pyspark.sql.functions import hash, trim, upper, col

df_Custom_Mapping_DIM = df_Custom_Mapping_DIM.withColumn("DIM_CustomLabelID",hash(col("DIM_CustomLabelID")).cast("bigint"))
df_Custom_Mapping_DIM = df_Custom_Mapping_DIM.select("DIM_CustomLabelID", "ProductCategory", "StoreRegion", "CustomerType", "MappingLabel")
df_Custom_Mapping_DIM.show()

# COMMAND ----------

df_Custom_Mapping_DIM.write.format("delta").mode("overwrite").saveAsTable("DIM_CustomLabel")

# COMMAND ----------

#ProductCategory DIM
df_Dim_ProductCategory = df_Dim_ProductCategory \
    .withColumn("ProductCategory", trim(upper(col("ProductCategory")))) \
    .withColumn("DIM_ProductCategoryID", hash(col("ProductCategory")).cast("bigint"))

#StoreRegion DIM
df_Dim_StoreRegion = df_Dim_StoreRegion \
    .withColumn("StoreRegion", trim(upper(col("StoreRegion")))) \
    .withColumn("DIM_StoreRegionID", hash(col("StoreRegion")).cast("bigint"))

#CustomerType DIM
df_Dim_CustomerType = df_Dim_CustomerType \
    .withColumn("CustomerType", trim(upper(col("CustomerType")))) \
    .withColumn("DIM_CustomerTypeID", hash(col("CustomerType")).cast("bigint"))

# COMMAND ----------

df_Dim_ProductCategory.show()

# COMMAND ----------

df_Dim_StoreRegion.show()

# COMMAND ----------

df_Dim_CustomerType.show()

# COMMAND ----------

df_Dim_ProductCategory.write.format("delta").mode("overwrite").saveAsTable("DIM_ProductCategory_Hashed")
df_Dim_StoreRegion.write.format("delta").mode("overwrite").saveAsTable("DIM_StoreRegion_Hashed")
df_Dim_CustomerType.write.format("delta").mode("overwrite").saveAsTable("DIM_CustomerType_Hashed")


# COMMAND ----------

df_fact_cleaned_input = df_Fact_Transactions \
    .withColumn("ProductCategory", upper(trim(col("ProductCategory")))) \
    .withColumn("StoreRegion", upper(trim(col("StoreRegion")))) \
    .withColumn("CustomerType", upper(trim(col("CustomerType"))))

# COMMAND ----------

# MAGIC %md
# MAGIC Normalize these tables so that t

# COMMAND ----------

df_fact_norm = df_fact_cleaned_input \
    .withColumn("ProductCategory", upper(trim(col("ProductCategory")))) \
    .withColumn("StoreRegion", upper(trim(col("StoreRegion")))) \
    .withColumn("CustomerType", upper(trim(col("CustomerType"))))


df_Dim_ProductCategory_norm = df_Dim_ProductCategory \
    .withColumn("ProductCategory", upper(trim(col("ProductCategory"))))

df_Dim_StoreRegion_norm = df_Dim_StoreRegion \
    .withColumn("StoreRegion", upper(trim(col("StoreRegion"))))

df_Dim_CustomerType_norm = df_Dim_CustomerType \
    .withColumn("CustomerType", upper(trim(col("CustomerType"))))

df_Custom_Mapping_DIM_norm = df_Custom_Mapping_DIM \
    .withColumn("ProductCategory", upper(trim(col("ProductCategory")))) \
    .withColumn("StoreRegion", upper(trim(col("StoreRegion")))) \
    .withColumn("CustomerType", upper(trim(col("CustomerType"))))


# COMMAND ----------

#Join with ProductCategory dimension
df_fact_enriched = df_fact_norm.join(
    df_Dim_ProductCategory_norm.select("ProductCategory", "DIM_ProductCategoryID"),
    on="ProductCategory", how="left")

#Join with StoreRegion dimension
df_fact_enriched = df_fact_enriched.join(
    df_Dim_StoreRegion_norm.select("StoreRegion", "DIM_StoreRegionID"),
    on="StoreRegion", how="left")

#Join with CustomerType dimension
df_fact_enriched = df_fact_enriched.join(
    df_Dim_CustomerType_norm.select("CustomerType", "DIM_CustomerTypeID"),
    on="CustomerType", how="left")


# COMMAND ----------

df_fact_final = df_fact_enriched.join(
    df_Custom_Mapping_DIM_norm.select("ProductCategory", "StoreRegion", "CustomerType", "DIM_CustomLabelID"),
    on=["ProductCategory", "StoreRegion", "CustomerType"],
    how="left")

# COMMAND ----------

df_fact_final.show()

# COMMAND ----------

#Check 
df_fact_cleaned = df_fact_final.select(
    "TransactionID", "DIM_ProductCategoryID", "DIM_StoreRegionID",
    "DIM_CustomerTypeID", "DIM_CustomLabelID", "Amount")
df_fact_cleaned.show()

# COMMAND ----------

df_fact_cleaned.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fact_transactions_cleaned")
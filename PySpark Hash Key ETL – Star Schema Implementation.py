# Databricks notebook source
from pyspark.sql import SparkSession 

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType, IntegerType

# COMMAND ----------

import datetime
from datetime import timedelta

# COMMAND ----------

spark = SparkSession.builder.appName('FactSalesDimensionality').getOrCreate()

# COMMAND ----------

dbutils.fs.ls("/FileStore/")

# COMMAND ----------

dbutils.fs.ls("/FileStore/shared_uploads/")


# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/")

# COMMAND ----------

df = spark.read.csv("/FileStore/tables/fact_sales.csv", header=True, inferSchema=True)



# COMMAND ----------

df.show()

# COMMAND ----------

df_region = df.select("Region").distinct()

# COMMAND ----------

df_Region = df_region.withColumn("DIM_RegionId", hash(upper(col("Region"))).cast("bigint"))

# COMMAND ----------

df_Region.show()

# COMMAND ----------

dfBase = spark.createDataFrame([("N/A", -1)], ["Region", "DIM_RegionId"])


# COMMAND ----------

df_Region_Final = df_Region.union(dfBase)
df_Region_Final.show()


# COMMAND ----------

df_sales_channel = df.select("SalesChannel").distinct()
df_sales_channel = df_sales_channel.withColumn("DIM_SalesChannelId", hash(upper(col("SalesChannel"))).cast("bigint"))
df_base = spark.createDataFrame([("N/A", -1)], ["SalesChannel", "DIM_SalesChannelId"])
df_sales_channel_final = df_sales_channel.union(df_base)
df_sales_channel_final.show()


# COMMAND ----------

df_customer = df.select("CustomerSegment").distinct()
df_customer = df_customer.withColumn("DIM_CustomerId", hash(upper(col("CustomerSegment"))).cast("bigint"))
df_base = spark.createDataFrame([("N/A", -1)], ["CustomerSegment", "DIM_CustomerId"])
df_customer_final = df_customer.union(df_base)
df_customer_final.show()


# COMMAND ----------

df_sales_rep = df.select("SalesRep").distinct()
df_sales_rep = df_sales_rep.withColumn("DIM_SalesRepId", hash(upper(col("SalesRep"))).cast("bigint"))
df_base = spark.createDataFrame([("N/A", -1)], ["SalesRep", "DIM_SalesRepId"])
df_sales_rep_final = df_sales_rep.union(df_base)
df_sales_rep_final.show()


# COMMAND ----------

df_location = df.select("StoreType").distinct()
df_location = df_location.withColumn("DIM_LocationId", hash(upper(col("StoreType"))).cast("bigint"))
df_base = spark.createDataFrame([("N/A", -1)], ["StoreType", "DIM_LocationId"])
df_location_final = df_location.union(df_base)
df_location_final.show()


# COMMAND ----------

df_ProductCategory = df.select("ProductCategory").distinct()
df_ProductCategory = df_ProductCategory.withColumn("DIM_ProductCategoryId", hash(upper(col("ProductCategory"))).cast("bigint"))
df_base = spark.createDataFrame([("N/A", -1)], ["ProductCategory", "DIM_ProductCategoryId"])
df_ProductCategory_final = df_ProductCategory.union(df_base)
df_ProductCategory_final.show()

# COMMAND ----------

df_ProductSubCategory = df.select("ProductSubCategory").distinct()
df_ProductSubCategory = df_ProductSubCategory.withColumn("DIM_ProductSubCategoryId", hash(upper(col("ProductSubCategory"))).cast("bigint"))
df_base = spark.createDataFrame([("N/A", -1)], ["ProductSubCategory", "DIM_ProductSubCategoryId"])
df_ProductSubCategory_final = df_ProductSubCategory.union(df_base)
df_ProductSubCategory_final.show()

# COMMAND ----------

date_range = df.select(min(col("SalesDate")).alias("MinDate"), max(col("SalesDate")).alias("MaxDate")).collect()
min_date = date_range[0]["MinDate"]
max_date = date_range[0]["MaxDate"]
print(min_date, max_date)

# COMMAND ----------

date_list = []
current_date = min_date
while current_date <= max_date:
    date_list.append((current_date,))
    current_date += timedelta(days=1)


df_dates = spark.createDataFrame(date_list, ["Date"])

# COMMAND ----------

df_dates.show()

# COMMAND ----------

df_dates = df_dates.withColumn("DIM_DateID", hash(col("Date")).cast("bigint")) \
                   .withColumn("MonthName", expr("date_format(Date, 'MMMM')")) \
                   .withColumn("Year", expr("year(Date)")) \
                   .withColumn("Semester", expr("CASE WHEN month(Date) IN (1,2,3,4,5,6) THEN 'H1' ELSE 'H2' END")) \
                   .withColumn("Quarter", expr("CASE WHEN month(Date) IN (1,2,3) THEN 'Q1' \
                                                WHEN month(Date) IN (4,5,6) THEN 'Q2' \
                                                WHEN month(Date) IN (7,8,9) THEN 'Q3' \
                                                ELSE 'Q4' END"))


# COMMAND ----------

df_dates.schema

# COMMAND ----------

schema = StructType([
    StructField("Date", DateType(), True),
    StructField("DIM_DateID", LongType(), False),
    StructField("MonthName", StringType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Semester", StringType(), False),
    StructField("Quarter", StringType(), False)
])

df_base = spark.createDataFrame([(None, -1, "N/A", -1, "N/A", "N/A")], schema=schema)


df_dates_final = df_dates.union(df_base)
display(df_dates_final)


# COMMAND ----------

df_Region_Final.write.format("delta").mode("overwrite").saveAsTable("DIM_Region")
df_sales_channel_final.write.format("delta").mode("overwrite").saveAsTable("DIM_SalesChannel")
df_customer_final.write.format("delta").mode("overwrite").saveAsTable("DIM_Customer")
df_sales_rep_final.write.format("delta").mode("overwrite").saveAsTable("DIM_SalesRep")
df_location_final.write.format("delta").mode("overwrite").saveAsTable("DIM_Location")
df_ProductCategory_final.write.format("delta").mode("overwrite").saveAsTable("DIM_ProductCategory")
df_ProductSubCategory_final.write.format("delta").mode("overwrite").saveAsTable("DIM_ProductSubCategory")
df_dates_final.write.format("delta").mode("overwrite").saveAsTable("DIM_Dates")


# COMMAND ----------

df_DIMRegion = spark.table("DIM_Region")
df_DimSalesChannel = spark.table("DIM_SalesChannel")
df_DIMCustomer = spark.table("DIM_Customer")
df_DimSalesRep = spark.table("DIM_SalesRep")
df_DIMLocation = spark.table("DIM_Location")
df_DIMProductCategory = spark.table("DIM_ProductCategory")
df_DIMProductSubCategory = spark.table("DIM_ProductSubCategory")
df_DIMDates = spark.table("DIM_Dates")


# COMMAND ----------

display(df_DIMRegion)

# COMMAND ----------

# MAGIC %md
# MAGIC ##All Tables are loaded

# COMMAND ----------

df_fact_clean = (
    df
    .withColumn("DIM_RegionId", when(col("Region").isNull(), -1)
                .otherwise(hash(upper(col("Region")))).cast("bigint"))
    .withColumn("DIM_ProductCategoryId", when(col("ProductCategory").isNull(), -1)
                .otherwise(hash(upper(col("ProductCategory")))).cast("bigint"))
    .withColumn("DIM_ProductSubCategoryId", when(col("ProductSubCategory").isNull(), -1)
                .otherwise(hash(upper(col("ProductSubCategory")))).cast("bigint"))
    .withColumn("DIM_SalesChannelId", when(col("SalesChannel").isNull(), -1)
                .otherwise(hash(upper(col("SalesChannel")))).cast("bigint"))
    .withColumn("DIM_CustomerId", when(col("CustomerSegment").isNull(), -1)
                .otherwise(hash(upper(col("CustomerSegment")))).cast("bigint"))
    .withColumn("DIM_SalesRepId", when(col("SalesRep").isNull(), -1)
                .otherwise(hash(upper(col("SalesRep")))).cast("bigint"))
    .withColumn("DIM_LocationId", when(col("StoreType").isNull(), -1)
                .otherwise(hash(upper(col("StoreType")))).cast("bigint"))
    .withColumn("DIM_DateId", when(col("SalesDate").isNull(), -1)
                .otherwise(hash(upper(col("SalesDate").cast("string")))).cast("bigint"))
    .select("DIM_RegionId", "DIM_ProductCategoryId", "DIM_ProductSubCategoryId",
            "DIM_SalesChannelId", "DIM_CustomerId", "DIM_SalesRepId", "DIM_LocationId", "DIM_DateId",
            "UnitsSold", "Revenue")
)

# COMMAND ----------

display(df_fact_clean)

# COMMAND ----------

df_fact_clean.select("DIM_ProductCategoryId").distinct().show()

# COMMAND ----------

df_DIMProductCategory.select("DIM_ProductCategoryId").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC Above table does not show -1 N/A as The -1 key (for "N/A" or missing values) is deliberately added in each DIM table as a default “unknown” or “not applicable” row.

# COMMAND ----------



df_fact_clean1 = (
    df
    .withColumn("DIM_RegionId", when(col("Region").isNull(), lit(-1))
                .otherwise(hash(upper(col("Region")))).cast("bigint"))
    .withColumn("DIM_ProductCategoryId", when(col("ProductCategory").isNull(), lit(-1))
                .otherwise(hash(upper(col("ProductCategory")))).cast("bigint"))
    .withColumn("DIM_ProductSubCategoryId", when(col("ProductSubCategory").isNull(), lit(-1))
                .otherwise(hash(upper(col("ProductSubCategory")))).cast("bigint"))
    .withColumn("DIM_SalesChannelId", when(col("SalesChannel").isNull(), lit(-1))
                .otherwise(hash(upper(col("SalesChannel")))).cast("bigint"))
    .withColumn("DIM_CustomerId", when(col("CustomerSegment").isNull(), lit(-1))
                .otherwise(hash(upper(col("CustomerSegment")))).cast("bigint"))
    .withColumn("DIM_SalesRepId", when(col("SalesRep").isNull(), lit(-1))
                .otherwise(hash(upper(col("SalesRep")))).cast("bigint"))
    .withColumn("DIM_LocationId", when(col("StoreType").isNull(), lit(-1))
                .otherwise(hash(upper(col("StoreType")))).cast("bigint"))
    .withColumn("DIM_DateId", when(col("SalesDate").isNull(), lit(-1))
                .otherwise(hash(col("SalesDate"))).cast("bigint"))
    .select(
        "DIM_RegionId", "DIM_ProductCategoryId", "DIM_ProductSubCategoryId",
        "DIM_SalesChannelId", "DIM_CustomerId", "DIM_SalesRepId",
        "DIM_LocationId", "DIM_DateId", "UnitsSold", "Revenue"
    )
)


# COMMAND ----------

display(df_fact_clean1)

# COMMAND ----------

df_joined = (
    df_fact_clean1
    .join(df_DIMRegion, "DIM_RegionId", "inner")
    .join(df_DIMProductCategory, "DIM_ProductCategoryId", "inner")
    .join(df_DIMProductSubCategory, "DIM_ProductSubCategoryId", "inner")
    .join(df_DimSalesChannel, "DIM_SalesChannelId", "inner")
    .join(df_DIMCustomer, "DIM_CustomerId", "inner")
    .join(df_DimSalesRep, "DIM_SalesRepId", "inner")
    .join(df_DIMLocation, "DIM_LocationId", "inner")
    .join(df_DIMDates, "DIM_DateId", "inner")
)

# COMMAND ----------

display(df_joined)
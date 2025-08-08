# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

employees_data = [
    (101, 'Alice'),
    (102, 'Bob'),
    (103, 'Charlie')
]

employees_schema = StructType([
    StructField("emp_id", IntegerType(), False),
    StructField("emp_name", StringType(), False)
])

# COMMAND ----------

employees_df = spark.createDataFrame(employees_data, employees_schema)

# COMMAND ----------

attendance_data = [
    (101, '2025-08-08 09:00:00', 'IN'),
    (101, '2025-08-08 17:00:00', 'OUT'),
    (102, '2025-08-08 08:30:00', 'IN'),
    (102, '2025-08-08 16:30:00', 'OUT'),
    (103, '2025-08-08 10:00:00', 'IN'),
    (103, '2025-08-08 18:00:00', 'OUT')
]

attendance_schema = StructType([
    StructField("emp_id", IntegerType(), False),
    StructField("punch_time", StringType(), False),
    StructField("punch_type", StringType(), False)
])

# COMMAND ----------

attendance_logs_df = spark.createDataFrame(attendance_data, attendance_schema)

# COMMAND ----------

attendance_logs_df = attendance_logs_df.withColumn(
    "punch_time", to_timestamp(col("punch_time"), "yyyy-MM-dd HH:mm:ss")
).withColumn(
    "work_date", to_date(col("punch_time"))
)

# COMMAND ----------

employees_df.createOrReplaceTempView("employees")
attendance_logs_df.createOrReplaceTempView("attendance_logs")

# COMMAND ----------

employees_df.show()

# COMMAND ----------

display(attendance_logs_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Method 1 --> Using punch_type

# COMMAND ----------

window_spec = Window.partitionBy("emp_id", "work_date").orderBy("punch_time")

# COMMAND ----------

joined_df = attendance_logs_df.join(employees_df, "emp_id")

# COMMAND ----------

punch_duration_df = joined_df.select(
    col("emp_name"),
    col("emp_id"),
    col("work_date"),
    col("punch_time"),
    col("punch_type"),
    lag(col("punch_time")).over(window_spec).alias("previous_punch")
)

# COMMAND ----------

daily_metrics_method1 = punch_duration_df.groupBy("emp_id", "emp_name", "work_date").agg(
    min(when(col("punch_type") == "IN", col("punch_time"))).alias("first_in"),
    max(when(col("punch_type") == "OUT", col("punch_time"))).alias("last_out"),
    round(
        sum(
            when(
                (col("punch_type") == "OUT") & col("previous_punch").isNotNull(),
                (unix_timestamp(col("punch_time")) - unix_timestamp(col("previous_punch"))) / 3600.0
            ).otherwise(0)
        ), 2
    ).alias("hours_at_work")
).orderBy("work_date", "emp_name")

# COMMAND ----------

daily_metrics_method1.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Method 2 --> Sequence-Based Approach

# COMMAND ----------

sequenced_punches_df = attendance_logs_df.alias("a").join(
    employees_df.alias("e"),
    col("a.emp_id") == col("e.emp_id")
).select(
    col("a.emp_id"),
    col("e.emp_name"),
    col("a.work_date"),
    col("a.punch_time"),
    row_number().over(
        Window.partitionBy("a.emp_id", "a.work_date").orderBy("a.punch_time")
    ).alias("punch_seq")
)

# COMMAND ----------

punch_pairs_df = sequenced_punches_df.alias("p1").join(
    sequenced_punches_df.alias("p2"),
    (col("p1.emp_id") == col("p2.emp_id")) &
    (col("p1.work_date") == col("p2.work_date")) &
    (col("p2.punch_seq") == col("p1.punch_seq") + 1) &
    (col("p1.punch_seq") % 2 == 1)
).select(
    col("p1.emp_id"),
    col("p1.emp_name"),
    col("p1.work_date"),
    col("p1.punch_time").alias("punch_in"),
    col("p2.punch_time").alias("punch_out"),
    ((unix_timestamp(col("p2.punch_time")) - unix_timestamp(col("p1.punch_time"))) / 3600.0).alias("duration_hours")
)

# COMMAND ----------

daily_metrics_method2 = punch_pairs_df.groupBy(col("emp_id"), col("emp_name"), col("work_date")).agg(
    min("punch_in").alias("first_in"),
    max("punch_out").alias("last_out"),
    round(sum("duration_hours"), 2).alias("hours_at_work")
).orderBy("work_date", "emp_name")

# COMMAND ----------

daily_metrics_method2.show(truncate=False)
# Databricks notebook source
df_final=spark.read.format("delta").load("/Volumes/workspace/default/nyc_volume/silver_v2/nyc_taxi")
df_final.display()

# COMMAND ----------

# DBTITLE 1,aggregation
from pyspark.sql.functions import sum, count
df_agg=df_final.groupBy("PULocationID")\
.agg(
count("*").alias("trip_count"),
sum("total_amount").alias("revenue")
)
df_agg.display()

# COMMAND ----------

# DBTITLE 1,window+rank
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

window=Window.orderBy(col("revenue").desc())

df_rank=df_agg.withColumn(
"rank",
rank().over(window)
)
df_rank.display()


# COMMAND ----------

spark.sql("create database if not exists nyc_taxi")


# COMMAND ----------

df_final.write.mode("overwrite").saveAsTable("nyc_taxi.gold_table")

# COMMAND ----------

spark.read.table("nyc_taxi.gold_table").display()
# Databricks notebook source
df=spark.read.format("parquet") \
.load("/Volumes/workspace/default/nyc_volume/yellow_tripdata_2026-02.parquet")
display(df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
df=df.withColumn("ingestion_time",current_timestamp())

# COMMAND ----------

df.select("ingestion_time").show(5)

# COMMAND ----------

# DBTITLE 1,SAVING TO BRONZE
df.write.format("delta")\
.mode("append") \
.save("/Volumes/workspace/default/nyc_volume/bronze_v2/nyc_taxi")    

# COMMAND ----------

df_bronze=spark.read.format("delta")\
.load("/Volumes/workspace/default/nyc_volume/bronze_v2/nyc_taxi")
df_bronze.select("ingestion_time").show(5)

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/default/nyc_volume/bronze_v2/nyc_taxi"))
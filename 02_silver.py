# Databricks notebook source
df=spark.read.format("delta") \
.load("/Volumes/workspace/default/nyc_volume/bronze_v2/nyc_taxi")
display(df)

# COMMAND ----------

df.printSchema()
df.count()

# COMMAND ----------

# DBTITLE 1,remove badtrips
from pyspark.sql.functions import col
df_clean=df.filter(
(col("fare_amount")>0) &
(col("trip_distance")>0)
)

# COMMAND ----------

# DBTITLE 1,convert timestamp columns
from pyspark.sql.functions import to_timestamp
df_clean=df_clean \
.withColumn("pickup_time",to_timestamp("tpep_pickup_datetime"))\
.withColumn("dropoff_time",to_timestamp("tpep_dropoff_datetime"))

# COMMAND ----------

# DBTITLE 1,business logic column
df_clean=df_clean \
.withColumn("trip_duration_minutes",(col("dropoff_time").cast("long")-col("pickup_time").cast("long")) / 60
)

# COMMAND ----------

# DBTITLE 1,remove null
df_clean=df_clean.dropna()

# COMMAND ----------

# DBTITLE 1,saving silver
df_clean.write.format("delta")\
.mode("overwrite")\
.save("/Volumes/workspace/default/nyc_volume/silver_v2/nyc_taxi")

# COMMAND ----------

df_silver=spark.read.format("delta") \
.load("/Volumes/workspace/default/nyc_volume/silver_v2/nyc_taxi")
display(df_silver)

# COMMAND ----------


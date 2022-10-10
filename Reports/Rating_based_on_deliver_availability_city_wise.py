# Databricks notebook source
fact_df= spark.read.parquet("/mnt/swiggyanalysis/fact/fact_table")

# COMMAND ----------

display(fact_df)

# COMMAND ----------

city_dim = spark.read.parquet("/mnt/swiggyanalysis/dimension/city")

# COMMAND ----------

online_del_dim=spark.read.parquet("/mnt/swiggyanalysis/dimension/has_online_delivery")

# COMMAND ----------

rating_in_stars_dim=spark.read.parquet("/mnt/swiggyanalysis/dimension/rating_stars_out_of_5")

# COMMAND ----------

result_df=fact_df.join(city_dim,city_dim.city_id==fact_df["city_id"]).join(online_del_dim,online_del_dim.has_online_delivery_id==fact_df["has_online_delivery_id"]).join(rating_in_stars_dim,rating_in_stars_dim.rating_stars_out_of_5_id==fact_df["rating_stars_out_of_5_id"]).select("city","has_online_delivery","rating_stars_out_of_5")

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number,desc,avg,round,min
windowDept = Window.partitionBy("city").orderBy(col("has_online_delivery"))
df2=result_df.withColumn("id",row_number().over(windowDept))
df3=df2.groupBy(col("city"),col("has_online_delivery")).agg(min("rating_stars_out_of_5").alias("Rating")).orderBy(col("city"),col("has_online_delivery").desc())

# COMMAND ----------

display(df3)

# COMMAND ----------

df3.write.mode("overwrite").parquet("/mnt/swiggyanalysis/processed/Rating_based_on_deliver_availability_city_wise")
# Databricks notebook source
fact_df= spark.read.parquet("/mnt/swiggyanalysis/fact/fact_table")

# COMMAND ----------

display(fact_df)

# COMMAND ----------

city_dim = spark.read.parquet("/mnt/swiggyanalysis/dimension/city")

# COMMAND ----------

rest_dim = spark.read.parquet("/mnt/swiggyanalysis/dimension/restaurant_name")

# COMMAND ----------

result_df=fact_df.join(city_dim,city_dim.city_id==fact_df["city_id"]).join(rest_dim,rest_dim.restaurant_name_id==fact_df["restaurant_name_id"]).select("restaurant_name","city","votes")

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
windowDept = Window.partitionBy("city").orderBy(col("votes").desc(),col("restaurant_name"))
df2=result_df.withColumn("row",row_number().over(windowDept))
# df2.show()
df3=df2.filter(col("row") <= 10).select("city","restaurant_name","votes")

# COMMAND ----------

display(df3)

# COMMAND ----------

df3.write.mode("overwrite").parquet("/mnt/swiggyanalysis/processed/Top_10_restaurants_based_on_avg_votes")
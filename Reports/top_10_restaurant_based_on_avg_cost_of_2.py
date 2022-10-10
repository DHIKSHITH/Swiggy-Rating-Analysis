# Databricks notebook source
fact_df= spark.read.parquet("/mnt/swiggyanalysis/fact/fact_table")

# COMMAND ----------

display(fact_df)

# COMMAND ----------

city_dim = spark.read.parquet("/mnt/swiggyanalysis/dimension/city")

# COMMAND ----------

rest_dim = spark.read.parquet("/mnt/swiggyanalysis/dimension/restaurant_name")

# COMMAND ----------

result_df=fact_df.join(city_dim,city_dim.city_id==fact_df["city_id"]).join(rest_dim,rest_dim.restaurant_name_id==fact_df["restaurant_name_id"]).select("restaurant_name","city","average_cost_for_two")

# COMMAND ----------

display(result_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum ,avg,col

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number,rank
windowDept = Window.partitionBy("city").orderBy(col("average_cost_for_two").desc(),col("restaurant_name"))
df2=result_df.withColumn("row",rank().over(windowDept))
df3=df2.filter(col("row") <= 10).select("city","restaurant_name","average_cost_for_two")


# COMMAND ----------

display(df3)

# COMMAND ----------

df3.write.mode("overwrite").parquet("/mnt/swiggyanalysis/processed/top_10_restaurant_based_on_avg_cost_of_2")
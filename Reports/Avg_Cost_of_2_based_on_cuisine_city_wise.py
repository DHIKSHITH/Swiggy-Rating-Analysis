# Databricks notebook source
fact_df= spark.read.parquet("/mnt/swiggyanalysis/fact/fact_table")

# COMMAND ----------

display(fact_df)

# COMMAND ----------

city_dim = spark.read.parquet("/mnt/swiggyanalysis/dimension/city")

# COMMAND ----------

cuisines_dim = spark.read.parquet("/mnt/swiggyanalysis/dimension/cuisines")

# COMMAND ----------

result_df=fact_df.join(city_dim,city_dim.city_id==fact_df["city_id"]).join(cuisines_dim,cuisines_dim.cuisines_id==fact_df["cuisines_id"]).select("city","cuisines","average_cost_for_two")

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number,desc,avg,round
# windowDept = Window.partitionBy("City").orderBy(col("Cuisines"))
# df2=df.withColumn("id",row_number().over(windowDept))
df3=result_df.groupBy(col("city"),col("cuisines")).agg(round(avg("average_cost_for_two"),0).alias("average_cost_of_2_based_on_cuisines")).orderBy(col("city"),col("average_cost_of_2_based_on_cuisines").desc())

# COMMAND ----------

display(df3)

# COMMAND ----------

df3.write.mode("overwrite").parquet("/mnt/swiggyanalysis/processed/Avg_Cost_of_2_based_on_cuisine_city_wise")

# COMMAND ----------


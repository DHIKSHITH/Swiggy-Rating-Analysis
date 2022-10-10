# Databricks notebook source
fact_df= spark.read.parquet("/mnt/swiggyanalysis/fact/fact_table")

# COMMAND ----------

display(fact_df)

# COMMAND ----------

city_dim = spark.read.parquet("/mnt/swiggyanalysis/dimension/city")

# COMMAND ----------

display(city_dim)

# COMMAND ----------

final_df=fact_df.join(city_dim,city_dim.city_id==fact_df["city_id"]).select("city").groupBy('city').count()

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/swiggyanalysis/processed/Count_of_Restaurant_City_Wise")

# COMMAND ----------

display(spark.read.parquet("/mnt/swiggyanalysis/processed/Count_of_Restaurant_City_Wise"))

# COMMAND ----------


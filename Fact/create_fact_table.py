# Databricks notebook source
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType

# COMMAND ----------

swiggy_Schema = StructType(fields=[StructField("Restaurant Name",StringType(),False),
                                   StructField("City",StringType(),False),
                                   StructField("Locality",StringType(),False),
                                   StructField("Cuisines",StringType(),False),
                                   StructField("Average Cost for Two",IntegerType(),False),
                                   StructField("Has Table booking",StringType(),False),
                                   StructField("Has Online delivery",StringType(),False),
                                   StructField("Rating Stars out of 5",DoubleType(),False),
                                   StructField("Rating in text",StringType(),False),
                                   StructField("Price range",IntegerType(),False),
                                   StructField("Votes",IntegerType(),False)
                                 ])

# COMMAND ----------

swiggy_df=spark.read.option("header",True).schema(swiggy_Schema).csv("dbfs:/mnt/swiggyanalysis/raw/Swiggy_Analysis_Source_File.csv")

# COMMAND ----------

display(swiggy_df)

# COMMAND ----------

rest_dim = spark.read.parquet("/mnt/swiggyanalysis/dimension/restaurant_name")

# COMMAND ----------

display(rest_dim)

# COMMAND ----------

city_dim = spark.read.parquet("/mnt/swiggyanalysis/dimension/city")

# COMMAND ----------

display(city_dim)

# COMMAND ----------

locality_dim = spark.read.parquet("/mnt/swiggyanalysis/dimension/locality")

# COMMAND ----------

display(locality_dim)

# COMMAND ----------

cuisines_dim = spark.read.parquet("/mnt/swiggyanalysis/dimension/cuisines")

# COMMAND ----------

display(cuisines_dim)

# COMMAND ----------

online_del_dim=spark.read.parquet("/mnt/swiggyanalysis/dimension/has_online_delivery")

# COMMAND ----------

display(online_del_dim)

# COMMAND ----------

table_booking_dim=spark.read.parquet("/mnt/swiggyanalysis/dimension/has_table_booking")

# COMMAND ----------

display(table_booking_dim)

# COMMAND ----------

rating_in_text_dim=spark.read.parquet("/mnt/swiggyanalysis/dimension/rating_in_text")

# COMMAND ----------

display(rating_in_text_dim)

# COMMAND ----------

rating_in_stars_dim=spark.read.parquet("/mnt/swiggyanalysis/dimension/rating_stars_out_of_5")

# COMMAND ----------

display(rating_in_stars_dim)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import *
window = Window.orderBy(col('restaurant_name_id')) # Created a window specification for generating ID

#Used a left join to get all the data from dimension table and generated fact_id and renamed the column
fact_df = swiggy_df.join(rest_dim,rest_dim.restaurant_name == swiggy_df["Restaurant Name"],"left")\
.join(city_dim,city_dim.city == swiggy_df["City"],"left")\
.join(locality_dim,locality_dim.locality == swiggy_df["Locality"],"left")\
.join(cuisines_dim,cuisines_dim.cuisines==swiggy_df["Cuisines"],"left")\
.join(online_del_dim,online_del_dim.has_online_delivery == swiggy_df["Has Online delivery"],"left")\
.join(table_booking_dim,table_booking_dim.has_table_booking==swiggy_df["Has Table booking"],"left")\
.join(rating_in_text_dim,rating_in_text_dim.rating_in_text == swiggy_df["Rating in text"],"left")\
.join(rating_in_stars_dim,rating_in_stars_dim.rating_stars_out_of_5 == swiggy_df["Rating Stars out of 5"],"left")\
.withColumn('fact_id', row_number().over(window))\
.select("fact_id","restaurant_name_id","city_id","locality_id","cuisines_id","has_online_delivery_id","has_table_booking_id","rating_in_text_id","rating_stars_out_of_5_id","Average Cost for Two","Price range","Votes").withColumnRenamed("Average Cost for Two","average_cost_for_two")\
.withColumnRenamed("Price range","price_range")\
.withColumnRenamed("Votes","votes")

# COMMAND ----------

display(fact_df)

# COMMAND ----------

fact_df.write.mode("overwrite").format("parquet").saveAsTable("swiggy_fact.fact_table")

# COMMAND ----------


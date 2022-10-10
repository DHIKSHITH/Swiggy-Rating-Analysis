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

from pyspark.sql.functions import *

df_with_increasing_id = swiggy_df.withColumn("monotonically_increasing_id", monotonically_increasing_id())
display(df_with_increasing_id)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import *

window = Window.orderBy(col('monotonically_increasing_id'))
df_with_id = df_with_increasing_id.withColumn('id', row_number().over(window))
final_df = df_with_id.drop("monotonically_increasing_id").select("id","Restaurant Name","City","Locality","Cuisines","Average Cost for Two","Has Table booking","Has Online delivery","Rating Stars out of 5","Rating in text","Price range","Votes")

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/swiggyanalysis/processed/rawProcessed")

# COMMAND ----------

display(spark.read.parquet("/mnt/swiggyanalysis/processed/rawProcessed"))

# COMMAND ----------


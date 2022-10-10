# Databricks notebook source
df= spark.read.parquet("/mnt/swiggyanalysis/processed/rawProcessed")

# COMMAND ----------

display(df)

# COMMAND ----------

arr=df.schema.names

# COMMAND ----------

excluded_arr=arr[1:10]
excluded_arr.remove("Average Cost for Two")

# COMMAND ----------

def automate(field_name):
    name_df = df.select(field_name).distinct() #select the distinct values from each field
    
    window = Window.orderBy(col(field_name)) #create a window specification
    
    df_with_id = name_df.withColumn(f'{field_name.replace(" ", "_").lower()}_id', row_number().over(window)) #generate a id column using window specification
    
    final_dim= df_with_id.withColumnRenamed(field_name,field_name.replace(" ", "_").lower()).select(f'{field_name.replace(" ", "_").lower()}_id',field_name.replace(" ", "_").lower()) #rename the column with lower case and replace space with _
    
    final_dim.write.mode("overwrite").format("parquet").saveAsTable(f"swiggy_dim.{field_name.replace(' ', '_').lower()}") #create a table with the swiggy_dim database 
    
    

# COMMAND ----------

print(excluded_arr)

# COMMAND ----------

for i in excluded_arr:
    automate(i)

# COMMAND ----------

display(spark.read.parquet("/mnt/swiggyanalysis/dimension/city")) # For testing City

# COMMAND ----------


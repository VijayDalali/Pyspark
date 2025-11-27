# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading CSV
# MAGIC  
# MAGIC

# COMMAND ----------

df = spark.read.csv("/Volumes/workspace/default/firststep/BigMart Sales.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DDL SCHEMA

# COMMAND ----------

my_ddl_schmea = '''
                item_identifier string,
                item_weight double,
                item_fat_content string,
                item_visibility double,
                item_type string,
                item_mrp double,
                outlet_identifier string,
                outlet_establishment_year integer,
                outlet_size string,
                outlet_location_type string,
                outlet_type string,
                item_outlet_sales double
                '''

# COMMAND ----------

df = spark.read.csv("/Volumes/workspace/default/firststep/BigMart Sales.csv",header=True,schema=my_ddl_schmea)



# COMMAND ----------

df.display()

# COMMAND ----------

 df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading json

# COMMAND ----------

df_json = spark.read.json("/Volumes/workspace/default/firststep/drivers.json")


df_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###   StructType() Schema

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
df_json.printSchema()
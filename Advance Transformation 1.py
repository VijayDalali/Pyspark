# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.csv("/Volumes/workspace/default/firststep/BigMart Sales.csv",header=True,inferSchema=True)

# COMMAND ----------

display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##EXPLODE

# COMMAND ----------

df_exp = df.withColumn('Outlet_type',split('Outlet_Type', ' '))
df_exp.display()

# COMMAND ----------

df_exp = df_exp.withColumn('Outlet_type',explode('Outlet_type'))
df_exp.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Arrray_Contains

# COMMAND ----------

df_exp = df.withColumn('Outlet_type',split('Outlet_Type', ' '))
df_exp.display()

# COMMAND ----------

df_exp.withColumn('Type_flag',array_contains('Outlet_type','Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Group_BY

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Scenrio-1

# COMMAND ----------

df = df.groupBy('Item_Type').agg(sum('Item_MRP')).display()

# COMMAND ----------

df = spark.read.csv("/Volumes/workspace/default/firststep/BigMart Sales.csv",header=True,inferSchema=True)
df.display()

# COMMAND ----------

#Scenrio - 2
df = df.groupBy('Item_Type').agg(avg('Item_MRP')).display()

# COMMAND ----------

df = spark.read.csv("/Volumes/workspace/default/firststep/BigMart Sales.csv",header=True,inferSchema=True)
df.display()

# COMMAND ----------

#Scenrio - 3
df=df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('Total_MRP')).display()

# COMMAND ----------

df = spark.read.csv("/Volumes/workspace/default/firststep/BigMart Sales.csv",header=True,inferSchema=True)
df.display()

# COMMAND ----------

#Scenrio - 4
df=df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP'),avg('item_MRP')).display()
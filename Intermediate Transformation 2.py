# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.csv("/Volumes/workspace/default/firststep/BigMart Sales.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Handling NULLS
# MAGIC Two Category
# MAGIC 1) Dropping Nulls
# MAGIC 2) Filling Nulls 

# COMMAND ----------

##Dropping Nulls
df = df.dropna('any').display()

# COMMAND ----------

df = spark.read.csv("/Volumes/workspace/default/firststep/BigMart Sales.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.dropna(subset=['Outlet_Size']).display()

# COMMAND ----------

df = spark.read.csv("/Volumes/workspace/default/firststep/BigMart Sales.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

df.fillna('NotAvailable').display(df)

# COMMAND ----------

 df = df.fillna(
    'NotAvailable',
    subset=['Outlet_Size']
)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##SPLIT and Indexing 

# COMMAND ----------

# MAGIC %md
# MAGIC Split

# COMMAND ----------

from pyspark.sql.functions import split, col
df = spark.read.csv("/Volumes/workspace/default/firststep/BigMart Sales.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn('Outlet_Type',split('Outlet_Type',' ')[1]).display()
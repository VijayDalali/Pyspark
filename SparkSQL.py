# Databricks notebook source
df = spark.read.csv("/Volumes/workspace/default/firststep/BigMart Sales.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### create temp view

# COMMAND ----------

df.createTempView('my_view1')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from my_view where Item_Fat_Content = 'LF'
# MAGIC
# MAGIC --Here are the DataFrames in the session:

# COMMAND ----------

df_sql = spark.sql("select * from my_view1 where Item_Fat_Content = 'LF'")
df_sql.display()

# COMMAND ----------


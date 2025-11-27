# Databricks notebook source
# MAGIC %md
# MAGIC ####DATA WRITING 

# COMMAND ----------

df = spark.read.csv("/Volumes/workspace/default/firststep/BigMart Sales.csv",header=True,inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC CSV

# COMMAND ----------

# MAGIC %md
# MAGIC SAVE

# COMMAND ----------

df.write.format('csv')\
    .save('/Volumes/workspace/default/firststep/Sales')

# COMMAND ----------

# MAGIC %md
# MAGIC APPEND

# COMMAND ----------

df.write.format('csv')\
    .mode('append')\
    .save('/Volumes/workspace/default/firststep/BigMart_Sales')

# COMMAND ----------

# MAGIC %md
# MAGIC overwrite

# COMMAND ----------

df.write.format('csv')\
    .mode('overwrite')\
    .save('/Volumes/workspace/default/firststep/BigMart_Sales')

# COMMAND ----------

# MAGIC %md
# MAGIC error

# COMMAND ----------

df.write.format('csv')\
    .mode('error')\
    .save('/Volumes/workspace/default/firststep/BigMart_Sales')

# COMMAND ----------

# MAGIC %md
# MAGIC ignore

# COMMAND ----------

quet df.write.format('csv')\
    .mode('ignore')\
    .save('/Volumes/workspace/default/firststep/BigMart_Sales')

# COMMAND ----------

# MAGIC %md
# MAGIC Parquet file format 

# COMMAND ----------

df.write.format('parquet')\
    .mode('overwrite')\
    .save('/Volumes/workspace/default/firststep/BigMart_Sales')

# COMMAND ----------

# MAGIC %md
# MAGIC Table

# COMMAND ----------

df.write.format('delta')\
    .mode('overwrite')\
    .saveAsTable('my_table')
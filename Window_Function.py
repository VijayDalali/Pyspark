# Databricks notebook source
# MAGIC %md
# MAGIC ##WINDOW_FUNCTIONS

# COMMAND ----------

df = spark.read.csv("/Volumes/workspace/default/firststep/BigMart Sales.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####ROW NUMBER()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import sum, col, rank, dense_rank

# COMMAND ----------

df.withColumn('rowCol',row_number().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####RANK

# COMMAND ----------

df.withColumn('rank',rank().over(Window.orderBy(col('Item_Identifier'))))\
    .withColumn('denseRank',dense_rank().over(Window.orderBy(col('Item_Identifier')))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Cumulative_SUM

# COMMAND ----------

df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type'))).display()
     

# COMMAND ----------

df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()

# COMMAND ----------

df.withColumn('totalsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing))).display()
     
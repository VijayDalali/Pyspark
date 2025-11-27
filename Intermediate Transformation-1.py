# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.csv("/Volumes/workspace/default/firststep/BigMart Sales.csv",header=True,inferSchema=True)


# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Date_Functions
# MAGIC current_date()\
# MAGIC date_add\
# MAGIC date_sub

# COMMAND ----------

from pyspark.sql.functions import current_date
df = spark.read.csv("/Volumes/workspace/default/firststep/BigMart Sales.csv",header=True,inferSchema=True)


# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###CurrentDate

# COMMAND ----------

###CurrentDate
df = df.withColumn('current_date',current_date())
df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC add_date
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import date_add, current_date

df = df.withColumn(
    'week_after',
    date_add(current_date(), 7)
)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Date_sub

# COMMAND ----------

df = df.withColumn("week_before", date_add(current_date(), -7)); display()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date Diff

# COMMAND ----------

from pyspark.sql.functions import datediff

# COMMAND ----------

df = df.withColumn('date_diff',datediff('current_date','week_after'))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date Format

# COMMAND ----------

from pyspark.sql.functions import date_format

# COMMAND ----------

df.display()

# COMMAND ----------

df =df.withColumn(
    'week_before',
    date_format(col('week_after'), 'dd-MM-yyyy')
)
display(df)

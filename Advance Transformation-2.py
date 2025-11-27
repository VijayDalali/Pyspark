# Databricks notebook source
df = spark.read.csv("/Volumes/workspace/default/firststep/BigMart Sales.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##PIVOT
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import avg

# COMMAND ----------

df.groupBy('Item_Type') \
  .pivot('Outlet_Size') \
  .agg(avg('Item_MRP')) \
  .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Collect_List

# COMMAND ----------

from pyspark.sql.functions import collect_list
data = [('user1','book1'),
        ('user1','book2'),
        ('user2','book2'),
        ('user2','book4'),
        ('user3','book1')]

schema = 'user string, book string'

df_book = spark.createDataFrame(data,schema)

df_book.display()

# COMMAND ----------

df_book.groupBy('user').agg(collect_list('book')).display()

# COMMAND ----------

from pyspark.sql.functions import when, col
df = spark.read.csv("/Volumes/workspace/default/firststep/BigMart Sales.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## When-Otherwise
# MAGIC

# COMMAND ----------

#Scenrio-1
df_new = df.withColumn('veg_flag',when(col('Item_Type')=='Meat','non_veg').otherwise('veg'))
df_new.display()

# COMMAND ----------

#Scenrio- 2
df = df_new.withColumn(
    'veg_exp_flag',
    when(
        (col('veg_flag') == 'veg') & (col('Item_MRP') < 100),
        'Veg_Inexpensive'
    ).when(
        (col('veg_flag') == 'veg') & (col('Item_MRP') > 100),
        'Veg_expensive'
    ).otherwise('non_veg')
)
display(df)


# COMMAND ----------


# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.csv("/Volumes/workspace/default/firststep/BigMart Sales.csv",header=True,inferSchema=True)


# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##String Function 
# MAGIC initcap()\
# MAGIC upper()\
# MAGIC lower()

# COMMAND ----------

#initcap
df.select(initcap('Item_Type').alias('initcap_item_Type')).display()
# lower
df.select(lower('Item_Type').alias('lower_item_Type')).display()
# upper
df.select(upper('Item_Type').alias('upper_item_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select
# MAGIC calling Col

# COMMAND ----------


 df.select(col("item_identifier"),col("item_weight"),col("item_fat_content")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ALIAS

# COMMAND ----------

df.select(col("item_identifier").alias("item_ID")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter
# MAGIC Scenrio 1
# MAGIC Filter the data with fat content = Regular 

# COMMAND ----------

df.filter(
    col("item_fat_content")=='Regular'
        ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenrio 2
# MAGIC Slice the data with item_type = Soft drinks and weight less than 10 

# COMMAND ----------

df.filter((col('item_type') == 'Soft Drinks') & (col('item_weight')<10)).display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenrio 3
# MAGIC Fetch the data with Tier n (Tier1 or Tier 2) and Outlet Size is Null

# COMMAND ----------

 df.filter((col('outlet_size').isNull()) & (col('outlet_location_type').isin('Tier 1','Tier 2'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## withColRenamed

# COMMAND ----------

df.withColumnRenamed('Item_weight','item_wt').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####withColumn
# MAGIC Scenrio 1\
# MAGIC new column 

# COMMAND ----------

df = df.withColumn('status',lit('Success')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC multiply and adding a new column 

# COMMAND ----------

df = spark.read.csv(
    "/Volumes/workspace/default/firststep/BigMart Sales.csv",
    header=True,
    inferSchema=True
)
df = df.withColumn(
    'multiply',
    col('item_weight') * col('item_mrp')
)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC scenrio 2 
# MAGIC edit the column without add a new column

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace

df = spark.read.csv(
    "/Volumes/workspace/default/firststep/BigMart Sales.csv",
    header=True,
    inferSchema=True
)

df = df.withColumn(
    "Item_fat_content",
    regexp_replace(
        col("Item_fat_content"),
        "Low Fat",
        "LF"
    )
)

df = df.withColumn(
    "Item_fat_content",
    regexp_replace(
        col("Item_fat_content"),
        "Regular",
        "reg"
    )
)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Type Casting

# COMMAND ----------

df = spark.read.csv(
    "/Volumes/workspace/default/firststep/BigMart Sales.csv",
    header=True,
    inferSchema=True
)

# COMMAND ----------

df=df.withColumn(
    'item_weight',
    col("item_weight").cast(StringType())
)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##SORT
# MAGIC scenrio 1
# MAGIC keep the column on descending order 

# COMMAND ----------

df.sort(col('item_weight').desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC scenrio 2 
# MAGIC Keep the column on ascending order 

# COMMAND ----------

df.sort(col("Item_Visibility").asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Scenrio 3
# MAGIC sort based on multiple column

# COMMAND ----------

df.sort(["item_weight","item_Visibility"], ascending=[0, 0]).display()


# COMMAND ----------

# MAGIC %md
# MAGIC scenrio 4 
# MAGIC

# COMMAND ----------

df.sort(["item_weight","Item_Visibility"],ascending=[0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## limit

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC DROP 
# MAGIC Drop Column in table 

# COMMAND ----------

df.drop('Item_Visibility','Item_Type').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop Duplicates

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

# MAGIC %md
# MAGIC drop duplicates on particular columns
# MAGIC

# COMMAND ----------

df.drop_duplicates(subset=['Item_Type']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Union and Union Byname

# COMMAND ----------


data1 = [('1','kad'),
        ('2','sid')]
schema1 = 'id STRING, name STRING' 

df1 = spark.createDataFrame(data1,schema1)

data2 = [('3','rahul'),
        ('4','jas')]
schema2 = 'id STRING, name STRING' 

df2 = spark.createDataFrame(data2,schema2)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

data1 = [('kad','1'),
        ('sid','2')]
schema1 = 'name STRING, id STRING' 

df1 = spark.createDataFrame(data1,schema1)

# COMMAND ----------

df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Union byname 
# MAGIC

# COMMAND ----------

df1.unionByName(df2).display()
# Databricks notebook source
df = spark.read.csv("/Volumes/workspace/default/firststep/BigMart Sales.csv",header=True,inferSchema=True)


# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

## change the schema
my_ddl_schmea = '''
                item_identifier string,
                item_weight string,
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
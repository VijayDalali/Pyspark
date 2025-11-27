# Databricks notebook source
# MAGIC %md
# MAGIC ##JOINS
# MAGIC

# COMMAND ----------

dataj1 = [('1','gaur','d01'),
          ('2','kit','d02'),
          ('3','sam','d03'),
          ('4','tim','d03'),
          ('5','aman','d05'),
          ('6','nad','d06')] 

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 

df1 = spark.createDataFrame(dataj1,schemaj1)

dataj2 = [('d01','HR'),
          ('d02','Marketing'),
          ('d03','Accounts'),
          ('d04','IT'),
          ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####INNER JOIN

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'inner').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####LEFT JOIN

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'left').display()


# COMMAND ----------

# MAGIC %md
# MAGIC ##RIGHT JOIN

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'right').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##ANTI JOIN

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'anti').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## OUTER JOIN

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'outer').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##FULL JOIN

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'full').display()
# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
schema = StructType([
    StructField('year', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('percent', FloatType(), True),
    StructField('sex', StringType(), True)
])

# schema = 'year INTEGER, name STRING, percent FLOAT, sex STRING'

# COMMAND ----------

df = spark.read.format('csv')\
    .option('header', True)\
    .schema(schema=schema)\
    .load('/Volumes/workspace/default/baby_names')

# COMMAND ----------

from pyspark.sql.functions import concat, lit, round
# df.filter(df.sex == 'boy').display()
df.filter(df.sex == 'boy').groupBy('year').agg({'percent': 'sum'})
df.withColumn('percent', concat((round(df.percent*100, 2)).cast('string'), lit('%'))).display()
# df.display()
df.write.mode("overwrite").saveAsTable("baby_name_tb")

# COMMAND ----------

listFile = dbutils.fs.ls('/Volumes/workspace/default/baby_names')
df = spark.createDataFrame(listFile).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from baby_name_tb

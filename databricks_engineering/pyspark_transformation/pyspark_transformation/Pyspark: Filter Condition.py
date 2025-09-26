# Databricks notebook source
# MAGIC %md
# MAGIC > ## Filter
# MAGIC ### To limit the data result or extract a subset of master data
# MAGIC - `filter(df.column != 50)`
# MAGIC - `filter((col(column1) > 50) & (col(column2) > 50))`
# MAGIC - `filter((col(column1) > 50) | (col(column2) > 50))`
# MAGIC - `filter(df.column.isNull())`
# MAGIC - `filter(df.column.isNotNull())`
# MAGIC - `filter(df.column.like('%%'))`
# MAGIC - `filter(df.name.isin())`
# MAGIC - `filter(df.column.contains(''))`
# MAGIC - `filter(df.column.startswith(''))`
# MAGIC - `filter(df.column.endswith(''))`

# COMMAND ----------

employee_data = [
    (10, "Raj Kumar", "1999", "100", "M", 2000),
    (20, "Rahul Ranjan", "2002", "200", "F", 8000),
    (30, "Raghav", "2010", "100", None, 6000),
    (40, "Raja Singh", "2011", "100", "F", 7000),
    (50, "Rama Krish", "2008", "400", "F", 5000),
    (60, "Rasul", "2004", "500", "M", 3000),
    (70, "Kumar Chand", "2005", "600", "M", 9000),
]

employee_schema = 'employee_id INTEGER, name STRING, doj STRING, dept_id STRING, gender STRING, salary INTEGER'
employeeDF = spark.createDataFrame(employee_data, schema = employee_schema)
employeeDF.display()

# COMMAND ----------

employeeDF.filter(employeeDF['salary'] >= 5000).display()

# COMMAND ----------

from pyspark.sql.functions import col
employeeDF.filter((col('salary') >= 5000) & (col('gender') == 'F')).display()

# COMMAND ----------

employeeDF.filter(col('gender').isNull()).display()
employeeDF.na.fill("#NA").display()
employeeDF.fillna("#NA").display()
employeeDF.filter(col('salary').isNotNull()).display()

# COMMAND ----------

from pyspark.sql.functions import upper
employeeDF.filter(upper(col('name')).like('%H')).display()

# COMMAND ----------

employeeDF.filter(col('dept_id').isin(['100', '200'])).display()
employeeDF.filter(~col('dept_id').isin(['100', '200'])).display()

# COMMAND ----------

employeeDF.filter(col('name').contains('R')).display()

# COMMAND ----------

employeeDF.filter(col('name').startswith('Raj')).display()
employeeDF.filter(col('name').endswith('Singh')).display()

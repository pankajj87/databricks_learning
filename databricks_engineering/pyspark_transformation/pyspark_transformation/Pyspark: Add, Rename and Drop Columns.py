# Databricks notebook source
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

# MAGIC %md
# MAGIC #### Adding new column using constant literal 

# COMMAND ----------

from pyspark.sql.functions import lit
employeeDF = employeeDF.withColumn('bonus', lit(1000))
employeeDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Adding new column by calculation

# COMMAND ----------

from pyspark.sql.functions import concat, upper
employeeDF = employeeDF.withColumn('total_salary', employeeDF.salary + employeeDF.bonus).withColumn('emplname_id', upper(concat(employeeDF.name, lit('_'), employeeDF.employee_id)))
employeeDF.display()

# COMMAND ----------

employeeDF = employeeDF.withColumnRenamed('name', 'full_name')
employeeDF.show()

# COMMAND ----------

employeeDF = employeeDF.drop('bonus')
employeeDF.show()

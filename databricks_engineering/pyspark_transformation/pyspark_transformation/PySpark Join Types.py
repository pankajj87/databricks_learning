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

department_data = [
    ("HR", 100),
    ("Supply", 200),
    ("Sales", 300),
    ("Stock", 400),
    ("Finance", 500)
]

department_schema = 'dept_name STRING, dept_id INTEGER'
departmentDF = spark.createDataFrame(department_data, schema = department_schema)
departmentDF.display()

# COMMAND ----------



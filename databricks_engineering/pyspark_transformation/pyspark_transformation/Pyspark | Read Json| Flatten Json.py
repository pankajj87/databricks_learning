# Databricks notebook source
df = spark.read.option('multiline', 'true').format('json').load('/Volumes/workspace/default/employee_json/employee_json.json')
# df.schema.fields

# COMMAND ----------

from pyspark.sql.functions import explode, col
df1 = df.select(explode(col("Employee")).alias("Employee"))

df2 = df1.select(
    col("Employee.emp_id"),
    col("Employee.Designation"),
    explode(col("Employee.attribute")).alias("attr")
)

df3 = df2.select(
    "emp_id",
    "Designation",
    col("attr.Parent_id").alias("Parent_id"),
    col("attr.status_flag").alias("status_flag"),
    explode(col("attr.Department")).alias("Department")
)

df4 = df3.select(
    "emp_id",
    "Designation",
    "Parent_id",
    "status_flag",
    col("Department.Code").alias("Dept_Code"),
    col("Department.Dept_id").alias("Dept_id"),
    col("Department.dept_flag").alias("Dept_flag"),
    col("Department.dept_type").alias("Dept_type")
)

df4.display()


# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# Flatten array of structs and structs
def flatten(df):
    # compute Complex Fields (Lists and Structs) in Schema   
    complex_fields = dict([
            (field.name, field.dataType)
            for field in df.schema.fields
            if type(field.dataType) == ArrayType or  type(field.dataType) == StructType
        ])
    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        print ("Processing :" + col_name + " Type : " + str(type(complex_fields[col_name])))

        # if StructType then convert all sub element to columns.
        # i.e. flatten structs
        if (type(complex_fields[col_name]) == StructType):
            expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [ n.name for n in  complex_fields[col_name]]]
            df = df.select("*", *expanded).drop(col_name)

        # if ArrayType then add the Array Elements as Rows using the explode function
        # i.e. explode Arrays
        elif (type(complex_fields[col_name]) == ArrayType):    
            df = df.withColumn(col_name,explode_outer(col_name))

        # recompute remaining Complex Fields in Schema       
        complex_fields = dict([
                (field.name, field.dataType)
                for field in df.schema.fields
                if type(field.dataType) == ArrayType or  type(field.dataType) == StructType
            ])
        
    return df

# COMMAND ----------

df_flatten = flatten(df)
df_flatten.display()

# COMMAND ----------

# flattened_df = df.withColumn("nested_field", explode(col("Employee")))
# flattened_df.display()
selected_df = df.select(
    col("Employee")
)

for data in selected_df.collect():
    print(data)

# selected_df.display()

# Databricks notebook source
# DBTITLE 1,How to list file dbutils.fs.ls
display(dbutils.fs.ls("/Volumes/dbr_practice_monish/practice_schema/practice_volume/csv_files/"))

# COMMAND ----------

# DBTITLE 1,Very simple read csv
df=spark.read.csv("/Volumes/dbr_practice_monish/practice_schema/practice_volume/csv_files/sales.csv")

df.show()

# COMMAND ----------

# DBTITLE 1,df.show() and df.display()
# | Feature                       | `df.show()`                  | `display(df)`                              |
# | ----------------------------- | ---------------------------- | ------------------------------------------ |
# | Type                          | PySpark method               | Databricks notebook utility                |
# | Output                        | Text-based table             | Rich, interactive table                    |
# | Default rows                  | First 20 rows                | Databricks-controlled display              |
# | Interactive sorting/filtering | No                           | Available in supported notebook interfaces |
# | Charts and visualizations     | No                           | Yes, in supported notebook environments    |
# | Works outside Databricks      | Yes, in PySpark environments | Generally Databricks-specific              |
# | Return value                  | `None`                       | Notebook display output, not a DataFrame   |

# COMMAND ----------

# DBTITLE 1,Use Format
df=spark.read.format("csv").load("/Volumes/dbr_practice_monish/practice_schema/practice_volume/csv_files/sales.csv")

display(df)

# COMMAND ----------

# DBTITLE 1,Use options and \
df = spark.read\
    .option("header", True)\
    .format("csv")\
    .load("/Volumes/dbr_practice_monish/practice_schema/practice_volume/csv_files/sales.csv")

display(df)

# COMMAND ----------

# DBTITLE 1,Best way to raed csv
df = (
    spark.read
    .option("header", True)
    .format("csv")
    .load("/Volumes/dbr_practice_monish/practice_schema/practice_volume/csv_files/sales.csv")
)

display(df)

# COMMAND ----------

# DBTITLE 1,.option("inferSchema",True)
df = (
    spark.read
    .option("header", True)
    .option("inferSchema",True)
    .format("csv")
    .load("/Volumes/dbr_practice_monish/practice_schema/practice_volume/csv_files/sales.csv")
)

display(df)

# COMMAND ----------

# DBTITLE 1,Schema Inference vs Schema Enforcement
# When you load a dataset, Spark can automatically infer the schema by scanning the data. While this seems convenient, it’s not always ideal.
# Sampling Ratio: Instead of scanning the entire dataset, inferring the schema based on the ratio provided.
# Problems with Schema Inference
# Schema might be incorrectly inferred due to inconsistent data types.
# Spark has to scan the data to infer schema, which adds unnecessary overhead and slows down performance.
# That’s why it’s generally a good idea to enforce your schema explicitly rather than relying on inference.

# COMMAND ----------

# DBTITLE 1,.option("samplingRatio", .1)  means 10%
# df = spark.read \
# .format("csv") \
# .option("header", "true") \
# .option("inferSchema", "true") \
# .option("samplingRatio", .1) \
# .load("")

# COMMAND ----------

# DBTITLE 1,Why is samplingRatio useful?
# Why is samplingRatio useful?
# - Lower ratio: May reduce the amount of data examined for schema inference, but can miss unusual values.
# - Higher ratio: Gives Spark more data to examine, potentially improving the inferred schema.
# - Ratio of 1.0: Requests full sampling for inference.

# COMMAND ----------

# DBTITLE 1,Problems with Schema Inference

# Schema might be incorrectly inferred due to inconsistent data types.
# Spark has to scan the data to infer schema, which adds unnecessary overhead and slows down performance.
# For example, suppose a column contains mostly integers but has a decimal value in a later row. A small sample might infer an integer type, while a larger sample may reveal that a wider numeric type is needed.
# That’s why it’s generally a good idea to enforce your schema explicitly rather than relying on inference.

# COMMAND ----------

# DBTITLE 1,Two Ways to Enforce Schema
### 1. Using Schema DDL
# - Define the schema
# - Pass the schema while reading the file

# COMMAND ----------

# DBTITLE 1,Craete schema ""
def_schema="\
id INT,\
customer_name STRING,\
product STRING,\
quantity INT,\
price DOUBLE,\
remarks STRING\
"

# COMMAND ----------

# DBTITLE 1,Use of  .schema(def_schema)
df=(
    spark.read
    .schema(def_schema)
    .format("csv")
    .load("/Volumes/dbr_practice_monish/practice_schema/practice_volume/csv_files/sales_no_header.csv")
)

df.display()

# COMMAND ----------

# DBTITLE 1,Using StructType

# - Define the schema
# - Pass the schema while reading the file

# COMMAND ----------

# DBTITLE 1,from pyspark.sql.types import
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType

# COMMAND ----------

# DBTITLE 1,Use of StructType([])
def_schema_1=StructType([
StructField("id",IntegerType(),True),
StructField("customer_name",StringType(),True),
StructField("product",StringType(),True),
StructField("quantity", IntegerType(), True),
StructField("price",DoubleType(), True),
StructField("remarks",StringType(),True)
])

# COMMAND ----------

# DBTITLE 1,Use of StructType([StructField(id,IntegerType(),True)])
df=(
    spark.read 
    .schema(def_schema_1)
    .format("csv")
    .load("/Volumes/dbr_practice_monish/practice_schema/practice_volume/csv_files/sales_no_header.csv")
)

df.display()

# COMMAND ----------

# DBTITLE 1,Use of delimiter(without)
df=(
    spark.read 
    .option("inferSchema",True)
    .option("header",True)
    .format("csv")
    .load("/Volumes/dbr_practice_monish/practice_schema/practice_volume/csv_files/sales_pipe.csv")
)
df.display()

# COMMAND ----------

# DBTITLE 1,Use of delimiter
df=(
    spark.read 
    .option("inferSchema",True)
    .option("header",True)
    .option("delimiter","|")
    .format("csv")
    .load("/Volumes/dbr_practice_monish/practice_schema/practice_volume/csv_files/sales_pipe.csv")
)
df.display()

# COMMAND ----------

# DBTITLE 1,Use of  .option("quote", "'") by default spark consider "" as quote
df=(
    spark.read 
    .option("inferSchema",True)
    .option("header",True)
    .option("delimiter","|")
    .format("csv")
    .load("/Volumes/dbr_practice_monish/practice_schema/practice_volume/csv_files/sales_pipe_single_quote.csv")
)
df.display()

# COMMAND ----------

# DBTITLE 1,Use of  .option("quote", "'")
df=(
    spark.read 
    .option("inferSchema",True)
    .option("header",True)
    .option("delimiter","|")
    .option("quote", "'")
    .format("csv")
    .load("/Volumes/dbr_practice_monish/practice_schema/practice_volume/csv_files/sales_pipe_single_quote.csv")
)
df.display()

# COMMAND ----------

# DBTITLE 1,Use of  .option("escape","#")
df=(
    spark.read 
    .option("inferSchema",True)
    .option("header",True)
    .option("delimiter","|")
    .option("quote", "'")
    .option("escape","#")
    .format("csv")
    .load("/Volumes/dbr_practice_monish/practice_schema/practice_volume/csv_files/sales_pipe_single_quote_escape.csv")
)
df.display()

# COMMAND ----------

df=(
    spark.read 
    .option("inferSchema",True)
    .option("header",True)
    .option("delimiter","|")
    .option("quote", "'")
    .option("escape","#")
    .option("multiline",True)
    .format("csv")
    .load("/Volumes/dbr_practice_monish/practice_schema/practice_volume/csv_files/sales_pipe_single_quote_escape_multiline.csv")
)
df.display()
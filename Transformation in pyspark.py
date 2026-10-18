# Databricks notebook source
# DBTITLE 1,Create df - pyspark
df=spark.read.table("samples.bakehouse.sales_franchises")
display(df)

# COMMAND ----------

# DBTITLE 1,Select -SQL
# MAGIC %sql
# MAGIC select * from samples.bakehouse.sales_franchises

# COMMAND ----------

# DBTITLE 1,Get Columns -pyspark
df.columns

# COMMAND ----------

# DBTITLE 1,Get columns SQL
# MAGIC %sql
# MAGIC show columns in samples.bakehouse.sales_franchises

# COMMAND ----------

# DBTITLE 1,Select -Pyspark
new_df=df.select("name","country")
display(new_df)

# COMMAND ----------

# DBTITLE 1,Select SQL
# MAGIC %sql
# MAGIC select name , country from samples.bakehouse.sales_franchises

# COMMAND ----------

# DBTITLE 1,Expr pyspark
from pyspark.sql.functions import expr
new_df=df.select("name",expr("country as new_country"))
new_df.display()

# COMMAND ----------

# DBTITLE 1,expr("concat(city,'-',country) as location")
from pyspark.sql.functions import expr
new_df=df.select("name",expr("concat(city,'-',country) as location"))
new_df.display()

# COMMAND ----------

# DBTITLE 1,selectExpr
from pyspark.sql.functions import expr
new_df=df.selectExpr("name","country as new_country")
new_df.display()

# COMMAND ----------

# DBTITLE 1,alias
from pyspark.sql.functions import expr

new_df=df.select("name",expr("concat(city,'-','country')").alias("location"))

display(new_df)

# COMMAND ----------

# DBTITLE 1,Add new column(withColumn)  pyspark
new_df=df.withColumn("address",expr("concat(city,'-',district,'-',country,'-',zipcode)"))
display(new_df)

# COMMAND ----------

# DBTITLE 1,Use of lit - pyspark
from pyspark.sql.functions import lit
new_df=df.withColumn("region",lit("north east"))
display(new_df)

# COMMAND ----------

# DBTITLE 1,add new column SQL
# MAGIC %sql
# MAGIC select *,"noth east" as region from samples.bakehouse.sales_franchises

# COMMAND ----------

# DBTITLE 1,Drop column pyspark
new_df=df.drop("country")
display(new_df)

# COMMAND ----------

# DBTITLE 1,Drop column SQL
# MAGIC %sql
# MAGIC select * from samples.bakehouse.sales_franchises where country is not null

# COMMAND ----------

# DBTITLE 1,Cast pyspark
from pyspark.sql.functions import col
df_new=df.withColumn("supplierID_double", col("supplierID").cast("double"))
display(df_new)



# COMMAND ----------

# DBTITLE 1,Cast SQL
# MAGIC %sql
# MAGIC select *, cast(supplierID as double) from samples.bakehouse.sales_franchises

# COMMAND ----------

# DBTITLE 1,filter pyspark
new_df=df.filter(col("country") == "US")
display(new_df)


# COMMAND ----------

# DBTITLE 1,where -pyspark
new_df1=df.where("country=='US'")
display(new_df1)


# COMMAND ----------

# DBTITLE 1,where SQL
# MAGIC %sql
# MAGIC select * from samples.bakehouse.sales_franchises where country='US'

# COMMAND ----------

# DBTITLE 1,distinct pyspark
new_df=df.distinct()
display(new_df)
new_df=df.select("country").distinct()
display(new_df)
new_df=df.select("country","city").distinct()
display(new_df)

# COMMAND ----------

# DBTITLE 1,union pyspark
df2=df

df3=df.union(df2)
df.display(df3)

# COMMAND ----------

# DBTITLE 1,union pyspark
df2=df

df3=df.unionAll(df2)
df.display(df3)

# COMMAND ----------

# DBTITLE 1,union SQL
# MAGIC %sql
# MAGIC select * from samples.bakehouse.sales_franchises
# MAGIC union
# MAGIC select * from samples.bakehouse.sales_franchises

# COMMAND ----------

# DBTITLE 1,union all SQL
# MAGIC %sql
# MAGIC select * from samples.bakehouse.sales_franchises
# MAGIC union all
# MAGIC select * from samples.bakehouse.sales_franchises
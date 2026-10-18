# Databricks notebook source
# DBTITLE 1,Create Catalog
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS dbr_practice_monish;

# COMMAND ----------

# DBTITLE 1,Create Schema
# MAGIC %sql
# MAGIC create schema if not exists dbr_practice_monish.practice_schema

# COMMAND ----------

# DBTITLE 1,Create Volume
# MAGIC %sql
# MAGIC create volume if not exists dbr_practice_monish.practice_schema.practice_volume
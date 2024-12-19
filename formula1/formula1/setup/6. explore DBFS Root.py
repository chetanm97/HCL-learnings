# Databricks notebook source
dbutils.fs.ls('/FileStore/circuits.csv')

# COMMAND ----------

display(spark.read.csv('/FileStore/circuits.csv'))

# COMMAND ----------


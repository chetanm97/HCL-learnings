# Databricks notebook source
# MAGIC %md 
# MAGIC ###### Access azure data lake using access keys
# MAGIC 1. Set teh spark config fs.azure.accont.key
# MAGIC 2. list files for demo containers
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

dbutils.secrets.listScopes()
dbutils.secrets.list(scope = 'formula1-scope')
formula1dl_account_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'accesskeysecret')
spark.conf.set(
    "fs.azure.account.key.chetan1.dfs.core.windows.net",formula1dl_account_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@chetan1.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@chetan1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


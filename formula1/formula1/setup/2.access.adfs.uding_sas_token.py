# Databricks notebook source
# MAGIC %md 
# MAGIC ###### Access azure data lake using access keys
# MAGIC 1. Set teh spark config sas token
# MAGIC 2. list files for demo containers
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

dbutils.secrets.listScopes()
dbutils.secrets.list(scope = 'formula1-scope')
formula1dl_sas_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'sastoken')
spark.conf.set("fs.azure.account.auth.type.chetan1.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.chetan1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.chetan1.dfs.core.windows.net", formula1dl_sas_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@chetan1.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@chetan1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


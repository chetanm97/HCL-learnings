# Databricks notebook source
dbutils.secrets.listScopes()
dbutils.secrets.list(scope = 'formula1-scope')

# COMMAND ----------


formula1dl_account_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'accesskeysecret')

# COMMAND ----------

formula1dl_account_key


# COMMAND ----------


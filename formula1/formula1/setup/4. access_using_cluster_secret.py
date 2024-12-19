# Databricks notebook source
display(dbutils.fs.ls("abfss://demo@chetan1.dfs.core.windows.net/"))


# add it in cluster config fs.azure.account.key.chetan1.dfs.core.windows.net {{secrets/formula1-scope/accesskeysecret}}

# COMMAND ----------


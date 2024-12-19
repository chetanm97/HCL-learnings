# Databricks notebook source
client_id = "client-id-for-service-principle"
tenant_id = "tenant-id-for-service-principle"
client_secret = "client-secret-for-service-principle"

# COMMAND ----------

dbutils.secrets.listScopes()
dbutils.secrets.list(scope = 'formula1-scope')
client_id = dbutils.secrets.get(scope = 'formula1-scope', key = client_id)
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = tenant_id)
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = client_secret)


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
dbutils.fs.mount(
  source = "abfss://demo@chetan1.dfs.core.windows.net/",
  mount_point = "/mnt/chetan1/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/chetan1/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/chetan1/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------


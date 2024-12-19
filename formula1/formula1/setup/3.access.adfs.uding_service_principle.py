# Databricks notebook source
# MAGIC %md 
# MAGIC #### Access azure data lake using service principle
# MAGIC 1. Register Azure AD Application/ service principle
# MAGIC 2. Generate a secret/ password for the Application
# MAGIC 3. Set Spark Config with App/Client Id, direactory / Tenant D & Secret
# MAGIC 4. Assign Role 'Storagr Blob Data Contributer' to the data Lake

# COMMAND ----------

client_id = "client-id-for-service-principle"
tenant_id = "tenant-id-for-service-principle"
client_secret = "client-secret-for-service-principle"

# COMMAND ----------

dbutils.secrets.listScopes()
dbutils.secrets.list(scope = 'formula1-scope')
client_id = dbutils.secrets.get(scope = 'formula1-scope', key = client_id)
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = tenant_id)
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = client_secret)
spark.conf.set("fs.azure.account.auth.type.chetan1.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.chetan1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.chetan1.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.chetan1.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.chetan1.dfs.core.windows.net", "https://login.microsoftonline.com/{}/oauth2/token".format(tenant_id))

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@chetan1.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@chetan1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


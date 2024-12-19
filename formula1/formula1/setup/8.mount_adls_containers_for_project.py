# Databricks notebook source
def mount_adls(storage_account_name,container_name):
    client_id = "client-id-for-service-principle"
    tenant_id = "tenant-id-for-service-principle"
    client_secret = "client-secret-for-service-principle"

    client_id = dbutils.secrets.get(scope = 'formula1-scope', key = client_id)
    tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = tenant_id)
    client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = client_secret)
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    configs = {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": client_id,
            "fs.azure.account.oauth2.client.secret": client_secret,
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    dbutils.fs.mount(
    source = "abfss://{}@{}.dfs.core.windows.net/".format(container_name, storage_account_name),
    mount_point = "/mnt/{}/{}".format( storage_account_name, container_name),
    extra_configs = configs)
    display(dbutils.fs.mounts())
mount_adls('chetan1','raw')
mount_adls('chetan1','processed')
mount_adls('chetan1','presentation')

# COMMAND ----------

mount_adls('chetan1','raw')
mount_adls('chetan1','processed')
mount_adls('chetan1','presentation')
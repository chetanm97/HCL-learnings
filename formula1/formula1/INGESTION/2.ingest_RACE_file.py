# Databricks notebook source
# MAGIC %md
# MAGIC Ingest ciecuits.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../include/configuration"

# COMMAND ----------

##dbutils.fs.ls('/mnt/chetan1/raw/')
# def mount_adls(storage_account_name,container_name):
#     client_id = "client-id-for-service-principle"
#     tenant_id = "tenant-id-for-service-principle"
#     client_secret = "client-secret-for-service-principle"

#     client_id = dbutils.secrets.get(scope = 'formula1-scope', key = client_id)
#     tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = tenant_id)
#     client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = client_secret)
#     if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
#         dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

#     configs = {"fs.azure.account.auth.type": "OAuth",
#             "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
#             "fs.azure.account.oauth2.client.id": client_id,
#             "fs.azure.account.oauth2.client.secret": client_secret,
#             "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
#     dbutils.fs.mount(
#     source = "abfss://{}@{}.dfs.core.windows.net/".format(container_name, storage_account_name),
#     mount_point = "/mnt/{}/{}".format( storage_account_name, container_name),
#     extra_configs = configs)
#     display(dbutils.fs.mounts())
# mount_adls('chetan1','raw')
# mount_adls('chetan1','processed')

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType,DateType
from pyspark.sql.functions import col
races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True) 
])

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit
races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))\
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), 
                                                   col('round'), col('circuitId').alias('circuit_id'),col('name'), col('ingestion_date'), col('race_timestamp'))

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")
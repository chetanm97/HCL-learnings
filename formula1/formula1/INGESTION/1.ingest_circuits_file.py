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
# MAGIC

# COMMAND ----------

# MAGIC %run "../include/common_functions"

# COMMAND ----------

dbutils.widgets.help()
dbutils.widgets.text('processed_folder_path',"/mnt/chetan1/processed")

# COMMAND ----------

processed_folder_path

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

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col
circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")


# COMMAND ----------

circuits_df.show()

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

circuits_renamed_df.show()

# COMMAND ----------

circuits_final_df =  add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

display(spark.read.parquet("{}/circuits".format(processed_folder_path)))


# COMMAND ----------

dbutils.notebook.exit("Success")
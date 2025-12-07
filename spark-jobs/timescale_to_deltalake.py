from pyspark.sql import SparkSession

# default azurite key found in github's readme.md
spark = SparkSession.builder \
    .appName("TimescaleToDeltaLake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("fs.azure.account.key.devstoreaccount1.blob.core.windows.net", "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==") \
    .config("fs.azure.account.auth.type.devstoreaccount1.blob.core.windows.net", "SharedKey") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://timescaledb:5432/iiot"

connection_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

df_machine_sensors = spark.read.jdbc(
    url=jdbc_url,
    table="machine_sensors",
    properties=connection_properties
)

df_sensor_aggregates = spark.read.jdbc(
    url=jdbc_url,
    table="sensor_aggregates",
    properties=connection_properties
)

# test first before writing

df_machine_sensors.show(5)
df_sensor_aggregates.show(5)

#df_machine_sensors.write \
#    .format("delta") \
#    .mode("overwrite") \
#    .save("wasbs://datalake@devstoreaccount1.blob.core.windows.net/machine_sensors")

#df_sensor_aggregates.write \
#    .format("delta") \
#    .mode("overwrite") \
#    .save("wasbs://datalake@devstoreaccount1.blob.core.windows.net/sensor_aggregates")

spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder \
    .appName("Load_STG_Trx_Summary") \
    .master("yarn") \
    .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33") \
    .enableHiveSupport() \
    .getOrCreate()

jdbc_url = "jdbc:mysql://cdpm1.cloudeka.ai:3306/transaction_channel?useSSL=false&serverTimezone=UTC&connectionTimeZone=Asia/Jakarta"
jdbc_properties = {
    "user": "cloudera",
    "password": "Admin123",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Load trx_atm from MySQL
df_atm = spark.read.jdbc(
    url=jdbc_url,
    table="trx_atm",
    properties=jdbc_properties
).withColumn("channel_type", lit("ATM")) \
 .withColumnRenamed("atm_id", "channel_id") \
 .withColumnRenamed("location_code", "merchant_location")

# Load trx_online from MySQL
df_online = spark.read.jdbc(
    url=jdbc_url,
    table="trx_online",
    properties=jdbc_properties
).withColumn("channel_type", lit("ONLINE")) \
 .withColumnRenamed("device_id", "channel_id") \
 .withColumnRenamed("merchant_code", "merchant_location")

# Load trx_edc from MySQL
df_edc = spark.read.jdbc(
    url=jdbc_url,
    table="trx_edc",
    properties=jdbc_properties
).withColumn("channel_type", lit("EDC")) \
 .withColumnRenamed("edc_id", "channel_id") \
 .withColumnRenamed("merchant_code", "merchant_location")

# Union all DataFrames to unified schema
common_cols = ["trx_id", "trx_date", "account_number", "amount", "channel_type", "channel_id", "merchant_location", "created_at"]
df_union = df_atm.select(*common_cols) \
            .unionByName(df_online.select(*common_cols)) \
            .unionByName(df_edc.select(*common_cols))

# Add partition column
df_union = df_union.withColumn("trx_date_partition", col("trx_date"))

# Write to Hive stg_trx_summary partitioned table
df_union.write \
    .mode("append") \
    .format("parquet") \
    .partitionBy("trx_date_partition") \
    .saveAsTable("stg_trx_summary")

print("✅ Load to Hive stg_trx_summary completed.")

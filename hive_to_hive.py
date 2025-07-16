#!/usr/bin/env python3

import os
import sys
from pyspark.sql import SparkSession

# Configurations
KEYTAB_PATH = "/path/to/your.keytab"
PRINCIPAL = "your_user@YOUR.REALM"
SOURCE_HIVE_TABLE = "source_db.source_table"
TARGET_HIVE_TABLE = "target_db.target_table"

# Target Hive JDBC config
TARGET_HIVE_JDBC_URL = "jdbc:hive2://target-hive-host:10000/target_db;principal=hive/_HOST@YOUR.REALM"
TARGET_HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver"

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("Hive Table Copy") \
    .enableHiveSupport() \
    .getOrCreate()

# Authenticate using Kerberos keytab
print("Authenticating using keytab...")
kinit_cmd = f"kinit -kt {KEYTAB_PATH} {PRINCIPAL}"
exit_code = os.system(kinit_cmd)
if exit_code != 0:
    print("Kerberos authentication failed.")
    sys.exit(1)

# Read from source Hive table
print(f"Reading data from {SOURCE_HIVE_TABLE}...")
source_df = spark.table(SOURCE_HIVE_TABLE)

# Write to target Hive using JDBC
print(f"Writing data to {TARGET_HIVE_TABLE} on target cluster...")
source_df.write \
    .format("jdbc") \
    .option("url", TARGET_HIVE_JDBC_URL) \
    .option("driver", TARGET_HIVE_DRIVER) \
    .option("dbtable", TARGET_HIVE_TABLE) \
    .option("user", "hive") \
    .mode("overwrite") \
    .save()

print("Data transfer completed successfully.")
spark.stop()

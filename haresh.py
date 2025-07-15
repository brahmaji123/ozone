from pyspark.sql import SparkSession

def main():
    # Initialize Spark Session with Kerberos authentication for both clusters
    spark = SparkSession.builder \
        .appName("CrossClusterHiveTransfer") \
        # Source cluster configurations (if needed)
        .config("spark.hadoop.hive.metastore.uris", "thrift://source-metastore-server:9083") \
        # Target cluster configurations
        .config("spark.yarn.keytab", "/path/to/your.keytab") \
        .config("spark.yarn.principal", "your_principal@YOUR.REALM") \
        .config("hive.metastore.uris", "thrift://target-metastore-server:9083") \
        .config("hive.metastore.sasl.enabled", "true") \
        .config("hive.metastore.kerberos.principal", "hive/_HOST@YOUR.REALM") \
        .enableHiveSupport() \
        .getOrCreate()

    try:
        # Step 1: Read from source Hive table
        print("Reading data from source Hive table...")
        source_df = spark.sql("SELECT * FROM source_db.source_table")
        
        # Optional: Show schema and sample data
        source_df.printSchema()
        source_df.show(5)
        
        # Step 2: Write to target Hive table
        print("Writing data to target Hive table...")
        source_df.write \
            .mode("overwrite") \  # Options: "overwrite", "append", "ignore", "error"
            .saveAsTable("target_db.target_table")
        
        print("Data transfer completed successfully!")
        
    except Exception as e:
        print(f"Error during data transfer: {str(e)}")
        raise

if __name__ == "__main__":
    main()

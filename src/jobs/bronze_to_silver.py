# src/jobs/bronze_to_silver.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_timestamp, when, expr, year, month, dayofmonth, current_timestamp
from delta import configure_spark_with_delta_pip
import logging # For production-level logging

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    logging.info("Starting Bronze to Silver ETL job.")

    # Setup Spark session with Delta
    # Removed .master() and .config("spark.jars.packages")
    builder = SparkSession.builder \
        .appName("BronzeToSilverJob") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.databricks.delta.autoCompact.enabled", "true") \
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Set Spark log level
    spark.sparkContext.setLogLevel("WARN")
    logging.info(f"SparkSession created. Spark version: {spark.version}")

    bronze_path = "s3a://retail-lakehouse/bronze/online_retail_bronze"
    silver_path = "s3a://retail-lakehouse/silver/online_retail_silver"

    try:
        logging.info(f"Reading from Bronze layer: {bronze_path}")
        df_bronze = spark.read.format("delta").load(bronze_path)
        # logging.info(f"Bronze data count: {df_bronze.count()}")
        logging.info("Bronze data schema:")
        df_bronze.printSchema()

        # Data transformations for Silver layer
        df_silver = df_bronze \
            .withColumn("InvoiceNo", trim(col("InvoiceNo"))) \
            .withColumn("StockCode", trim(col("StockCode"))) \
            .withColumn("Description", trim(col("Description"))) \
            .withColumn("Country", trim(col("Country"))) \
            .withColumn("InvoiceDate", to_timestamp("InvoiceDate", "MM/dd/yyyy HH:mm")) \
            .withColumn("TotalPrice", col("Quantity") * col("UnitPrice")) \
            .withColumn("Year", year(col("InvoiceDate"))) \
            .withColumn("Month", month(col("InvoiceDate"))) \
            .withColumn("Day", dayofmonth(col("InvoiceDate"))) \
            .withColumn("IsUKCustomer", when(col("Country") == "United Kingdom", True).otherwise(False)) \
            .withColumn("HighValueOrder", when(col("TotalPrice") > 1000, True).otherwise(False)) \
            .withColumn("BulkOrder", when(col("Quantity") >= 100, True).otherwise(False)) \
            .withColumn("IsValidRecord", when(
                (col("Quantity") > 0) & (col("UnitPrice") > 0) & (col("InvoiceNo").isNotNull()),
                True
            ).otherwise(False)) \
            .dropDuplicates(["InvoiceNo", "StockCode", "CustomerID"]) \
            .withColumn("silver_load_timestamp", current_timestamp()) # Add load timestamp

        logging.info(f"Writing to Silver layer: {silver_path}")
        # 'overwrite' for full refresh of Silver. For incremental, consider 'merge'.
        df_silver.write.format("delta").mode("overwrite").save(silver_path)
        logging.info(f"✅ Silver layer written successfully to: {silver_path}")

    except Exception as e:
        logging.error(f"Error during Bronze to Silver ETL: {e}", exc_info=True)
        raise

    finally:
        spark.stop()
        logging.info("SparkSession stopped for Bronze to Silver.")

if __name__ == "__main__":
    main()
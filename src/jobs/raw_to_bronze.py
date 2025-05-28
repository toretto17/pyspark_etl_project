# File: src/jobs/raw_to_bronze.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
# --- MODIFIED: Removed 'from delta import configure_spark_with_delta_pip' ---
from datetime import datetime
import logging

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    logging.info("Starting Raw to Bronze ETL job.")

    # Setup Spark session manually, relying on spark-submit for JARs/extensions
    builder = SparkSession.builder \
        .appName("RawToBronzeJob") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.autoCompact.enabled", "true") \
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")

    # --- MODIFIED: Changed this line ---
    spark = builder.getOrCreate() # Rely on spark-submit's --jars and --conf for Delta capabilities

    # Set Spark log level - "WARN" is good for production to see warnings but not too much INFO
    spark.sparkContext.setLogLevel("WARN")
    logging.info(f"SparkSession created. Spark version: {spark.version}")

    # File paths (read from MinIO S3A, write to MinIO S3A)
    raw_path = "s3a://retail-lakehouse/raw/online_retail.csv"
    bronze_path = "s3a://retail-lakehouse/bronze/online_retail_bronze"

    # Define schema explicitly for robust CSV reading
    csv_schema = StructType([
        StructField("InvoiceNo", StringType(), True),
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("InvoiceDate", StringType(), True), # Keep as StringType for raw
        StructField("UnitPrice", DoubleType(), True),
        StructField("CustomerID", IntegerType(), True),
        StructField("Country", StringType(), True)
    ])

    try:
        logging.info(f"Reading raw data from: {raw_path}")
        df_raw = spark.read.csv(raw_path, header=True, schema=csv_schema)
        logging.info("Raw data schema:")
        df_raw.printSchema()

        # Data cleaning and validation rules
        df_cleaned = df_raw.dropna(subset=["InvoiceNo", "StockCode", "CustomerID", "Quantity", "UnitPrice"])
        df_cleaned = df_cleaned.filter(
            (col("InvoiceNo").rlike("^[0-9]+$")) &
            (col("StockCode").rlike("^[A-Za-z0-9]+$")) &
            (col("Quantity").isNotNull()) & (col("Quantity") > 0) &
            (col("UnitPrice").isNotNull()) & (col("UnitPrice") > 0)
        )
        df_cleaned = df_cleaned.withColumn("processing_timestamp", lit(datetime.now()))

        logging.info(f"Writing cleaned data to Bronze Delta table at: {bronze_path}")
        df_cleaned.write.format("delta").mode("overwrite").save(bronze_path)
        logging.info(f"✅ Bronze data written successfully to {bronze_path}")

    except Exception as e:
        logging.error(f"Error during Raw to Bronze ETL: {e}", exc_info=True)
        raise # Re-raise the exception to make Airflow task fail

    finally:
        spark.stop()
        logging.info("SparkSession stopped for Raw to Bronze.")

if __name__ == "__main__":
    main()
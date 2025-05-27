# src/jobs/raw_to_bronze.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from delta import configure_spark_with_delta_pip


def is_valid_invoice(invoice):
    try:
        int(invoice)
        return True
    except:
        return False


def main():
    # Setup Spark session with Delta
    builder = SparkSession.builder \
        .appName("RawToBronzeJob") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    spark.sparkContext.setLogLevel("ERROR") # Or "WARN", "INFO", "DEBUG", "TRACE"
                                        # Start with "WARN" or "INFO" to see more details

    # File paths
    raw_path = "s3a://retail-lakehouse/raw/online_retail.csv"
    bronze_path = "s3a://retail-lakehouse/bronze/online_retail_bronze"


    csv_schema = StructType([
        StructField("invoice_no", StringType(), True),
        StructField("stock_code", StringType(), True),
        StructField("description", StringType(), True),
        StructField("qty", IntegerType(), True),
        StructField("invoice_date", StringType(), True), # Keep as StringType for now if parsing as timestamp is done later
        StructField("unit_price", DoubleType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("country", StringType(), True)
    ])

    print(f"Reading from: {raw_path}")

    # Read raw CSV
    df_raw = spark.read.csv(raw_path, header=True, schema=csv_schema)

    df_cleaned = df_raw.dropna(subset=["invoice_no", "stock_code", "customer_id"])

    # Data cleaning rules
    
    df_cleaned = df_raw.filter(
        (col("invoice_no").rlike("^[0-9]+$")) &
        (col("stock_code").rlike("^[A-Z0-9]+$")) &
        (col("qty").isNotNull()) & (col("qty") > 0) &
        (col("unit_price").isNotNull()) & (col("unit_price") > 0)
    )

    print(f"Writing to: {bronze_path}")
    df_cleaned.write.format("delta").mode("overwrite").save(bronze_path)

    print(f"✅ Bronze data written to {bronze_path}")
    spark.stop()


if __name__ == "__main__":
    main()

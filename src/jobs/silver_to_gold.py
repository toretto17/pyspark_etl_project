# src/jobs/silver_to_gold.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, date_format, current_timestamp
from delta import configure_spark_with_delta_pip
import logging # For production-level logging

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    logging.info("Starting Silver to Gold ETL job.")

    # Initialize Spark session
    # Removed .master() and .config("spark.jars.packages")
    builder = SparkSession.builder \
        .appName("SilverToGoldJob") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.autoCompact.enabled", "true") \
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")


    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    spark.sparkContext.setLogLevel("WARN") # Set Spark log level
    logging.info(f"SparkSession created. Spark version: {spark.version}")

    silver_path = "s3a://retail-lakehouse/silver/online_retail_silver"
    gold_base_path = "s3a://retail-lakehouse/gold" # Base path for gold layer

    try:
        logging.info(f"Reading from Silver layer: {silver_path}")
        df_silver = spark.read.format("delta").load(silver_path)
        # logging.info(f"Silver data count: {df_silver.count()}")
        logging.info("Silver data schema:")
        df_silver.printSchema()

        # Add gold layer load timestamp
        df_silver = df_silver.withColumn("gold_load_timestamp", current_timestamp())

        # Create YearMonth field for reporting
        df_silver = df_silver.withColumn("YearMonth", date_format("InvoiceDate", "yyyy-MM"))

        # --- 1. Monthly Revenue by Country ---
        logging.info("Generating Monthly Revenue by Country KPI...")
        df_monthly_country = df_silver.groupBy("Country", "YearMonth").agg(
            _sum(col("TotalPrice")).alias("Monthly_Revenue")
        )
        monthly_country_path = f"{gold_base_path}/monthly_revenue_by_country"
        df_monthly_country.write.format("delta").mode("overwrite").save(monthly_country_path)
        logging.info(f"✅ Monthly Revenue by Country written to {monthly_country_path}")


        # --- 2. Top 10 Customers by Revenue ---
        logging.info("Generating Top 10 Customers by Revenue KPI...")
        df_customer_revenue = df_silver.groupBy("CustomerID", "Country").agg(
            _sum(col("TotalPrice")).alias("Total_Revenue")
        ).orderBy(col("Total_Revenue").desc()).limit(10)

        top_customers_path = f"{gold_base_path}/top_customers_by_revenue"
        df_customer_revenue.write.format("delta").mode("overwrite").save(top_customers_path)
        logging.info(f"✅ Top Customers by Revenue written to {top_customers_path}")

        # --- 3. Product Performance by Country ---
        logging.info("Generating Product Performance by Country KPI...")
        df_product_perf = df_silver.groupBy("StockCode", "Description", "Country").agg(
            _sum("Quantity").alias("Total_Quantity_Sold"),
            _sum(col("TotalPrice")).alias("Total_Product_Revenue")
        )
        product_perf_path = f"{gold_base_path}/product_performance_by_country"
        df_product_perf.write.format("delta").mode("overwrite").save(product_perf_path)
        logging.info(f"✅ Product Performance by Country written to {product_perf_path}")

        logging.info("✅ Gold layer KPIs generated successfully.")

    except Exception as e:
        logging.error(f"Error during Silver to Gold ETL: {e}", exc_info=True)
        raise

    finally:
        spark.stop()
        logging.info("SparkSession stopped for Silver to Gold.")

if __name__ == "__main__":
    main()
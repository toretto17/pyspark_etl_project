from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, date_format, to_date, concat_ws

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("SilverToGoldJob") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    silver_path = "s3a://retail-lakehouse/silver/online_retail_silver"
    gold_path = "s3a://retail-lakehouse/gold"

    df_silver = spark.read.format("delta").load(silver_path)

    # Create YearMonth field
    df_silver = df_silver.withColumn("year_month", date_format("invoice_date", "yyyy-MM"))

    # --- 1. Monthly Revenue by Country ---
    df_monthly_country = df_silver.groupBy("country", "year_month").agg(_sum(col("qty") * col("unit_price")).alias("Monthly_Revenue")
    )

    df_monthly_country.write.format("delta").mode("overwrite").save(f"{gold_path}/monthly_revenue_by_country")

    # --- 2. Top 10 Customers by Revenue ---
    df_customer_revenue = df_silver.groupBy("customer_id", "country").agg(
        _sum(col("qty") * col("unit_price")).alias("Total_Revenue")
    ).orderBy(col("Total_Revenue").desc()).limit(10)

    df_customer_revenue.write.format("delta").mode("overwrite").save(f"{gold_path}/top_customers_by_revenue")

    # --- 3. Product Performance by Country ---
    df_product_perf = df_silver.groupBy("stock_code", "description", "country").agg(
        _sum("qty").alias("Total_Quantity"),
        _sum(col("qty") * col("unit_price")).alias("Total_Revenue")
    )

    df_product_perf.write.format("delta").mode("overwrite").save(f"{gold_path}/product_performance_by_country")
    print()
    print("✅ Gold layer KPIs generated successfully.")
    print()

if __name__ == "__main__":
    main()


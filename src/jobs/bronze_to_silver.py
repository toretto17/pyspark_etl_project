from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_timestamp, when,expr, year, month, dayofmonth
from delta import configure_spark_with_delta_pip

def main():
    builder = SparkSession.builder \
        .appName("BronzeToSilverJob") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    bronze_path = "s3a://retail-lakehouse/bronze/online_retail_bronze"
    silver_path = "s3a://retail-lakehouse/silver/online_retail_silver"
    print()
    print(f"📥 Reading from Bronze: {bronze_path}")
    print()
    df_bronze = spark.read.format("delta").load(bronze_path)

    df_silver = df_bronze \
        .withColumn("invoice_no", trim(col("invoice_no"))) \
        .withColumn("stock_code", trim(col("stock_code"))) \
        .withColumn("description", trim(col("description"))) \
        .withColumn("country", trim(col("country"))) \
        .withColumn("invoice_date", to_timestamp("invoice_date", "MM/dd/yyyy HH:mm")) \
        .withColumn("total_price", col("qty") * col("unit_price")) \
        .withColumn("year", year(col("invoice_date"))) \
        .withColumn("month", month(col("invoice_date"))) \
        .withColumn("day", dayofmonth(col("invoice_date"))) \
        .withColumn("is_uk_customer", when(col("country") == "United Kingdom", True).otherwise(False)) \
        .withColumn("high_value_order", when(col("total_price") > 1000, True).otherwise(False)) \
        .withColumn("bulk_order", when(col("qty") >= 100, True).otherwise(False)) \
        .withColumn("is_valid_record", when(
            (col("qty") > 0) & (col("unit_price") > 0) & (col("invoice_no").isNotNull()),
            True
        ).otherwise(False)) \
        .dropDuplicates(["invoice_no", "stock_code", "customer_id"])

    print("💾 Writing to Silver Layer...")
    df_silver.write.format("delta").mode("overwrite").save(silver_path)

    print(f"✅ Silver layer written to: {silver_path}")
    spark.stop()

if __name__ == "__main__":
    main()

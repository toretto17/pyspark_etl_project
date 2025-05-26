from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder \
    .appName("PySparkETL") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

    
    # Some sample data (you could read from a file, S3, etc. in a real project)
    data = [
        "Hello Spark",
        "Spark is great",
        "Hello world",
        "Hello again Spark"
    ]

    # Create an RDD from the data
    rdd = spark.sparkContext.parallelize(data)

    # Perform word count
    word_counts = rdd.flatMap(lambda line: line.split(" ")) \
                     .map(lambda word: (word.lower(), 1)) \
                     .reduceByKey(lambda a, b: a + b)

    # Collect and print the results
    results = word_counts.collect()
    for word, count in results:
        print(f"Word: '{word}', Count: {count}")

    # Stop the SparkSession
    spark.stop()
    print("PySpark job completed successfully!")
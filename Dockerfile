# File: pyspark-etl-project/Dockerfile

# Use a pre-built Spark image that includes Python and Java
FROM bitnami/spark:3.4.2-debian-11-r10

# Switch to root temporarily to install system-level dependencies.
USER root

# Update apt package lists and install Python3, pip3, and wget
RUN apt-get update && \
    apt-get install -y python3 python3-pip wget && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory inside the container.
WORKDIR /app

# Copy the requirements.txt file first to leverage Docker layer caching.
COPY requirements.txt .

# Install Python dependencies from requirements.txt.
RUN pip install --no-cache-dir -r requirements.txt

# --- MODIFIED SECTION: Download and place JARs (REMOVE spark-defaults.conf edits) ---
# Define JAR versions
ENV DELTA_VERSION=2.4.0
ENV HADOOP_AWS_VERSION=3.3.1
ENV AWS_SDK_VERSION=1.12.593
ENV SPARK_JARS_DIR=/opt/bitnami/spark/jars/
# No need for SPARK_CONF_DIR if we aren't modifying spark-defaults.conf here.

# Download Delta Lake and Hadoop AWS JARs directly into Spark's jars directory
RUN wget -q "https://repo1.maven.org/maven2/io/delta/delta-core_2.12/${DELTA_VERSION}/delta-core_2.12-${DELTA_VERSION}.jar" -P ${SPARK_JARS_DIR} && \
    wget -q "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" -P ${SPARK_JARS_DIR} && \
    wget -q "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" -P ${SPARK_JARS_DIR} && \
    wget -q "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" -P ${SPARK_JARS_DIR}

# --- REMOVED THE FOLLOWING LINES ---
# RUN echo "spark.jars=${SPARK_JARS_DIR}/delta-core_2.12-${DELTA_VERSION}.jar,${SPARK_JARS_DIR}/delta-storage-${DELTA_VERSION}.jar,${SPARK_JARS_DIR}/hadoop-aws-${HADOOP_AWS_VERSION}.jar,${SPARK_JARS_DIR}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" >> ${SPARK_CONF_DIR}/spark-defaults.conf && \
#     echo "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" >> ${SPARK_CONF_DIR}/spark-defaults.conf && \
#     echo "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" >> ${SPARK_CONF_DIR}/spark-defaults.conf
# --- END REMOVED SECTION ---

# Create a dedicated non-root user and group for security.
RUN groupadd -g 1001 appgroup && \
    useradd -u 1001 -g 1001 -m appuser

# Create necessary directories for Spark's temporary files and checkpoints.
RUN mkdir -p /tmp/spark-local /home/appuser/spark-checkpoints && \
    chown -R appuser:appgroup /tmp/spark-local /home/appuser/spark-checkpoints

# Switch to the non-root user.
USER appuser

# Set Spark environment variables to point to the user-owned directories.
ENV SPARK_LOCAL_DIRS=/tmp/spark-local \
    SPARK_WORKER_DIR=/home/appuser/spark-worker-dir

# Copy your PySpark application code into the container.
COPY . .

# Define a default command. Airflow's DockerOperator will provide the specific script.
CMD ["/opt/bitnami/spark/bin/spark-submit", "--master", "spark://spark-master:7077"]
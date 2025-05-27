# File: pyspark-etl-project/Dockerfile

# Use a pre-built Spark image that includes Python and Java
# Bitnami images are excellent for this as they are well-maintained.
FROM bitnami/spark:3.4.2-debian-11-r10

# Switch to root temporarily to install system-level dependencies.
# This is required for `apt-get` commands.
USER root

# Update apt package lists and install Python3 and pip3.
# `rm -rf /var/lib/apt/lists/*` cleans up the apt cache to keep image size small.
RUN apt-get update && \
    apt-get install -y python3 python3-pip wget && \
    rm -rf /var/lib/apt/lists/*

# Download Delta Lake JAR and put it into Spark jars directory
# RUN wget https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar -P /opt/bitnami/spark/jars/


# Set the working directory inside the container.
# All subsequent COPY/RUN commands will operate relative to this directory.
WORKDIR /app

# Copy the requirements.txt file first to leverage Docker layer caching.
# If requirements.txt doesn't change, this layer won't be rebuilt.
COPY requirements.txt .

# Install Python dependencies from requirements.txt.
# `--no-cache-dir` reduces the size of the image by not storing pip's cache.
RUN pip install --no-cache-dir -r requirements.txt

# Create a dedicated non-root user and group for security.
# Using UID/GID 1001 is a common practice to avoid conflicts with system users.
RUN groupadd -g 1001 appgroup && \
    useradd -u 1001 -g 1001 -m appuser

# Create necessary directories for Spark's temporary files and checkpoints.
# Ensure the newly created 'appuser' has ownership of these directories.
# Spark needs writable locations for shuffle files, and checkpoints for fault tolerance.
RUN mkdir -p /tmp/spark-local /home/appuser/spark-checkpoints && \
    chown -R appuser:appgroup /tmp/spark-local /home/appuser/spark-checkpoints

# Switch to the non-root user.
# All subsequent commands (including the application execution) will run as 'appuser'.
USER appuser

# Set Spark environment variables to point to the user-owned directories.
# These variables tell Spark where to store its temporary local data.
ENV SPARK_LOCAL_DIRS=/tmp/spark-local \
    SPARK_WORKER_DIR=/home/appuser/spark-worker-dir

# Copy your PySpark application code into the container.
# Ensure your main PySpark script is named 'main.py' in your project root.
COPY . .

# Set the Entrypoint for the container to 'spark-submit'.
# This means 'spark-submit' will always be the executable run when this container starts.
# Any 'command' specified in docker-compose.yml will be passed as arguments to 'spark-submit'.
ENTRYPOINT ["spark-submit"]

# Define a default command (empty in this case).
# The actual arguments for spark-submit will be provided by docker-compose.yml's 'command'.
CMD []

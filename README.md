# Retail Lakehouse ETL Pipeline

A production-ready, Dockerized ETL pipeline designed for building a **retail data lakehouse**. This project leverages **Apache Airflow** for workflow orchestration and **PySpark** for scalable data processing, transforming raw retail data into a refined, queryable format within a MinIO-backed object storage environment.

---

## Project Structure

Your project structure now looks like this, indicating different layers of your lakehouse (`bronze`, `silver`, `gold`):

```
.
├── docker-compose.yml           # Docker Compose configuration for Airflow, Spark, and MinIO
├── Dockerfile                   # Defines the PySpark ETL application Docker image
├── main.py                      # Placeholder or pipeline entry point (if applicable)
├── README.md                    # Project overview and guide
├── requirements.txt             # Python dependencies for the Airflow environment
├── silver_to_gold               # Likely a leftover/misplaced directory, consider moving contents to src/jobs if it's a script.
|
+---airflow
|   +---dags                     # Airflow DAG definitions
|   |   |   retail_lakehouse_dag.py
|   |
|   +---logs                     # Airflow scheduler and task logs (should be ignored by Git)
|   +---plugins                  # Custom Airflow plugins (if any)
|
+---data                         # Persistent storage for lakehouse layers (mapped to MinIO buckets)
|   +---bronze                   # Raw, untransformed data
|   |   ---online_retail_bronze
|   |       └── _delta_log       # Delta Lake transaction logs
|   |
|   +---gold                     # Highly aggregated, business-ready data for analytics/BI
|   |   ---onlin_retail_gold
|   |       ├── monthly_revenue_by_country
|   |       ├── product_performance_by_country
|   |       └── top_customers_by_revenue
|   |           └── _delta_log
|   |
|   +---input                    # Initial raw input files (e.g., online_retail.csv)
|   ---silver                   # Cleaned, conformed, and integrated data
|       ---online_retail_silver
|           └── _delta_log
|
+---notebooks                    # Jupyter notebooks for data exploration, analysis, and prototyping
|
---src
+---jobs                     # PySpark ETL job scripts
|   |   bronze_to_silver.py
|   |   etl_job.py           # Generic ETL job template/example
|   |   raw_to_bronze.py
|   |   silver_to_gold.py
|
---utils                    # Utility functions and helper modules
    helpers.py
```

---

## Technologies Used

* **Apache Airflow**: For orchestrating the ETL workflows.
* **PySpark**: For distributed data processing and transformations.
* **Delta Lake**: As the storage layer for the lakehouse, enabling ACID transactions and schema evolution on data stored in object storage.
* **MinIO**: An S3-compatible object storage server, used as the data lake storage backend.
* **Docker & Docker Compose**: For containerization and managing the multi-service environment (Airflow, Spark, MinIO).

---

## Installation

### Prerequisites

Before you begin, ensure you have the following installed on your system:

* **Python 3.10+**
* **Docker Engine** (20.10.x+)
* **Docker Compose** (1.29.x+ or Docker Compose V2)

### Setup Locally

1.  **Clone the repository:**

    ```bash
    git clone https://github.com/your-username/pyspark-etl-project.git
    cd pyspark-etl-project
    ```

2.  **Create and activate a Python virtual environment** (optional but recommended):

    ```bash
    python3.10 -m venv venv
    source venv/bin/activate    # Linux/macOS
    .env\Scriptsctivate     # Windows PowerShell
    ```

3.  **Install Python dependencies**:

    ```bash
    pip install -r requirements.txt
    ```

---

## Usage

### Running Airflow Services with Docker Compose

1.  **Build the PySpark ETL image:**

    ```bash
    docker build -t pyspark-etl-app .
    ```

2.  **Start all services:**

    ```bash
    docker-compose up --build -d
    ```

    * Airflow UI: [http://localhost:8080](http://localhost:8080)
    * MinIO UI: [http://localhost:9001](http://localhost:9001) (default: `admin` / `password123`)
    * Spark UI: Check `docker-compose.yml` to avoid port conflict with Airflow

3.  **Upload Input Data to MinIO:**

    Upload `online_retail.csv` to the `input` bucket.

4.  **Trigger the DAG:**

    Run `retail_lakehouse_etl` DAG via Airflow UI.

### Shut Down

```bash
docker-compose down -v
```

---

## ETL Workflow Overview

### raw_to_bronze.py
- Reads raw data from `input`
- Adds metadata
- Writes Delta table to `bronze`

### bronze_to_silver.py
- Reads from `bronze`
- Cleans and transforms data
- Writes to `silver`

### silver_to_gold.py
- Reads from `silver`
- Aggregates insights:
  - Monthly revenue by country
  - Product performance by country
  - Top customers by revenue
- Writes to `gold`

---

## Spark Dependency Management

```bash
--packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.593
```

---

## Contributing

1. Fork the repository
2. Create a new branch: `git checkout -b feature/AmazingFeature`
3. Commit changes: `git commit -m 'Add some AmazingFeature'`
4. Push to the branch: `git push origin feature/AmazingFeature`
5. Open a Pull Request
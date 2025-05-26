
# Airflow ETL Pipeline Project

A production-ready, Dockerized ETL pipeline built with Apache Airflow and PySpark. This project orchestrates data workflows using Airflow DAGs, performs data processing jobs, and manages data input/output efficiently.

---

## Project Structure

```
.
├── docker-compose.yml       # Docker Compose configuration for Airflow and services
├── Dockerfile               # Docker image definition
├── main.py                  # Pipeline entry point (if applicable)
├── requirements.txt         # Python dependencies
├── dags/                    # Airflow DAG definitions
├── data/                    # Data files
│   ├── input/               # Source input datasets
│   └── output/              # Processed output datasets
├── notebooks/               # Jupyter notebooks for data exploration and prototyping
└── src/                     # Source code modules
    ├── jobs/                # ETL job scripts (e.g., etl_job.py)
    └── utils/               # Utility functions (e.g., helpers.py)
```

---

## Installation

### Prerequisites

- Python 3.11+
- Docker & Docker Compose
- Apache Airflow (if running without Docker)

### Setup Locally

1. Clone the repository:

   ```bash
   git clone <repo_url>
   cd <project_folder>
   ```

2. Create and activate a Python virtual environment:

   ```bash
   python3.11 -m venv venv
   source venv/bin/activate        # Linux/macOS
   .\venv\Scripts\activate         # Windows PowerShell
   ```

3. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

---

## Usage

### Running Airflow Services with Docker

Start Airflow webserver and scheduler with Docker Compose:

```bash
docker-compose up --build
```

- Access the Airflow UI at [http://localhost:8080](http://localhost:8080)
- Trigger and monitor DAGs via the UI

### Running Locally (without Docker)

Activate your virtual environment and start services manually:

- Start Airflow webserver:

  ```bash
  airflow webserver --port 8080
  ```

- In a separate terminal (same environment), start the scheduler:

  ```bash
  airflow scheduler
  ```

### Running ETL Jobs

ETL jobs are implemented under `src/jobs`. They can be executed independently or triggered by Airflow DAGs located in `dags/`.

---

## Contributing

Contributions are welcome! Please fork the repo and open a pull request for any improvements or bug fixes.

---

## License

[Specify your license here, e.g., MIT License]


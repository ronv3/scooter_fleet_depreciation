FROM apache/airflow:2.10.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install dbt + DuckDB adapter inside Airflow image
RUN pip install --no-cache-dir \
    dbt-core==1.8.8 \
    dbt-duckdb==1.8.2 \
    pandas==2.2.2 \
    numpy==1.26.4
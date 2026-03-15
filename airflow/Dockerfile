FROM apache/airflow:2.10.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    dbt-core==1.9.5 \
    dbt-duckdb==1.9.1 \
    pandas==2.2.2 \
    numpy==1.26.4 \
    python-dateutil==2.9.0
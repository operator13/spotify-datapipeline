# =============================================================================
# Airflow Dockerfile
# Custom Apache Airflow image with dbt and data pipeline dependencies
# =============================================================================

FROM apache/airflow:2.8.0-python3.11

# Switch to root to install system dependencies
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    git \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Set working directory
WORKDIR /opt/airflow

# Copy requirements file
COPY requirements.txt /opt/airflow/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    dbt-core==1.7.0 \
    dbt-postgres==1.7.0 \
    kagglehub>=0.2.0 \
    pandas>=2.0.0 \
    sqlalchemy>=2.0.0 \
    psycopg2-binary>=2.9.0 \
    apache-airflow-providers-postgres>=5.0.0 \
    apache-airflow-providers-slack>=8.0.0 \
    python-dotenv>=1.0.0 \
    pyyaml>=6.0.0

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV DBT_PROFILES_DIR=/opt/airflow/dbt
ENV PYTHONPATH=/opt/airflow

# Create directories for dbt and scripts
RUN mkdir -p /opt/airflow/dbt /opt/airflow/scripts

# Expose Airflow webserver port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl --fail http://localhost:8080/health || exit 1

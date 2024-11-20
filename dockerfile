FROM apache/airflow:2.10.3

# Switch to root user to install system dependencies
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-dev \
    build-essential \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user
USER airflow

# Upgrade pip, setuptools, and wheel
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Install Python dependencies
RUN pip install apache-airflow==2.10.3 \
    pandas \
    requests \
    beautifulsoup4 \
    urllib3 \
    pymongo \
    python-dotenv \
    psycopg2-binary \
    numpy \
    bs4 \
    typing \
    geopy \
    pymongo[srv] \
    dnspython

# Create necessary directories for Airflow
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins

# Copy your project files
COPY . /opt/airflow/

# Set the default command
CMD ["bash"]
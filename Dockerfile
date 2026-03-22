# Dockerfile
# ----------
# Custom Airflow image with all required Python packages pre-installed.

FROM apache/airflow:2.9.2

# Switch to root to install system dependencies if needed
USER root

# Switch back to airflow user for pip installs (required by Airflow)
USER airflow

# Copy and install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
FROM apache/airflow:2.9.1

# Install Python packages as airflow user (default)
RUN pip install --no-cache-dir confluent_kafka psycopg2-binary

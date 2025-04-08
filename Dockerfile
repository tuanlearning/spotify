# Use this to run python script spotify.py
# FROM python:3.7.17
# WORKDIR /opt/spotify
# COPY . .
# RUN pip install -r requirements.txt

FROM apache/airflow:2.9.0-python3.10

# Install system build dependencies first
# RUN apt-get update && apt-get install -y \
#     build-essential \
#     gcc \
#     libffi-dev \
#     libpq-dev \
#     libssl-dev \
#     python3-dev \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/spotify
COPY . .

# RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

USER root
RUN chmod -R 777 data/ && chmod -R 777 /opt/spotify/spotify/ && chmod -R 777 /opt/spotify/airflow/dags/data

USER airflow

EXPOSE 8080
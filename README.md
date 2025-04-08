# Spotify Data Pipeline 🚀🎧

A modern data engineering pipeline that extracts data from the Spotify API and transforms it into analytics-ready datasets using Airflow, Terraform, and dbt on Google Cloud Platform (GCP).

## 🛠 Tech Stack

- **Terraform** – Infrastructure as Code (IaC) to provision:
  - Google Cloud Storage bucket: `spotify_stg`
  - BigQuery datasets: `spotify_stg`, `spotify`
- **Airflow** – Orchestrates the entire pipeline from extraction to loading.
- **Spotify API** – Source of raw data (artists, albums, tracks).
- **Google Cloud Storage (GCS)** – Temporary staging layer for raw CSVs.
- **Google BigQuery** – Data warehouse for structured analytical queries.
- **dbt** – Transformations, modeling, and documentation of analytics tables.

## 📁 Project Structure
<pre lang="markdown"> ## 📁 Project Structure ``` spotify/ ├── dags/ # Airflow DAGs ├── common/ # Helper functions ├── terraform/ # Terraform configs │ ├── main.tf │ ├── variables.tf │ └── ... ├── dbt/ │ ├── models/ │ └── ... ├── dashboard/ # Streamlit dashboard (optional) ├── credentials/ # GCP service account key └── README.md ``` </pre>

## ⚙️ Pipeline Overview

1. **Infrastructure Setup**  
   Use Terraform to spin up:
   - GCS bucket (`spotify_stg`)
   - BigQuery datasets (`spotify_stg`, `spotify`)

2. **Data Extraction**  
   Airflow DAG extracts data from the Spotify API:
   - Artist, album, and track metadata
   - Auth handled via bearer token

3. **Staging**  
   - Data is written as CSV files to GCS
   - BigQuery external tables reference GCS data (`spotify_stg` schema)

4. **Transformation**  
   - dbt transforms raw data into dimensional models (`spotify` schema)
   - Factless fact tables for many-to-many relationships

## 📊 Dashboard (Optional)

- Streamlit app to visualize:
  - Artist popularity, album count, track statistics
  - Explicit content ratio and average duration

---

This project is built for learning purposes and showcases a full modern data stack in action 💪
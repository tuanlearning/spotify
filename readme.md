Install DBT
- Packages required: dbt-core 1.5.5, dbt-bigquery 1.5.2
- dbt init to create the project
- dbt deps to install required dependencies
- docker-compose run --rm airflow-webserver airflow db init to init airflow webserver service
- Add webserver admin user
docker ps
docker-exec -it <webserver_container_id> bash
airflow users create \
    --username airflow \
    --firstname Airflow \
    --lastname Admin \
    --role Admin \
    --email admin@example.com \
    --password airflow
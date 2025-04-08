# Run python script
docker-compose run --rm python python spotify.py
# Run dbt model
docker-compose run --rm python sh -c "cd spotify && dbt run --select dim_artists"
docker-compose run --rm --entrypoint "" python sh -c "cd spotify 
&& dbt run --select dim_artists" # With airflow image, overwrite entrypoint since sh is airflow-cli
# Init airflow
docker-compose run --rm airflow-webserver airflow db init to init airflow webserver service
# Add webserver admin user
docker ps
docker-exec -it <webserver_container_id> bash
airflow users create \
    --username airflow \
    --firstname Airflow \
    --lastname Admin \
    --role Admin \
    --email admin@example.com \
    --password airflow

# Run streamlit dashboard
docker-compose run --rm --entrypoint "" python streamlit run dashboard/spotify_dashboard.py
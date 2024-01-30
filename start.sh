#!/bin/bash
source .env
filepath=$(pwd)

# Start Airflow
docker compose --env-file ./.env up airflow-init
docker compose --env-file ./.env up -d

# Create table for job data
poetry run python3 create_db.py

# Add a connection to PostgreSQL for Airflow
CONNECTION_URI="postgresql://$db_user:$db_pass@$db_host:$db_port/$db_name"

docker exec -it scheduler_metals sh -c "airflow connections add 'postgres' --conn-uri '$CONNECTION_URI'"
docker exec -it scheduler_metals sh -c "airflow connections add 'metals_api' --conn-json '{\"conn_type\": \"http\",\"host\": \"https://api.metalpriceapi.com\"}'"
docker exec -it scheduler_metals sh -c "airflow connections add 'aws' --conn-uri 'aws://$aws_access_key_id:$aws_secret_access_key@/?region_name=$aws_region'"

host.docker.internal - host
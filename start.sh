#!/bin/bash

# Start Airflow
cd docker/
docker build
docker compose --env-file airflow/plugins/scripts/.env up airflow-init
docker compose --env-file airflow/plugins/scripts/.env up -d

# Set variables and connections in Airflow
docker exec -it scheduler_metals sh -c "python ./plugins/scripts/add_global_vars.py"
docker exec -it worker_metals sh -c "mkdir $backup_dir && mkdir $backup_dir/postgres && mkdir $backup_dir/models"
cd ..

# Create table for job data
poetry run python3 ./db/create_db.py
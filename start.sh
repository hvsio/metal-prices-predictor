#!/bin/bash
source .env
filepath=$(pwd)

# Start Airflow
docker compose up airflow-init
docker compose ./.env up -d

# Create table for job data
poetry run python3 ./db/create_db.py

# Set variables and connections in Airflow
docker exec -it scheduler_metals sh -c "python ./plugins/scripts/add_global_vars.py"

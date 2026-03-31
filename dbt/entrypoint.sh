#!/bin/bash
set -e

echo "Waiting for initial data to accumulate..."
sleep 15

echo "Running dbt seed (dimension tables)..."
dbt seed --profiles-dir /app --project-dir /app

echo "Starting dbt run loop (every ${DBT_RUN_INTERVAL:-60}s)..."
while true; do
    echo "--- dbt run at $(date -u +%Y-%m-%dT%H:%M:%SZ) ---"
    dbt run --profiles-dir /app --project-dir /app
    sleep "${DBT_RUN_INTERVAL:-60}"
done
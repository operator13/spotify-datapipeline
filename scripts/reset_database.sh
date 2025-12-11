#!/bin/bash
# =============================================================================
# Reset Spotify Database
# Cleans all schemas and prepares for fresh data load
# =============================================================================

set -e

echo "=========================================="
echo "Resetting Spotify Data Pipeline Database"
echo "=========================================="

# Step 1: Clean all schemas
echo ""
echo "Step 1: Dropping all schemas..."
docker exec spotify_postgres psql -U postgres -d spotify_warehouse -c "
-- Drop all dbt-created schemas
DROP SCHEMA IF EXISTS staging_staging CASCADE;
DROP SCHEMA IF EXISTS staging_intermediate CASCADE;
DROP SCHEMA IF EXISTS staging_marts CASCADE;
DROP SCHEMA IF EXISTS staging_seeds CASCADE;
DROP SCHEMA IF EXISTS staging_data_quality CASCADE;
DROP SCHEMA IF EXISTS raw CASCADE;

-- Recreate raw schema
CREATE SCHEMA raw AUTHORIZATION spotify;
GRANT ALL ON SCHEMA raw TO spotify;
"

echo "  - All schemas dropped and raw schema recreated"

# Step 2: Delete Airflow DAG runs
echo ""
echo "Step 2: Clearing Airflow DAG history..."
docker exec spotify_airflow_webserver airflow dags delete spotify_etl_pipeline --yes 2>/dev/null || true
docker exec spotify_airflow_webserver airflow dags delete data_quality_monitoring --yes 2>/dev/null || true
echo "  - DAG history cleared"

# Step 3: Wait for DAGs to be re-registered
echo ""
echo "Step 3: Waiting for DAGs to re-register..."
sleep 10

# Step 4: Unpause DAG
echo ""
echo "Step 4: Unpausing DAG..."
docker exec spotify_airflow_webserver airflow dags unpause spotify_etl_pipeline 2>/dev/null || true

echo ""
echo "=========================================="
echo "Database reset complete!"
echo ""
echo "You can now:"
echo "1. Go to Airflow UI: http://localhost:8080"
echo "2. Click the play button on 'spotify_etl_pipeline'"
echo "3. Or run: docker exec spotify_airflow_webserver airflow dags trigger spotify_etl_pipeline"
echo "=========================================="

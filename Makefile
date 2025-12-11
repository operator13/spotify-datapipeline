# =============================================================================
# Makefile - Spotify Data Quality Pipeline
#
# Common commands for development and operations
# =============================================================================

.PHONY: help up down logs clean dbt-deps dbt-run dbt-test dbt-docs lint test setup reset-db notebook

# Default target
help:
	@echo "Spotify Data Quality Pipeline - Available Commands"
	@echo "=================================================="
	@echo ""
	@echo "Infrastructure:"
	@echo "  make up          - Start all Docker services"
	@echo "  make down        - Stop all Docker services"
	@echo "  make restart     - Restart all services"
	@echo "  make logs        - View container logs (follow mode)"
	@echo "  make ps          - Show running containers"
	@echo "  make clean       - Remove containers, volumes, and images"
	@echo "  make reset-db    - Reset database (clears all data)"
	@echo ""
	@echo "dbt Commands:"
	@echo "  make dbt-deps    - Install dbt packages"
	@echo "  make dbt-seed    - Load seed data"
	@echo "  make dbt-run     - Run all dbt models"
	@echo "  make dbt-test    - Run all dbt tests"
	@echo "  make dbt-docs    - Generate and serve dbt docs"
	@echo "  make dbt-fresh   - Full refresh (deps, seed, run, test)"
	@echo ""
	@echo "Development:"
	@echo "  make setup       - Initial project setup"
	@echo "  make lint        - Run SQL and YAML linting"
	@echo "  make test        - Run all tests"
	@echo "  make venv        - Create virtual environment"
	@echo "  make notebook    - Launch Jupyter notebook for data exploration"
	@echo ""

# =============================================================================
# Infrastructure Commands
# =============================================================================

up:
	@echo "Starting all services..."
	cd docker && docker compose up -d
	@echo "Services started. Access points:"
	@echo "  Airflow: http://localhost:8080 (admin/admin)"
	@echo "  Grafana: http://localhost:3000 (admin/admin)"
	@echo "  PostgreSQL: localhost:5432 (spotify/spotify_password)"

down:
	@echo "Stopping all services..."
	cd docker && docker compose down

restart: down up

logs:
	cd docker && docker compose logs -f

ps:
	cd docker && docker compose ps

clean:
	@echo "Cleaning up Docker resources..."
	cd docker && docker compose down -v --rmi local
	@echo "Cleanup complete."

# =============================================================================
# dbt Commands
# =============================================================================

DBT_DIR = dbt/spotify_dq
DBT_CMD = cd $(DBT_DIR) && dbt

dbt-deps:
	@echo "Installing dbt packages..."
	$(DBT_CMD) deps

dbt-seed:
	@echo "Loading seed data..."
	$(DBT_CMD) seed --full-refresh

dbt-run:
	@echo "Running all dbt models..."
	$(DBT_CMD) run

dbt-run-staging:
	@echo "Running staging models..."
	$(DBT_CMD) run --select staging

dbt-run-intermediate:
	@echo "Running intermediate models..."
	$(DBT_CMD) run --select intermediate

dbt-run-marts:
	@echo "Running marts models..."
	$(DBT_CMD) run --select marts

dbt-test:
	@echo "Running dbt tests..."
	$(DBT_CMD) test --store-failures

dbt-docs:
	@echo "Generating dbt documentation..."
	$(DBT_CMD) docs generate
	@echo "Serving docs at http://localhost:8001"
	$(DBT_CMD) docs serve --port 8001

dbt-fresh: dbt-deps dbt-seed dbt-run dbt-test
	@echo "Full dbt refresh complete."

dbt-compile:
	@echo "Compiling dbt models..."
	$(DBT_CMD) compile

# =============================================================================
# Development Commands
# =============================================================================

venv:
	@echo "Creating virtual environment..."
	python -m venv venv
	@echo "Activate with: source venv/bin/activate"

setup: venv
	@echo "Installing Python dependencies..."
	. venv/bin/activate && pip install -r requirements.txt
	@echo "Setup complete. Don't forget to:"
	@echo "  1. Activate venv: source venv/bin/activate"
	@echo "  2. Configure Kaggle credentials"
	@echo "  3. Start services: make up"

lint:
	@echo "Running SQLFluff..."
	sqlfluff lint $(DBT_DIR)/models --dialect postgres || true
	@echo "Running yamllint..."
	yamllint -d relaxed $(DBT_DIR)/*.yml || true

test: dbt-test
	@echo "All tests complete."

# =============================================================================
# Utility Commands
# =============================================================================

# Reset database (clears all data)
reset-db:
	@echo "Resetting database..."
	./scripts/reset_database.sh
	@echo "Database reset complete."

# Download dataset from Kaggle
download-data:
	@echo "Downloading Kaggle dataset..."
	python -c "import kagglehub; kagglehub.dataset_download('rohiteng/spotify-music-analytics-dataset-20152025')"

# Connect to PostgreSQL
psql:
	PGPASSWORD=spotify_password psql -h localhost -p 5432 -U spotify -d spotify_warehouse

# Trigger Airflow DAG
trigger-etl:
	docker compose -f docker/docker-compose.yml exec airflow-webserver \
		airflow dags trigger spotify_etl_pipeline

trigger-dq:
	docker compose -f docker/docker-compose.yml exec airflow-webserver \
		airflow dags trigger data_quality_monitoring

# Launch Jupyter notebook
notebook:
	@echo "Starting Jupyter notebook..."
	jupyter notebook notebooks/

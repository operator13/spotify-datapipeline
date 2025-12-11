-- =============================================================================
-- 01-create-databases.sql
-- Creates databases and users for the Spotify Data Pipeline
-- =============================================================================

-- Create the spotify data warehouse database
CREATE DATABASE spotify_warehouse;

-- Create the airflow metadata database
CREATE DATABASE airflow;

-- Create dedicated users with passwords
CREATE USER spotify WITH PASSWORD 'spotify_password';
CREATE USER airflow WITH PASSWORD 'airflow';

-- Grant all privileges on respective databases
GRANT ALL PRIVILEGES ON DATABASE spotify_warehouse TO spotify;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

-- Grant connection privileges
GRANT CONNECT ON DATABASE spotify_warehouse TO spotify;
GRANT CONNECT ON DATABASE airflow TO airflow;

-- PostgreSQL 15+ requires explicit schema grants
-- Connect to airflow database and grant schema permissions
\connect airflow
GRANT ALL ON SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO airflow;

-- Connect to spotify_warehouse and grant schema permissions
\connect spotify_warehouse
GRANT ALL ON SCHEMA public TO spotify;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO spotify;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO spotify;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO spotify;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO spotify;

-- Switch back to default database
\connect postgres

-- Log completion
DO $$
BEGIN
    RAISE NOTICE 'Databases and users created successfully';
END $$;

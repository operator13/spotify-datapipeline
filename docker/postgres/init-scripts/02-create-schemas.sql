-- =============================================================================
-- 02-create-schemas.sql
-- Creates schemas for dbt layers in the spotify_warehouse database
-- =============================================================================

-- Connect to the spotify_warehouse database
\c spotify_warehouse;

-- Create schemas for each dbt layer
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS data_quality;
CREATE SCHEMA IF NOT EXISTS seeds;

-- Grant all privileges on schemas to spotify user
GRANT ALL ON SCHEMA raw TO spotify;
GRANT ALL ON SCHEMA staging TO spotify;
GRANT ALL ON SCHEMA intermediate TO spotify;
GRANT ALL ON SCHEMA marts TO spotify;
GRANT ALL ON SCHEMA analytics TO spotify;
GRANT ALL ON SCHEMA data_quality TO spotify;
GRANT ALL ON SCHEMA seeds TO spotify;

-- Grant usage and create privileges
GRANT USAGE, CREATE ON SCHEMA raw TO spotify;
GRANT USAGE, CREATE ON SCHEMA staging TO spotify;
GRANT USAGE, CREATE ON SCHEMA intermediate TO spotify;
GRANT USAGE, CREATE ON SCHEMA marts TO spotify;
GRANT USAGE, CREATE ON SCHEMA analytics TO spotify;
GRANT USAGE, CREATE ON SCHEMA data_quality TO spotify;
GRANT USAGE, CREATE ON SCHEMA seeds TO spotify;

-- Set default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT ALL ON TABLES TO spotify;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON TABLES TO spotify;
ALTER DEFAULT PRIVILEGES IN SCHEMA intermediate GRANT ALL ON TABLES TO spotify;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT ALL ON TABLES TO spotify;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT ALL ON TABLES TO spotify;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_quality GRANT ALL ON TABLES TO spotify;
ALTER DEFAULT PRIVILEGES IN SCHEMA seeds GRANT ALL ON TABLES TO spotify;

-- Log completion
DO $$
BEGIN
    RAISE NOTICE 'Schemas created successfully in spotify_warehouse';
END $$;

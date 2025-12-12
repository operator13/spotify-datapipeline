-- =============================================================================
-- 03-create-dq-tables.sql
-- Creates data quality tracking tables in the spotify_warehouse database
-- =============================================================================

-- Connect to the spotify_warehouse database
\c spotify_warehouse;

-- =============================================================================
-- Data Quality Metrics Table
-- Stores results from all DQ checks across 6 dimensions
-- =============================================================================
CREATE TABLE IF NOT EXISTS data_quality.dq_metrics (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(50) NOT NULL,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dimension VARCHAR(50) NOT NULL,  -- completeness, accuracy, consistency, timeliness, validity, uniqueness
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100),
    metric_name VARCHAR(100) NOT NULL,
    metric_value NUMERIC,
    threshold_value NUMERIC,
    passed BOOLEAN NOT NULL,
    error_message TEXT,
    row_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for efficient querying
CREATE INDEX idx_dq_metrics_dimension ON data_quality.dq_metrics(dimension);
CREATE INDEX idx_dq_metrics_timestamp ON data_quality.dq_metrics(run_timestamp);
CREATE INDEX idx_dq_metrics_table ON data_quality.dq_metrics(table_name);
CREATE INDEX idx_dq_metrics_passed ON data_quality.dq_metrics(passed);
CREATE INDEX idx_dq_metrics_run_id ON data_quality.dq_metrics(run_id);

-- Add comment
COMMENT ON TABLE data_quality.dq_metrics IS 'Stores data quality metrics across all 6 dimensions for monitoring and alerting';

-- =============================================================================
-- SLA Monitoring Table
-- Tracks pipeline execution times against SLA targets
-- =============================================================================
CREATE TABLE IF NOT EXISTS data_quality.sla_monitoring (
    id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    expected_completion_time TIME NOT NULL,
    actual_completion_time TIMESTAMP,
    sla_met BOOLEAN,
    deviation_minutes INTEGER,
    dag_run_id VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX idx_sla_monitoring_pipeline ON data_quality.sla_monitoring(pipeline_name);
CREATE INDEX idx_sla_monitoring_sla_met ON data_quality.sla_monitoring(sla_met);
CREATE INDEX idx_sla_monitoring_created ON data_quality.sla_monitoring(created_at);

-- Add comment
COMMENT ON TABLE data_quality.sla_monitoring IS 'Tracks pipeline SLA compliance for timeliness monitoring';

-- =============================================================================
-- Test Results History Table
-- Stores dbt test execution results over time
-- =============================================================================
CREATE TABLE IF NOT EXISTS data_quality.test_results_history (
    id SERIAL PRIMARY KEY,
    test_name VARCHAR(200) NOT NULL,
    test_type VARCHAR(50) NOT NULL,  -- generic, singular, unit
    model_name VARCHAR(100),
    status VARCHAR(20) NOT NULL,  -- pass, fail, warn, error, skipped
    execution_time_seconds NUMERIC,
    rows_affected INTEGER,
    message TEXT,
    run_id VARCHAR(50),
    dbt_invocation_id VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX idx_test_results_test_name ON data_quality.test_results_history(test_name);
CREATE INDEX idx_test_results_status ON data_quality.test_results_history(status);
CREATE INDEX idx_test_results_model ON data_quality.test_results_history(model_name);
CREATE INDEX idx_test_results_created ON data_quality.test_results_history(created_at);
CREATE INDEX idx_test_results_run_id ON data_quality.test_results_history(run_id);

-- Add comment
COMMENT ON TABLE data_quality.test_results_history IS 'Historical record of all dbt test executions for trend analysis';

-- =============================================================================
-- Data Anomalies Table
-- Stores detected anomalies in streaming counts and other metrics
-- =============================================================================
CREATE TABLE IF NOT EXISTS data_quality.data_anomalies (
    id SERIAL PRIMARY KEY,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    anomaly_type VARCHAR(50) NOT NULL,  -- outlier, missing_data, sudden_change, etc.
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100),
    record_identifier VARCHAR(200),
    expected_value NUMERIC,
    actual_value NUMERIC,
    deviation_percent NUMERIC,
    severity VARCHAR(20),  -- low, medium, high, critical
    resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP,
    notes TEXT
);

-- Create indexes
CREATE INDEX idx_anomalies_type ON data_quality.data_anomalies(anomaly_type);
CREATE INDEX idx_anomalies_severity ON data_quality.data_anomalies(severity);
CREATE INDEX idx_anomalies_resolved ON data_quality.data_anomalies(resolved);
CREATE INDEX idx_anomalies_detected ON data_quality.data_anomalies(detected_at);

-- Add comment
COMMENT ON TABLE data_quality.data_anomalies IS 'Tracks detected data anomalies for investigation and resolution';

-- =============================================================================
-- Function to get dbt test failures dynamically
-- Returns all tables in staging_data_quality with row counts
-- =============================================================================
CREATE OR REPLACE FUNCTION data_quality.get_dbt_test_failures()
RETURNS TABLE (
    test_table TEXT,
    test_type TEXT,
    severity TEXT,
    failed_records BIGINT
) AS $$
DECLARE
    tbl RECORD;
    cnt BIGINT;
BEGIN
    FOR tbl IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'staging_data_quality'
    LOOP
        EXECUTE format('SELECT COUNT(*) FROM staging_data_quality.%I', tbl.table_name) INTO cnt;

        IF cnt > 0 THEN
            test_table := tbl.table_name;

            -- Determine test type
            IF tbl.table_name LIKE '%not_null%' THEN
                test_type := 'NULL Check';
            ELSIF tbl.table_name LIKE '%unique%' THEN
                test_type := 'Uniqueness';
            ELSIF tbl.table_name LIKE '%accepted_values%' THEN
                test_type := 'Valid Values';
            ELSIF tbl.table_name LIKE '%relationship%' THEN
                test_type := 'FK Reference';
            ELSIF tbl.table_name LIKE '%expect%' THEN
                test_type := 'Expectation';
            ELSE
                test_type := 'Other';
            END IF;

            -- Determine severity (source tests = ERROR, staging tests = WARN)
            IF tbl.table_name LIKE 'source_%' THEN
                severity := 'ERROR';
            ELSE
                severity := 'WARN';
            END IF;

            failed_records := cnt;

            RETURN NEXT;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION data_quality.get_dbt_test_failures() IS 'Dynamically returns all dbt test failure tables with row counts';

-- =============================================================================
-- Grant permissions to spotify user
-- =============================================================================
GRANT ALL ON ALL TABLES IN SCHEMA data_quality TO spotify;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA data_quality TO spotify;
GRANT EXECUTE ON FUNCTION data_quality.get_dbt_test_failures() TO spotify;

-- Log completion
DO $$
BEGIN
    RAISE NOTICE 'Data quality tables created successfully';
END $$;

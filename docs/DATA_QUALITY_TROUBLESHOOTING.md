# Data Quality Troubleshooting Guide

This guide explains how to investigate data quality issues and test failures in the Spotify Data Pipeline.

---

## Quick Reference

| Method | Best For | Command/Location |
|--------|----------|------------------|
| Airflow Logs | Real-time debugging | Airflow UI → dbt_test → Logs |
| dq_metrics_summary | Dashboard/overview | `SELECT * FROM staging_marts.dq_metrics_summary` |
| Stored Failures | Detailed failed records | `SELECT * FROM staging_data_quality.<test_name>` |
| run_results.json | Programmatic analysis | `/opt/airflow/dbt/target/run_results.json` |

---

## Method 1: Airflow Task Logs

**Best for**: Quick debugging during/after pipeline runs

### Steps:
1. Open Airflow UI: http://localhost:8080
2. Navigate to DAG: `spotify_etl_pipeline`
3. Click on the `dbt_test` task (in `dbt_tasks` group)
4. Click the **"Logs"** tab

### What to look for:
```
[PASS] test_name .......................... [PASS in 0.05s]
[WARN 46] test_name ....................... [WARN 46 in 0.08s]
[FAIL 21] test_name ....................... [FAIL 21 in 0.04s]
```

### Summary line at end:
```
Done. PASS=138 WARN=2 ERROR=1 SKIP=0 TOTAL=141
```

---

## Method 2: DQ Metrics Summary Table

**Best for**: High-level data quality overview, Grafana dashboards

### Query all metrics:
```sql
SELECT
    dimension,
    metric_name,
    metric_value,
    threshold_value,
    passed,
    calculated_at
FROM staging_marts.dq_metrics_summary
ORDER BY passed, dimension;
```

### Query only failed metrics:
```sql
SELECT * FROM staging_marts.dq_metrics_summary
WHERE passed = false;
```

### Run from terminal:
```bash
docker exec spotify_postgres psql -U postgres -d spotify_warehouse -c \
"SELECT dimension, metric_name, round(metric_value::numeric, 4) as value,
 threshold_value as threshold, passed
 FROM staging_marts.dq_metrics_summary
 ORDER BY passed, dimension;"
```

### Metrics Covered (6 DQ Dimensions):

| Dimension | Metrics |
|-----------|---------|
| **Completeness** | track_name_completeness, genre_completeness, country_completeness |
| **Accuracy** | popularity_in_range, audio_features_valid, tempo_reasonable |
| **Consistency** | genre_fk_valid, country_fk_valid |
| **Timeliness** | data_freshness_hours |
| **Validity** | popularity_tier_valid, streaming_counts_positive, release_year_valid |
| **Uniqueness** | track_id_unique |

---

## Method 3: Stored Failures (Detailed Records)

**Best for**: Investigating specific failed records

When dbt tests run with `--store-failures`, failed records are saved to the `staging_data_quality` schema.

### List all stored failure tables:
```bash
docker exec spotify_postgres psql -U postgres -d spotify_warehouse -c \
"\dt staging_data_quality.*"
```

### Query a specific failure table:
```sql
-- Example: View records that failed the not_null test on track_name
SELECT * FROM staging_data_quality.source_not_null_spotify_raw_tracks_track_name
LIMIT 100;
```

### Common failure tables:
| Table Name | What It Contains |
|------------|------------------|
| `source_not_null_spotify_raw_tracks_track_name` | Rows with NULL track names |
| `not_null_stg_spotify__tracks_album_name` | Rows with NULL album names |
| `dbt_expectations_*` | Failed expectation test records |

> **Note**: Tables only exist if tests actually failed. If all tests pass, no tables are created.

---

## Method 4: dbt run_results.json

**Best for**: Programmatic analysis, CI/CD integration

### View raw results:
```bash
docker exec spotify_airflow_webserver \
cat /opt/airflow/dbt/target/run_results.json | python3 -m json.tool | head -100
```

### Parse test results with Python:
```bash
docker exec spotify_airflow_webserver python3 -c "
import json
with open('/opt/airflow/dbt/target/run_results.json') as f:
    data = json.load(f)

for r in data['results']:
    if r.get('unique_id', '').startswith('test.'):
        status = r.get('status', 'unknown')
        failures = r.get('failures', 0) or 0
        name = r.get('unique_id', '').split('.')[-1]
        if status != 'pass':
            print(f'{status.upper()}: {name} ({failures} failures)')
"
```

### Key fields in run_results.json:
| Field | Description |
|-------|-------------|
| `status` | pass, fail, warn, error, skipped |
| `failures` | Number of failing rows |
| `unique_id` | Full test identifier |
| `execution_time` | How long the test took |
| `message` | Error message if applicable |

---

## Current Known Issues

Based on the latest pipeline run:

### Failed Tests (ERROR)
| Test | Failures | Root Cause |
|------|----------|------------|
| `source_not_null_spotify_raw_tracks_track_name` | 21 | 21 tracks in Kaggle source data have NULL track names |

### Warning Tests (WARN)
| Test | Failures | Root Cause |
|------|----------|------------|
| `not_null_stg_spotify__tracks_album_name` | 46 | 46 tracks have NULL album names in source |

### Investigation Query:
```sql
-- Find the tracks with NULL names in raw data
SELECT
    artist_name,
    album_name,
    genre,
    country,
    popularity,
    _loaded_at
FROM raw.spotify_tracks_raw
WHERE track_name IS NULL;
```

### Run from terminal:
```bash
# Find the 21 tracks with NULL names
docker exec spotify_postgres psql -U postgres -d spotify_warehouse -c \
"SELECT artist_name, album_name, genre, country
 FROM raw.spotify_tracks_raw WHERE track_name IS NULL;"
```

---

## Alerting & Monitoring

### Check test results via CLI:
```bash
# Quick status check
docker exec spotify_postgres psql -U postgres -d spotify_warehouse -c \
"SELECT
    COUNT(*) FILTER (WHERE passed = true) as passing,
    COUNT(*) FILTER (WHERE passed = false) as failing,
    COUNT(*) as total
FROM staging_marts.dq_metrics_summary;"
```

### Grafana Dashboard
Access the DQ dashboard at: http://localhost:3000
- Default credentials: admin / admin
- Dashboard: "Data Quality Overview"

---

## Reset & Re-run Pipeline

If you need to start fresh:

```bash
# Run the reset script
./scripts/reset_database.sh

# Trigger a new pipeline run
docker exec spotify_airflow_webserver airflow dags trigger spotify_etl_pipeline
```

---

## Test Severity Levels

| Severity | Behavior | Use Case |
|----------|----------|----------|
| `error` | Fails the dbt command | Critical data issues |
| `warn` | Logs warning, continues | Non-critical issues |

Configure in `dbt_project.yml`:
```yaml
tests:
  +severity: warn  # Default for all tests
  +store_failures: true
```

Override per-test in schema YAML:
```yaml
tests:
  - not_null:
      severity: error  # This specific test will fail on issues
```

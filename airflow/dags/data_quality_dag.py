"""
Data Quality Monitoring DAG

Dedicated DAG for continuous data quality monitoring that runs independently
of the main ETL pipeline. Generates metrics for Grafana dashboards.

Schedule: Every 4 hours
Purpose: Monitor data freshness, collect DQ metrics, check SLA compliance

Author: Data Engineering Team
Created: 2024
"""

from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import os


# =============================================================================
# DAG Configuration
# =============================================================================
default_args = {
    'owner': 'data_quality',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['dq-alerts@company.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

DBT_PROJECT_DIR = os.environ.get('DBT_PROFILES_DIR', '/opt/airflow/dbt')
POSTGRES_CONN_ID = 'spotify_postgres'
SLACK_CONN_ID = 'slack_dq_alerts'


# =============================================================================
# Python Callables
# =============================================================================
def collect_dq_metrics(**context: Any) -> Dict[str, Any]:
    """Collect and store data quality metrics for monitoring."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    run_id = context['run_id']
    metrics = {
        'run_id': run_id,
        'run_timestamp': datetime.now().isoformat(),
        'dimensions': {}
    }

    # -------------------------------------------------------------------------
    # Completeness Metrics
    # -------------------------------------------------------------------------
    completeness_query = """
    SELECT
        COUNT(*) as total_rows,
        COUNT(track_name) as track_name_count,
        COUNT(genre) as genre_count,
        COUNT(country) as country_count,
        COUNT(artists) as artists_count
    FROM staging_marts.fct_tracks
    """

    try:
        result = hook.get_first(completeness_query)
        if result and result[0] > 0:
            metrics['dimensions']['completeness'] = {
                'total_rows': result[0],
                'track_name_ratio': round(result[1] / result[0], 4) if result[0] > 0 else 0,
                'genre_ratio': round(result[2] / result[0], 4) if result[0] > 0 else 0,
                'country_ratio': round(result[3] / result[0], 4) if result[0] > 0 else 0,
            }
    except Exception as e:
        print(f"Error collecting completeness metrics: {e}")

    # -------------------------------------------------------------------------
    # Uniqueness Metrics
    # -------------------------------------------------------------------------
    uniqueness_query = """
    SELECT
        COUNT(*) as total,
        COUNT(DISTINCT track_id) as unique_tracks
    FROM staging_marts.fct_tracks
    """

    try:
        result = hook.get_first(uniqueness_query)
        if result and result[0] > 0:
            metrics['dimensions']['uniqueness'] = {
                'total_records': result[0],
                'unique_records': result[1],
                'duplicate_ratio': round(1 - (result[1] / result[0]), 6) if result[0] > 0 else 0
            }
    except Exception as e:
        print(f"Error collecting uniqueness metrics: {e}")

    # -------------------------------------------------------------------------
    # Accuracy Metrics
    # -------------------------------------------------------------------------
    accuracy_query = """
    SELECT
        COUNT(*) as total,
        COUNT(CASE WHEN popularity_score BETWEEN 0 AND 100 THEN 1 END) as valid_popularity,
        COUNT(CASE WHEN danceability BETWEEN 0 AND 1 THEN 1 END) as valid_danceability,
        COUNT(CASE WHEN energy BETWEEN 0 AND 1 THEN 1 END) as valid_energy
    FROM staging_marts.fct_tracks
    """

    try:
        result = hook.get_first(accuracy_query)
        if result and result[0] > 0:
            metrics['dimensions']['accuracy'] = {
                'popularity_valid_ratio': round(result[1] / result[0], 4) if result[0] > 0 else 0,
                'danceability_valid_ratio': round(result[2] / result[0], 4) if result[0] > 0 else 0,
                'energy_valid_ratio': round(result[3] / result[0], 4) if result[0] > 0 else 0,
            }
    except Exception as e:
        print(f"Error collecting accuracy metrics: {e}")

    # -------------------------------------------------------------------------
    # Store Metrics in DQ Tables
    # -------------------------------------------------------------------------
    insert_query = """
    INSERT INTO data_quality.dq_metrics
    (run_id, run_timestamp, dimension, table_name, metric_name, metric_value, passed)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    for dimension, values in metrics['dimensions'].items():
        for metric_name, value in values.items():
            if isinstance(value, (int, float)):
                threshold = 0.95 if 'ratio' in metric_name else None
                passed = value >= threshold if threshold else True
                try:
                    hook.run(insert_query, parameters=(
                        run_id,
                        datetime.now(),
                        dimension.capitalize(),
                        'fct_tracks',
                        metric_name,
                        value,
                        passed
                    ))
                except Exception as e:
                    print(f"Error inserting metric {metric_name}: {e}")

    print(f"Collected metrics: {metrics}")
    return metrics


def check_sla_compliance(**context: Any) -> Dict[str, Any]:
    """Check if data freshness meets SLA requirements."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # Check data freshness
    freshness_query = """
    SELECT
        MAX(dbt_loaded_at) as latest_load,
        EXTRACT(EPOCH FROM (NOW() - MAX(dbt_loaded_at))) / 3600 as hours_since_load
    FROM staging_marts.fct_tracks
    """

    try:
        result = hook.get_first(freshness_query)
        latest_load = result[0] if result else None
        hours_since_load = result[1] if result and result[1] else 999

        sla_hours = 24  # 24-hour SLA
        sla_met = hours_since_load <= sla_hours

        # Log SLA status
        insert_query = """
        INSERT INTO data_quality.sla_monitoring
        (pipeline_name, expected_completion_time, actual_completion_time, sla_met, deviation_minutes, dag_run_id)
        VALUES (%s, %s, %s, %s, %s, %s)
        """

        deviation = int((hours_since_load - sla_hours) * 60) if not sla_met else 0

        hook.run(insert_query, parameters=(
            'spotify_etl',
            '06:00:00',
            latest_load,
            sla_met,
            deviation,
            context['run_id']
        ))

        result_dict = {
            'sla_met': sla_met,
            'hours_since_load': round(hours_since_load, 2),
            'sla_hours': sla_hours,
            'latest_load': str(latest_load) if latest_load else None
        }

        if not sla_met:
            print(f"WARNING: SLA violated! Data is {hours_since_load:.1f} hours old (SLA: {sla_hours}h)")

        return result_dict

    except Exception as e:
        print(f"Error checking SLA: {e}")
        return {'sla_met': False, 'error': str(e)}


def send_alert_if_needed(**context: Any) -> Dict[str, Any]:
    """Send alerts for SLA violations or DQ issues."""
    ti = context['ti']

    sla_result = ti.xcom_pull(task_ids='check_sla_compliance')
    metrics_result = ti.xcom_pull(task_ids='collect_dq_metrics')

    alerts = []

    # Check SLA
    if sla_result and not sla_result.get('sla_met', True):
        alerts.append(f"SLA VIOLATION: Data is {sla_result.get('hours_since_load', 'unknown')} hours stale")

    # Check metrics thresholds
    if metrics_result:
        dimensions = metrics_result.get('dimensions', {})
        for dim, values in dimensions.items():
            for metric, value in values.items():
                if 'ratio' in metric and isinstance(value, (int, float)) and value < 0.90:
                    alerts.append(f"LOW {dim.upper()}: {metric} = {value:.2%}")

    result = {
        'has_alerts': len(alerts) > 0,
        'alert_count': len(alerts),
        'alerts': alerts
    }

    if alerts:
        print("=" * 60)
        print("ALERTS TRIGGERED:")
        for alert in alerts:
            print(f"  - {alert}")
        print("=" * 60)
    else:
        print("All checks passed - no alerts needed")

    return result


def build_slack_message(**context: Any) -> str:
    """Build Slack message from alert results."""
    ti = context['ti']
    alert_result = ti.xcom_pull(task_ids='send_alerts')

    if not alert_result or not alert_result.get('has_alerts'):
        return ":white_check_mark: *Data Quality Check Passed*\nAll metrics within acceptable thresholds."

    alerts = alert_result.get('alerts', [])
    alert_list = "\n".join([f"• {alert}" for alert in alerts])

    message = f"""
:warning: *Data Quality Alert*

*{len(alerts)} issue(s) detected:*
{alert_list}

*Pipeline:* spotify_etl
*Time:* {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
*Dashboard:* <http://localhost:3000|View Grafana Dashboard>
"""
    return message


# =============================================================================
# DAG Definition
# =============================================================================
with DAG(
    dag_id='data_quality_monitoring',
    default_args=default_args,
    description='Continuous data quality monitoring for Grafana dashboards',
    schedule_interval='0 */4 * * *',  # Every 4 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['data-quality', 'monitoring', 'sla'],
    max_active_runs=1,
    doc_md=__doc__,
) as dag:

    # =========================================================================
    # Task Definitions
    # =========================================================================
    start = EmptyOperator(task_id='start')

    collect_metrics = PythonOperator(
        task_id='collect_dq_metrics',
        python_callable=collect_dq_metrics,
        provide_context=True,
    )

    check_sla = PythonOperator(
        task_id='check_sla_compliance',
        python_callable=check_sla_compliance,
        provide_context=True,
    )

    run_dbt_tests = BashOperator(
        task_id='run_dbt_tests',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROJECT_DIR} --store-failures || true',
    )

    refresh_dq_summary = BashOperator(
        task_id='refresh_dq_summary',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROJECT_DIR} --select dq_metrics_summary',
    )

    send_alerts = PythonOperator(
        task_id='send_alerts',
        python_callable=send_alert_if_needed,
        provide_context=True,
    )

    slack_notification = SlackWebhookOperator(
        task_id='slack_notification',
        slack_webhook_conn_id=SLACK_CONN_ID,
        message="{{ ti.xcom_pull(task_ids='build_slack_message') }}",
        channel='#data-quality-alerts',
    )

    build_message = PythonOperator(
        task_id='build_slack_message',
        python_callable=build_slack_message,
        provide_context=True,
    )

    end = EmptyOperator(task_id='end')

    # =========================================================================
    # DAG Dependencies
    # =========================================================================
    start >> [collect_metrics, check_sla, run_dbt_tests]
    [collect_metrics, check_sla] >> send_alerts >> build_message >> slack_notification
    [run_dbt_tests, slack_notification] >> refresh_dq_summary >> end

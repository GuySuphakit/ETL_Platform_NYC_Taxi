from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
from airflow.exceptions import AirflowException
import logging

# Set up logger
logger = logging.getLogger(__name__)

# DAG parameters
DAG_ID = 'daily_top_five_taxi_zones'
SPARK_JOB_PATH = '/opt/airflow/dags/spark_jobs/calculate_top_taxi_zones.py'
DATA_PATH = '/opt/airflow/spark-data/tripdata/'
DB_CONN_ID = 'postgres_default'
SPARK_CONN_ID = 'spark_default'

# Get database details from Airflow Variables
DB_NAME = Variable.get("db_name", "airflow")
TABLE_NAME = Variable.get("top_taxi_zones_table", "daily_topfive_taxi_zone")

DEFAULT_ARGS = {
    'owner': 'suphakit',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

SQL_CREATE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    taxi_zone_id BIGINT,
    rank INT,
    calculated_at TIMESTAMP
)
"""

def _handle_error(context):
    logger.error(f"Task failed: {context['task_instance'].task_id}")
    raise AirflowException("Task failed. Check logs for details.")

def _log_task_success(context):
    logger.info(f"Task {context['task_instance'].task_id} in DAG {context['dag'].dag_id} completed successfully")

with DAG(
    DAG_ID,
    default_args=DEFAULT_ARGS,
    description='Calculate daily top 5 TLC Taxi Zones where trips began',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['hvfhv', 'daily', 'top_taxi_zones'],
) as dag:

    check_for_data = FileSensor(
        task_id='check_for_data',
        filepath=f"{DATA_PATH}fhvhv_tripdata_{{{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m') }}}}.parquet",
        poke_interval=300,
        timeout=1800,
        mode='poke',
        on_failure_callback=_handle_error,
    )

    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id=DB_CONN_ID,
        sql=SQL_CREATE_TABLE,
        on_failure_callback=_handle_error,
        on_success_callback=_log_task_success,
    )

    calculate_top_zones = SparkSubmitOperator(
        task_id='calculate_top_taxi_zones',
        application=SPARK_JOB_PATH,
        conn_id=SPARK_CONN_ID,
        application_args=["{{ ds }}", DATA_PATH, DB_CONN_ID, TABLE_NAME],
        conf={
            "spark.driver.maxResultSize": "2g",
            "spark.network.maxFrameSize": "200m",
            "spark.jars": "/opt/airflow/jars/postgresql-42.7.4.jar",
            "spark.driver.extraClassPath": "/opt/airflow/jars/postgresql-42.7.4.jar",
            "spark.executor.extraClassPath": "/opt/airflow/jars/postgresql-42.7.4.jar"
        },
        on_failure_callback=_handle_error,
        on_success_callback=_log_task_success,
        verbose=True
    )

    dag.doc_md = __doc__  # Add docstring to DAG documentation
    dag.on_failure_callback = _handle_error
    dag.on_success_callback = _log_task_success

    check_for_data >> create_table >> calculate_top_zones

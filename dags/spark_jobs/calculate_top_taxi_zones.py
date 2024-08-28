import sys
from datetime import datetime, timedelta, date
import logging
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_date, col, count, dense_rank, current_timestamp
from pyspark.sql.window import Window
from pyspark.sql.types import StructType
from airflow.hooks.base import BaseHook

# Constants
PICKUP_DATETIME_COL = "pickup_datetime"
PULOCATION_ID_COL = "PULocationID"
TOP_N_ZONES = 5

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_file_schema(spark: SparkSession, file_path: str) -> StructType:
    """Get the schema of a Parquet file."""
    return spark.read.parquet(file_path).schema

def create_dataframe_with_schema(spark: SparkSession, file_path: str, schema: StructType) -> DataFrame:
    """Create a DataFrame from a Parquet file using the provided schema."""
    return spark.read.schema(schema).parquet(file_path)

def process_dataframe(df: DataFrame, cutoff_date: date) -> DataFrame:
    """Process a DataFrame to get the top taxi zones."""
    return (df.filter(to_date(col(PICKUP_DATETIME_COL)) <= cutoff_date)
              .groupBy(PULOCATION_ID_COL)
              .agg(count("*").alias("trip_count"))
              .withColumn("rank", dense_rank().over(Window.orderBy(col("trip_count").desc())))
              .filter(col("rank") <= TOP_N_ZONES))

def get_parquet_files(data_path: str) -> List[str]:
    """Get all Parquet files in the given directory."""
    import os
    return [os.path.join(data_path, f) for f in os.listdir(data_path) if f.endswith('.parquet')]

def write_to_database(df: DataFrame, db_conn_id: str, table_name: str) -> None:
    """Write DataFrame to the database."""
    conn = BaseHook.get_connection(db_conn_id)
    port = conn.port or 5432
    jdbc_url = f"jdbc:postgresql://{conn.host}:{port}/{conn.schema}"

    df.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", conn.login) \
        .option("password", conn.password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

def calculate_top_taxi_zones(spark: SparkSession, execution_date: datetime, data_path: str, db_conn_id: str, table_name: str) -> None:
    """Calculate the top 5 TLC Taxi Zones where trips began for all data up to the day before the execution date."""
    logger.info(f"Starting calculation for execution_date: {execution_date}")

    try:
        cutoff_date = execution_date.date() - timedelta(days=1)
        logger.info(f"Calculating top taxi zones for data up to: {cutoff_date}")

        parquet_files = get_parquet_files(data_path)
        if not parquet_files:
            raise ValueError(f"No Parquet files found in {data_path}")

        all_results = []
        for file in parquet_files:
            logger.info(f"Processing file: {file}")
            schema = get_file_schema(spark, file)
            df = create_dataframe_with_schema(spark, file, schema)
            result = process_dataframe(df, cutoff_date)
            all_results.append(result)

        final_result = all_results[0]
        for result in all_results[1:]:
            final_result = final_result.union(result)

        top_5_zones = final_result.select(
            col(PULOCATION_ID_COL).alias("taxi_zone_id"),
            col("rank"),
            current_timestamp().alias("calculated_at")
        ).orderBy("rank")

        write_to_database(top_5_zones, db_conn_id, table_name)
        logger.info(f"Data successfully written to {table_name} for date {cutoff_date}")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("DailyTopFiveTaxiZones")
             .master("local[*]")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.driver.extraClassPath", "/opt/airflow/jars/postgresql-42.7.4.jar")
             .config("spark.executor.extraClassPath", "/opt/airflow/jars/postgresql-42.7.4.jar")
             .getOrCreate())

    execution_date = datetime.strptime(sys.argv[1], "%Y-%m-%d")
    data_path = sys.argv[2]
    db_conn_id = sys.argv[3]
    table_name = sys.argv[4]

    calculate_top_taxi_zones(spark, execution_date, data_path, db_conn_id, table_name)
    spark.stop()

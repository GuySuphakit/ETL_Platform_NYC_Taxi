import sys
from datetime import datetime, date, timedelta
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_date, col, count
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, LongType
from airflow.hooks.base import BaseHook

# Constants
PICKUP_DATETIME_COL = "pickup_datetime"
TRANSACTION_DATE_COL = "transaction_date"
TOTAL_TRANSACTIONS_COL = "total_transactions"

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_custom_schema() -> StructType:
    """Define and return the custom schema for the Parquet files."""
    return StructType([
        StructField("hvfhs_license_num", StringType(), True),
        StructField("dispatching_base_num", StringType(), True),
        StructField("originating_base_num", StringType(), True),
        StructField("request_datetime", TimestampType(), True),
        StructField("on_scene_datetime", TimestampType(), True),
        StructField("pickup_datetime", TimestampType(), True),
        StructField("dropoff_datetime", TimestampType(), True),
        StructField("PULocationID", LongType(), True),
        StructField("DOLocationID", LongType(), True),
        StructField("trip_miles", DoubleType(), True),
        StructField("trip_time", LongType(), True),
        StructField("base_passenger_fare", DoubleType(), True),
        StructField("tolls", DoubleType(), True),
        StructField("bcf", DoubleType(), True),
        StructField("sales_tax", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("airport_fee", DoubleType(), True),
        StructField("tips", DoubleType(), True),
        StructField("driver_pay", DoubleType(), True),
        StructField("shared_request_flag", StringType(), True),
        StructField("shared_match_flag", StringType(), True),
        StructField("access_a_ride_flag", StringType(), True),
        StructField("wav_request_flag", StringType(), True),
        StructField("wav_match_flag", StringType(), True)
    ])

def read_parquet_files(spark: SparkSession, data_path: str, schema: StructType) -> DataFrame:
    """Read Parquet files using the provided schema."""
    return spark.read.schema(schema).parquet(f"{data_path}fhvhv_tripdata_*.parquet")

def process_dataframe(df: DataFrame, process_date: date) -> DataFrame:
    """Process DataFrame to calculate daily transactions."""
    return (df.withColumn(TRANSACTION_DATE_COL, to_date(col(PICKUP_DATETIME_COL)))
              .filter(col(TRANSACTION_DATE_COL) == process_date)
              .groupBy(TRANSACTION_DATE_COL)
              .agg(count("*").alias(TOTAL_TRANSACTIONS_COL)))

def create_result_dataframe(spark: SparkSession, process_date: date, total_transactions: int) -> DataFrame:
    """Create a result DataFrame with the calculated transactions."""
    return spark.createDataFrame([
        (process_date, total_transactions, datetime.now())
    ], ["transaction_date", "total_transactions", "calculated_at"])

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
        .mode("append") \
        .save()

def calculate_daily_transactions(spark: SparkSession, execution_date: datetime, data_path: str, db_conn_id: str, table_name: str) -> None:
    """Calculate daily transactions for the day before the execution date."""
    logger.info(f"Starting calculation for execution_date: {execution_date}")

    try:
        process_date = execution_date.date() - timedelta(days=1)
        logger.info(f"Calculating transactions for date: {process_date}")

        schema = get_custom_schema()
        df = read_parquet_files(spark, data_path, schema)

        if df.rdd.isEmpty():
            raise ValueError("No data found in the input files")

        result_df = process_dataframe(df, process_date)
        final_result = result_df.collect()

        if final_result:
            total_transactions = final_result[0][TOTAL_TRANSACTIONS_COL]
            if total_transactions < 0:
                raise ValueError(f"Negative transaction count found: {total_transactions}")
        else:
            logger.warning(f"No data found for date: {process_date}")
            total_transactions = 0

        result_df = create_result_dataframe(spark, process_date, total_transactions)
        write_to_database(result_df, db_conn_id, table_name)

        logger.info(f"Data successfully written to {table_name} for date {process_date}")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("DailyTotalTransactions")
             .master("local[*]")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.parquet.mergeSchema", "true")
             .config("spark.driver.extraClassPath", "/opt/airflow/jars/postgresql-42.7.4.jar")
             .config("spark.executor.extraClassPath", "/opt/airflow/jars/postgresql-42.7.4.jar")
             .getOrCreate())

    execution_date = datetime.strptime(sys.argv[1], "%Y-%m-%d")
    data_path = sys.argv[2]
    db_conn_id = sys.argv[3]
    table_name = sys.argv[4]

    calculate_daily_transactions(spark, execution_date, data_path, db_conn_id, table_name)
    spark.stop()

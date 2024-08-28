# ETL Platform for NYC Taxi Data

This project contains two ETL (Extract, Transform, Load) pipelines for processing NYC High Volume For-Hire Vehicle Trip Records. The pipelines are designed to run daily and calculate:

1. Daily total transactions
2. Top 5 TLC Taxi Zones where trips began

## Prerequisites

- Docker
- Docker Compose

## Project Structure

```
.
├── Dockerfile
├── README.md
├── airflow.cfg
├── dags/
│   ├── daily_topfive_taxi_zone_dag.py
│   ├── daily_transactions_dag.py
│   └── spark_jobs/
│       ├── calculate_daily_transactions.py
│       └── calculate_top_taxi_zones.py
├── docker-compose.yml
├── jars/
│   └── postgresql-42.7.4.jar
├── logs/
├── plugins/
├── requirements.txt
└── spark-data/
    └── tripdata/
```

## Setup and Installation

1. Clone the repository:

   ```
   git clone <repository-url>
   cd <repository-name>
   ```

2. Build and start the Docker containers:

   ```
   docker-compose up -d --build
   ```

   This command will build the custom Airflow image and start all necessary services defined in the `docker-compose.yml` file.

3. The Airflow webserver will be available at `http://localhost:8080`. The default username and password are typically `airflow` unless changed in the configuration.

## Data Preparation

Ensure your NYC taxi trip data (in Parquet format) is placed in the `spark-data/tripdata/` directory. The files should be named in the format `fhvhv_tripdata_YYYY-MM.parquet`.

## Usage

1. Access the Airflow web interface at `http://localhost:8080`

2. Enable the DAGs:
   - `daily_total_transactions`
   - `daily_top_five_taxi_zones`

The DAGs are scheduled to run daily. They will process the data for the previous day and store the results in the specified PostgreSQL tables.

## DAG Details

### 1. Daily Total Transactions

- **DAG ID:** `daily_total_transactions`
- **Schedule:** Daily
- **Script:** `calculate_daily_transactions.py`
- **Output Table:** `daily_transaction`

This DAG calculates the total number of taxi trips for the previous day.

### 2. Top 5 TLC Taxi Zones

- **DAG ID:** `daily_top_five_taxi_zones`
- **Schedule:** Daily
- **Script:** `calculate_top_taxi_zones.py`
- **Output Table:** `daily_topfive_taxi_zone`

This DAG calculates the top 5 TLC Taxi Zones where trips began, based on all historical data up to the previous day.

## Configuration

- The `airflow.cfg` file contains the Airflow configuration. Modify this file if you need to change any Airflow settings.
- The `requirements.txt` file lists all Python dependencies. If you need to add more packages, add them to this file and rebuild the Docker image.
- The PostgreSQL JDBC driver is located in the `jars/` directory and is automatically made available to the Spark jobs.

## Logs

Airflow logs are stored in the `logs/` directory. You can access these logs for debugging purposes.

## Important Notes

- Ensure that your Parquet files are named in the format `fhvhv_tripdata_YYYY-MM.parquet`
- The scripts assume that the `pickup_datetime` column is present in your data files
- The Top 5 Taxi Zones calculation overwrites the entire table each day, while the Daily Transactions appends new records

## Troubleshooting

- If you encounter any issues, check the Airflow logs in the `logs/` directory
- Ensure that the `spark-data/tripdata/` directory contains the correct Parquet files
- If you need to modify any Spark configurations, you can do so in the respective DAG files

## Stopping the Application

To stop and remove the Docker containers, run:

```
docker-compose down
```

## Contributing

Please read CONTRIBUTING.md for details on our code of conduct, and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the LICENSE.md file for details

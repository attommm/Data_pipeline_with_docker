from datetime import datetime, timedelta
from airflow.decorators import dag, task
import pandas as pd
from pathlib import Path

import sys
sys.path.append("/opt/airflow")

from scripts.pipeline import extract, transform, connect_db, create_table, insert_data, logger

base_path = Path("/opt/airflow/data")

from scripts.pipeline import extract, transform, connect_db, create_table, insert_data
from scripts.pipeline import logger

default_args = {
    "owner": 'Nifemi',
    'retries': 2,
    "retry_delay": timedelta(minutes=5)
}

@dag(
    dag_id = "taxi_zone_lookup_dag",
    default_args = default_args,
    description = "A DAG to load taxi zone lookup data into Postgres",
    schedule_interval = "@daily",
    start_date = datetime(2026, 4, 7)
)

def run_pipeline():

    @task()
    def extract_data():
        input_path      = base_path / "taxi_zone_lookup.csv"
        output_path     = base_path / "extracted_taxi_zone_lookup.csv"

        if not input_path.exists():
            logger.error(f"Input file {input_path} does not exist. Aborting extraction.")
            raise FileNotFoundError(f"Input file {input_path} not found.")

        df = extract(input_path)
        df.to_csv(output_path, index=False)
        return str(output_path)
    
    @task()
    def transform_data(input_path: str):
        input_path     = Path(input_path)
        output_path    = base_path / "transformed_taxi_zone_lookup.csv"
        df  = pd.read_csv(input_path)
        df  = transform(df)
        df.to_csv(output_path, index=False)
        return str(output_path)    
    
    @task()
    def load_data(file_path: str):
        pg_conn = connect_db()
        if pg_conn is None:
            logger.error("Failed to connect to Postgres. Aborting load.")
            return
        
        pg_cursor = pg_conn.cursor()

        df = pd.read_csv(file_path)

        create_table(pg_cursor, pg_conn)
        insert_data(df, pg_cursor, pg_conn )
        pg_cursor.close()
        pg_conn.close()
    

    df = extract_data()
    transformed_df = transform_data(df)
    load_data(transformed_df)

run_pipeline()

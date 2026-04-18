# from dotenv import load_dotenv
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
import time
import logging
from pathlib import Path

log_path = Path("/opt/airflow/logs")
log_path.mkdir(parents=True, exist_ok=True)
# load_dotenv()
logging.basicConfig(
    filename= log_path / 'etl_pipeline.log',
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding='utf-8'
)

logger = logging.getLogger()


## Postgres connection parameters
PG_HOST         = os.getenv("PG_HOST")
PG_PORT         = os.getenv("PG_PORT")
PG_DATABASE     = os.getenv("PG_DATABASE")
PG_USER         = os.getenv("PG_USER")
PG_PASSWORD     = os.getenv("PG_PASSWORD")

## extract data from CSV file and prepare for insertion

# file_path = "taxi_zone_lookup.csv"

def extract(file_path):
    try:
        df = pd.read_csv(file_path)
        return df
    except FileNotFoundError:                   
        print(f"File not found: {file_path}")
        return pd.DataFrame()



def transform(df):
    try:
        old_len = len(df)

        df = df.dropna(how="any")

        new_len = len(df)
        print(f"Removed {old_len - new_len} rows with missing values.")
        return df
    except Exception as e:
        print(f"Error during transformation: {e}")
        return df

def connect_db():
    while True:
        try:
            conn = psycopg2.connect(
                host        =PG_HOST,
                port        =PG_PORT,
                database    =PG_DATABASE,
                user        =PG_USER,
                password    =PG_PASSWORD    
                )
            logger.info("Successfully connected to Postgres.")
            print("Successfully connected to Postgres.")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to Postgres: {e}")
            time.sleep(5)
            return None


# pg_conn = connect_db()
# pg_cursor = pg_conn.cursor()

def create_table(pg_cursor, pg_conn):
    try:
        create_table_query = """
            CREATE TABLE IF NOT EXISTS taxi_zone (
                LocationID INTEGER PRIMARY KEY,
                Borough VARCHAR(100), 
                Zone VARCHAR(100), 
                service_zone VARCHAR(100)
        )
        """

        pg_cursor.execute(create_table_query)
        pg_conn.commit()

        logger.info("Table 'taxi_zone' created successfully.")
    except Exception as e: 
        logger.error(f"Error creating table: {e}") 

def insert_data(df, pg_cursor, pg_conn):
    try:
        rows = [tuple(row) for row in df.to_numpy()]

        insert_query = """
            INSERT INTO taxi_zone
            VALUES
            %s
            ON CONFLICT (LocationID) DO NOTHING 
        """

        execute_values(pg_cursor, insert_query, rows)
        pg_conn.commit()

        logger.info(f"Inserted {len(rows)} rows into the taxi_zone table.")
    except Exception as e:
        logger.error(f"Error inserting data: {e}")

def main():
    logger.info("Starting ETL pipeline.")

    df = extract(file_path)

    logger.info(f"Extracted {len(df)} rows from the CSV file.")
    
    if df.empty:
        logger.warning("No data to process.")
        return

    df = transform(df)
    if df.empty:
        logger.warning("No valid data after transformation.")
        return

    create_table()

    logger.info("Ensured the taxi_zone table exists.")

    insert_data(df)

    logger.info("ETL completed successfully.")

if __name__ == "__main__":
    file_path = "taxi_zone_lookup.csv"
    main()

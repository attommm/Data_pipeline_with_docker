* DOCKERIZED ETL PIPELINE

** OVERVIEW

This project demonstrates a simple ETL (Extract, Transform, Load) pipeline built with Python and containerized using Docker.

The pipeline:

Extracts data from a CSV file
Transforms it using pandas
Loads it into a PostgreSQL database

** TECH STACK

Python (pandas, psycopg2)
PostgreSQL
Docker & Docker Compose

** HOW TO RUN

docker compose run --build

** FEATURES

Containerized ETL pipeline
PostgreSQL running in Docker
Idempotent loads (no duplicate records)
Logging for observability
Environment-based configuration

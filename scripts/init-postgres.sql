-- Enterprise Data Lakehouse - PostgreSQL Database Initialization

-- Airflow
CREATE DATABASE airflow;
CREATE USER airflow WITH PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

-- Hive Metastore
CREATE DATABASE hive_metastore;
GRANT ALL PRIVILEGES ON DATABASE hive_metastore TO admin;

-- Superset
CREATE DATABASE superset;
GRANT ALL PRIVILEGES ON DATABASE superset TO admin;

-- DataHub (if using PG instead of MySQL)
CREATE DATABASE datahub_pg;
GRANT ALL PRIVILEGES ON DATABASE datahub_pg TO admin;

-- Application metadata
CREATE DATABASE lakehouse_meta;
GRANT ALL PRIVILEGES ON DATABASE lakehouse_meta TO admin;

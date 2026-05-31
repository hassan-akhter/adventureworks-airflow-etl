#!/bin/bash
# Creates the adventureworks database on first container start.
# Mounted to /docker-entrypoint-initdb.d/ — runs automatically.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE adventureworks;
    GRANT ALL PRIVILEGES ON DATABASE adventureworks TO $POSTGRES_USER;
EOSQL

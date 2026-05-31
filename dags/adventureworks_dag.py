"""
AdventureWorks ETL DAG
======================
Orchestrates a four-stage pipeline that moves AdventureWorks data from raw CSV files into a normalised PostgreSQL schema.

Stages
------
  extract   → parse 10 raw CSV files → data/processed/
  transform → type-cast, null-normalise, deduplicate in place
  load      → TRUNCATE + bulk-load into PostgreSQL (FK order)
  summary   → log final row counts for observability

Schedule: daily, no backfill
Retry: 1 attempt with a 5-minute delay before alerting
"""

from __future__ import annotations

import logging
import sys
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task

# Make scripts/ importable inside Airflow workers
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "hassan-akhter",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


@dag(
    dag_id="adventureworks_etl",
    description=(
        "End-to-end ETL: AdventureWorks CSVs → PostgreSQL "
        "(10 tables · 120K+ rows)"
    ),
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["adventureworks", "etl", "postgresql"],
)
def adventureworks_etl() -> None:

    @task(task_id="extract")
    def extract() -> dict[str, int]:
        from extract import extract_all
        counts = extract_all()
        logger.info(
            "Extract complete — %d total rows from %d tables",
            sum(counts.values()), len(counts),
        )
        return counts

    @task(task_id="transform")
    def transform(extract_counts: dict[str, int]) -> dict[str, int]:
        from transform import transform_all
        counts = transform_all()
        logger.info(
            "Transform complete — %d clean rows across %d tables",
            sum(counts.values()), len(counts),
        )
        return counts

    @task(task_id="load")
    def load(transform_counts: dict[str, int]) -> dict[str, int]:
        from load import load_all
        counts = load_all()
        logger.info(
            "Load complete — %d rows persisted across %d tables",
            sum(counts.values()), len(counts),
        )
        return counts

    @task(task_id="summary")
    def summary(load_counts: dict[str, int]) -> None:
        total = sum(load_counts.values())
        logger.info(
            "Pipeline finished — %d total rows loaded across %d tables",
            total, len(load_counts),
        )
        for table, n in load_counts.items():
            logger.info("  %-25s %7d rows", table, n)

    counts_e = extract()
    counts_t = transform(counts_e)
    counts_l = load(counts_t)
    summary(counts_l)


adventureworks_etl()

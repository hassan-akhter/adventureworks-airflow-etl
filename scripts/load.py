"""
Load layer — reads the cleaned processed CSVs and bulk-loads them into PostgreSQL using psycopg2's execute_values for high-performance batch inserts.

pandas.to_sql() is not used here because pandas 2.x dropped support for SQLAlchemy 1.4 connections; direct psycopg2 inserts avoid the version mismatch entirely and are significantly faster.

Set POSTGRES_CONN in your environment to override the default:
    postgresql://airflow:airflow@localhost:5432/adventureworks
"""

import logging
import os
from urllib.parse import urlparse

import pandas as pd
import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

_BASE = os.path.join(os.path.dirname(__file__), "..")
PROCESSED_DIR = os.path.normpath(os.path.join(_BASE, "data", "processed"))

DEFAULT_CONN = "postgresql://airflow:airflow@localhost:5432/adventureworks"

LOAD_ORDER: list[tuple[str, str]] = [
    ("CountryRegion",      "countryregion"),
    ("StateProvince",      "stateprovince"),
    ("Address",            "address"),
    ("Person",             "person"),
    ("ProductCategory",    "productcategory"),
    ("ProductSubcategory", "productsubcategory"),
    ("Product",            "product"),
    ("Customer",           "customer"),
    ("SalesOrderHeader",   "salesorderheader"),
    ("SalesOrderDetail",   "salesorderdetail"),
]

PAGE_SIZE = 10_000


def _parse_dsn(conn_str: str) -> dict:
    """Convert a postgresql:// URL into a psycopg2 connect() kwarg dict."""
    u = urlparse(conn_str)
    return {
        "host":     u.hostname,
        "port":     u.port or 5432,
        "dbname":   u.path.lstrip("/"),
        "user":     u.username,
        "password": u.password,
    }


def _get_conn_str() -> str:
    return os.environ.get("POSTGRES_CONN", DEFAULT_CONN)


def load_table(file_name: str, db_table: str) -> int:
    """TRUNCATE then bulk-load one table via psycopg2 execute_values."""
    path = os.path.join(PROCESSED_DIR, f"{file_name}.csv")
    df = pd.read_csv(path, low_memory=False)

    # Convert all numpy scalars and NaN/NaT to plain Python types / None
    df = df.astype(object).where(pd.notnull(df), None)
    cols = list(df.columns)
    records = [tuple(row) for row in df.itertuples(index=False)]

    col_str = ", ".join(f'"{c}"' for c in cols)
    dsn = _parse_dsn(_get_conn_str())
    conn = psycopg2.connect(**dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {db_table} CASCADE")
            if records:
                psycopg2.extras.execute_values(
                    cur,
                    f'INSERT INTO {db_table} ({col_str}) VALUES %s',
                    records,
                    page_size=PAGE_SIZE,
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info("Loaded %-20s → %-20s %7d rows", file_name, db_table, len(df))
    return len(df)


def load_all() -> dict[str, int]:
    """
    Load all 10 tables into PostgreSQL in FK dependency order.
    Returns a dict of {db_table: row_count}.
    """
    counts: dict[str, int] = {}

    for file_name, db_table in LOAD_ORDER:
        counts[db_table] = load_table(file_name, db_table)

    total = sum(counts.values())
    logger.info("Load complete — %d rows across %d tables", total, len(counts))
    return counts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
    result = load_all()
    print("\nLoad summary:")
    for table, n in result.items():
        print(f"  {table:<25} {n:>7,} rows")
    print(f"  {'TOTAL':<25} {sum(result.values()):>7,} rows")

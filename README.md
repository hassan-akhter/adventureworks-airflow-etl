# AdventureWorks ETL Pipeline

End-to-end data engineering pipeline that ingests the AdventureWorks dataset from raw CSV files, cleans and type-casts the data in Python, and loads it into a normalised PostgreSQL schema — all orchestrated by Apache Airflow running in Docker.

**10 tables · 213K+ rows · 4 pipeline stages · 10 analytics queries · Executive Business Report**

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Apache Airflow DAG                               │
│              adventureworks_etl  ·  schedule: @daily                    │
└─────────────────────────────────────────────────────────────────────────┘
         │                │                │                │
    [extract]        [transform]        [load]         [summary]
         │                │                │                │
         ▼                ▼                ▼                ▼
  data/raw/          data/processed/   PostgreSQL      Airflow
  10 raw CSVs   →   typed & cleaned →  10 tables    →  task logs
  (no headers,      CSVs with named    FK constraints
   mixed delims)    snake_case cols    indexed joins
```

### Schema — 4 Domains, 10 Tables

```
Geography                 Product                   Sales
──────────────────        ──────────────────────    ──────────────────────────
countryregion             productcategory           salesorderheader 
stateprovince             productsubcategory        salesorderdetail  
address                   product

Customer
──────────────────
person
customer
```

---

## Dataset

| Table | Rows | Description |
|---|---|---|
| `salesorderdetail` | 121,317 | Individual line items per order |
| `salesorderheader` | 31,465 | Order-level metadata, totals, dates |
| `customer` | 19,820 | Customer accounts |
| `person` | 19,972 | Full name and contact info |
| `address` | 19,614 | Shipping and billing addresses |
| `stateprovince` | 181 | State/province lookup |
| `product` | 504 | Product catalogue with pricing |
| `productsubcategory` | 37 | 37 subcategories |
| `countryregion` | 238 | ISO country codes |
| `productcategory` | 4 | Bikes · Components · Clothing · Accessories |
| **Total** | **~213K** | |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.9.1 (TaskFlow API) |
| Data processing | Python 3.11 · pandas 2.2 · NumPy 1.26 |
| Database | PostgreSQL 15 |
| Loader | psycopg2 2.9 (execute_values bulk insert) |
| Infrastructure | Docker · Docker Compose |

---

## Pipeline Stages

### 1. Extract (`scripts/extract.py`)
- Reads 10 raw CSV files from `data/raw/`
- Handles two source formats: tab-separated (9 tables) and a custom `+|` / `&|` delimiter format (`Person.csv`)
- Assigns snake_case column names from a central `COLUMN_DEFS` mapping
- Writes standardised CSVs with headers to `data/processed/`

### 2. Transform (`scripts/transform.py`)
- Parses timestamps with `pd.to_datetime(errors="coerce")` — invalid dates become `NULL` rather than crashing the run
- Casts integer and float columns via `pd.to_numeric(errors="coerce")`
- Maps `"1"/"0"` strings to `True/False` for boolean columns
- Strips leading/trailing whitespace; replaces blank strings with `NULL`
- Drops rows missing their primary key and removes exact duplicates

### 3. Load (`scripts/load.py`)
- Connects to PostgreSQL via `POSTGRES_CONN` environment variable
- `TRUNCATE TABLE … CASCADE` before each load — fully idempotent, safe to re-trigger
- Loads in FK dependency order (parent tables before child tables)
- Bulk-inserts in batches of 10,000 rows via `psycopg2.extras.execute_values`

### 4. DAG (`dags/adventureworks_dag.py`)
- TaskFlow API (`@dag` / `@task` decorators) — clean, testable task functions
- Task outputs passed as XCom dicts for downstream logging
- 1 retry with a 5-minute delay before failure
- Tagged `adventureworks`, `etl`, `postgresql` for easy filtering in the Airflow UI

---

## Analytics Queries (`sql/queries.sql`)

Ten queries answer business questions across revenue, products, customers, and geography:

| # | Query | Insight |
|---|---|---|
| 1 | Revenue by year | Year-over-year growth trend |
| 2 | Top 10 products by quantity | Highest-volume SKUs |
| 3 | Revenue by product category | Bikes vs Components vs Clothing vs Accessories |
| 4 | Online vs in-store orders | Channel split by order count and revenue |
| 5 | Top 10 customers by lifetime value | Identify highest-value accounts |
| 6 | Monthly revenue trend | Seasonality and demand patterns |
| 7 | Sales by country / region | Geographic revenue distribution |
| 8 | Products with no sales | Inventory with zero demand |
| 9 | Average discount by category | Discount strategy by product segment |
| 10 | Orders per customer distribution | Single-purchase vs repeat customer ratio |

---

## Business Insights & Executive Analysis

Beyond building the ETL pipeline, the loaded data was analysed using SQL to uncover business trends across revenue, customers, products, channels, and geography.

## Key Findings

| Finding | Insight |
|---|---|
| Revenue Growth | Revenue increased from **$16.3M in 2022** to **$49.0M in 2024**, demonstrating strong growth over the analysed period. |
| Category Concentration | **Bikes generated 87% of total revenue**, creating significant dependency on a single product category. |
| Customer Retention | **61% of customers made only one purchase**, indicating substantial opportunity for retention and re-engagement strategies. |
| High-Value Customers | **The top 10 customers contributed approximately $9M** in lifetime revenue, highlighting the importance of key account management. |
| Product Performance | **238 of 504 products (47%) recorded zero sales**, suggesting opportunities for catalogue optimisation. |
| Sales Channels | In-store transactions generated significantly higher average order values than online orders despite representing a smaller share of order volume. |
| Geographic Distribution | 57% of revenue originated from the United States, while European markets showed potential for further growth. |

## Executive Report

A CEO-style executive report was created from the analytical findings to demonstrate how technical data engineering outputs can be translated into actionable business recommendations.

**Report: reports/AdventureWorks_CEO_Report.pdf**

## Recommended Actions

1. Investigate the slowdown in 2025 revenue growth.
2. Launch customer retention campaigns targeting one-time buyers.
3. Introduce dedicated account management for high-value customers.
4. Review and rationalise inactive products.
5. Expand commercial efforts in underpenetrated European markets.

This project demonstrates the complete data lifecycle: ingestion, transformation, warehousing, analytics, and business decision support.

---
## Quick Start

### Prerequisites
- Docker ≥ 24 and Docker Compose ≥ 2.20

### 1 — Get the data

Download the AdventureWorks dataset from the official Microsoft source:

- [AdventureWorks CSV files — Microsoft SQL Server Samples](https://github.com/Microsoft/sql-server-samples/tree/master/samples/databases/adventure-works)

Place the CSV files in `data/raw/` before running the pipeline.

### 2 — Clone and start containers

```bash
git clone https://github.com/hassan-akhter/adventureworks-airflow-etl.git
cd adventureworks-airflow-etl

docker compose up airflow-init   # one-time DB migration + admin user
docker compose up -d             # start Postgres, webserver, scheduler
```

### 3 — Set up the PostgreSQL schema

```bash
# Connect to the adventureworks database and run the DDL
docker exec -it <postgres_container_id> \
  psql -U airflow -d adventureworks -f /dev/stdin < sql/create_tables.sql
```

Or from your local machine (requires `psql`):

```bash
psql -h localhost -U airflow -d adventureworks -f sql/create_tables.sql
```

Password: `airflow`

### 4 — Trigger the pipeline

Open the Airflow UI at **http://localhost:8080** (admin / admin), enable the `adventureworks_etl` DAG, and trigger a run manually.

The four tasks run in sequence:

```
extract → transform → load → summary
```

Total run time: ~2–3 minutes on a standard laptop.

### 5 — Run analytics queries

```bash
psql -h localhost -U airflow -d adventureworks -f sql/queries.sql
```

---

## Project Structure

```
adventureworks-airflow-etl/
├── dags/
│   └── adventureworks_dag.py     # Airflow DAG (TaskFlow API)
├── scripts/
│   ├── extract.py                # Parse raw CSVs → data/processed/
│   ├── transform.py              # Type-cast, clean, deduplicate
│   └── load.py                   # Bulk-load into PostgreSQL
├── sql/
│   ├── create_tables.sql         # DDL — 10 tables with FK constraints + indexes
│   └── queries.sql               # 10 analytics queries
├── data/
│   ├── raw/                      # Source CSV files (committed)
│   └── processed/                # Pipeline intermediate output (gitignored)
├── reports
│    └── AdventureWorks_CEO_Report.pdf   # Executive Business Report
├── docker/
│   └── init_db.sh                # Creates adventureworks DB on first start
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── .env.example
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_CONN` | `postgresql://airflow:airflow@localhost:5432/adventureworks` | SQLAlchemy connection string used by `load.py` |

Copy `.env.example` to `.env` and update values as needed.

---

## Running Scripts Locally (without Docker)

```bash
pip install -r requirements.txt

# Set the connection string
export POSTGRES_CONN="postgresql://user:pass@localhost:5432/adventureworks"

# Run each stage individually
python scripts/extract.py
python scripts/transform.py
python scripts/load.py
```

Each script prints a row-count summary on completion.

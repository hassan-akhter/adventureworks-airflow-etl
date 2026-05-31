"""
Extract layer — reads raw AdventureWorks CSV files and writes standardised, column-named CSVs to data/processed/.

Raw files have no header row.  Most are tab-separated; Person.csv uses '+|' as field delimiter and '&|' as line terminator.
"""

import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)

_BASE = os.path.join(os.path.dirname(__file__), "..")
RAW_DIR = os.path.normpath(os.path.join(_BASE, "data", "raw"))
PROCESSED_DIR = os.path.normpath(os.path.join(_BASE, "data", "processed"))

# Column order matches the raw file exactly (no header in source files)
COLUMN_DEFS: dict[str, list[str]] = {
    "CountryRegion": [
        "country_region_code", "country_region_name", "modified_date",
    ],
    "StateProvince": [
        "state_province_id", "state_province_code", "country_region_code",
        "is_only_state_province_flag", "state_province_name", "territory_id",
        "row_guid", "modified_date",
    ],
    "Address": [
        "address_id", "street_address", "street_address_2", "city",
        "state_province_id", "postal_code", "spatial_location",
        "row_guid", "modified_date",
    ],
    "Person": [
        "business_entity_id", "person_type", "name_style", "title",
        "first_name", "middle_name", "last_name", "suffix",
        "email_promotion", "additional_contact_info", "demographics",
        "row_guid", "modified_date",
    ],
    "Customer": [
        "customer_id", "person_id", "store_id", "territory_id",
        "account_number", "row_guid", "modified_date",
    ],
    "ProductCategory": [
        "product_category_id", "category_name", "row_guid", "modified_date",
    ],
    "ProductSubcategory": [
        "product_subcategory_id", "product_category_id", "subcategory_name",
        "row_guid", "modified_date",
    ],
    "Product": [
        "product_id", "product_name", "product_number", "make_flag",
        "finished_goods_flag", "color", "safety_stock_level", "reorder_point",
        "standard_cost", "list_price", "size", "size_unit_measure_code",
        "weight_unit_measure_code", "weight", "days_to_manufacture",
        "product_line", "class", "style", "product_subcategory_id",
        "product_model_id", "sell_start_date", "sell_end_date",
        "discontinued_date", "row_guid", "modified_date",
    ],
    "SalesOrderHeader": [
        "sales_order_id", "revision_number", "order_date", "due_date",
        "ship_date", "status", "online_order_flag", "sales_order_number",
        "purchase_order_number", "account_number", "customer_id",
        "salesperson_id", "territory_id", "bill_to_address_id",
        "ship_to_address_id", "ship_method_id", "credit_card_id",
        "credit_card_approval_code", "currency_rate_id", "sub_total",
        "tax_amount", "freight", "total_due", "comment",
        "row_guid", "modified_date",
    ],
    "SalesOrderDetail": [
        "sales_order_id", "sales_order_detail_id", "carrier_tracking_number",
        "order_qty", "product_id", "special_offer_id", "unit_price",
        "unit_price_discount", "line_total", "row_guid", "modified_date",
    ],
}


def _read_person(filepath: str) -> pd.DataFrame:
    """
    Person.csv uses '+|' as field separator and '&|' as line terminator.
    Standard csv parsers cannot handle this, so we parse it manually.
    """
    columns = COLUMN_DEFS["Person"]
    expected = len(columns)
    records: list[list[str]] = []
    skipped = 0

    with open(filepath, encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.rstrip("\n\r")
            if line.endswith("&|"):
                line = line[:-2]
            parts = line.split("+|")
            if len(parts) == expected:
                records.append(parts)
            else:
                skipped += 1

    if skipped:
        logger.warning("Person.csv: skipped %d malformed rows", skipped)

    return pd.DataFrame(records, columns=columns)


def extract_table(table_name: str) -> pd.DataFrame:
    """Read one raw CSV and return a DataFrame with named columns."""
    raw_path = os.path.join(RAW_DIR, f"{table_name}.csv")

    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Raw file not found: {raw_path}")

    if table_name == "Person":
        df = _read_person(raw_path)
    else:
        df = pd.read_csv(
            raw_path,
            sep="\t",
            header=None,
            names=COLUMN_DEFS[table_name],
            engine="python",
            on_bad_lines="warn",
            dtype=str,
        )

    logger.info("Extracted %-20s %7d rows", table_name, len(df))
    return df


def extract_all() -> dict[str, int]:
    """
    Extract all 10 tables from data/raw/ and write to data/processed/.
    Returns a dict of {table_name: row_count}.
    """
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    counts: dict[str, int] = {}

    for table_name in COLUMN_DEFS:
        df = extract_table(table_name)
        out_path = os.path.join(PROCESSED_DIR, f"{table_name}.csv")
        df.to_csv(out_path, index=False)
        counts[table_name] = len(df)

    total = sum(counts.values())
    logger.info("Extraction complete — %d rows across %d tables", total, len(counts))
    return counts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
    result = extract_all()
    print("\nExtraction summary:")
    for table, n in result.items():
        print(f"  {table:<25} {n:>7,} rows")
    print(f"  {'TOTAL':<25} {sum(result.values()):>7,} rows")

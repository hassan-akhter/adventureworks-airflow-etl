"""
Transform layer — reads processed CSVs, applies type casting, null normalisation, string trimming, and deduplication, then
overwrites the processed files ready for loading.
"""

import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)

_BASE = os.path.join(os.path.dirname(__file__), "..")
PROCESSED_DIR = os.path.normpath(os.path.join(_BASE, "data", "processed"))

# Columns that require pandas datetime parsing
DATE_COLS: dict[str, list[str]] = {
    "CountryRegion":      ["modified_date"],
    "StateProvince":      ["modified_date"],
    "Address":            ["modified_date"],
    "Person":             ["modified_date"],
    "Customer":           ["modified_date"],
    "ProductCategory":    ["modified_date"],
    "ProductSubcategory": ["modified_date"],
    "Product":            ["sell_start_date", "sell_end_date", "discontinued_date", "modified_date"],
    "SalesOrderHeader":   ["order_date", "due_date", "ship_date", "modified_date"],
    "SalesOrderDetail":   ["modified_date"],
}

# 1/0 string values that map to True/False
BOOL_COLS: dict[str, list[str]] = {
    "StateProvince":    ["is_only_state_province_flag"],
    "Person":           ["name_style"],
    "Product":          ["make_flag", "finished_goods_flag"],
    "SalesOrderHeader": ["online_order_flag"],
}

INT_COLS: dict[str, list[str]] = {
    "StateProvince":      ["state_province_id", "territory_id"],
    "Address":            ["address_id", "state_province_id"],
    "Person":             ["business_entity_id", "email_promotion"],
    "Customer":           ["customer_id", "person_id", "store_id", "territory_id"],
    "ProductCategory":    ["product_category_id"],
    "ProductSubcategory": ["product_subcategory_id", "product_category_id"],
    "Product": [
        "product_id", "safety_stock_level", "reorder_point",
        "days_to_manufacture", "product_subcategory_id", "product_model_id",
    ],
    "SalesOrderHeader": [
        "sales_order_id", "revision_number", "status", "customer_id",
        "salesperson_id", "territory_id", "bill_to_address_id",
        "ship_to_address_id", "ship_method_id", "credit_card_id",
        "currency_rate_id",
    ],
    "SalesOrderDetail": [
        "sales_order_id", "sales_order_detail_id", "order_qty",
        "product_id", "special_offer_id",
    ],
}

FLOAT_COLS: dict[str, list[str]] = {
    "Product":          ["standard_cost", "list_price", "weight"],
    "SalesOrderHeader": ["sub_total", "tax_amount", "freight", "total_due"],
    "SalesOrderDetail": ["unit_price", "unit_price_discount", "line_total"],
}

# Primary key columns — rows with a null PK are dropped
PK_COLS: dict[str, str] = {
    "CountryRegion":      "country_region_code",
    "StateProvince":      "state_province_id",
    "Address":            "address_id",
    "Person":             "business_entity_id",
    "Customer":           "customer_id",
    "ProductCategory":    "product_category_id",
    "ProductSubcategory": "product_subcategory_id",
    "Product":            "product_id",
    "SalesOrderHeader":   "sales_order_id",
    "SalesOrderDetail":   "sales_order_detail_id",
}


def _cast_types(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    for col in DATE_COLS.get(table_name, []):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in BOOL_COLS.get(table_name, []):
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .map({"1": True, "0": False, "true": True, "false": False})
            )

    for col in INT_COLS.get(table_name, []):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in FLOAT_COLS.get(table_name, []):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def _normalise_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Replace blank / whitespace-only strings with NaN and strip leading/trailing spaces."""
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = (
        df[str_cols]
        .apply(lambda s: s.str.strip())
        .replace(r"^\s*$", None, regex=True)
    )
    return df


def transform_table(table_name: str) -> pd.DataFrame:
    path = os.path.join(PROCESSED_DIR, f"{table_name}.csv")
    df = pd.read_csv(path, low_memory=False, dtype=str)

    rows_in = len(df)
    df = _normalise_strings(df)
    df = _cast_types(df, table_name)

    # Drop rows missing the primary key
    pk = PK_COLS.get(table_name)
    if pk and pk in df.columns:
        before = len(df)
        df = df.dropna(subset=[pk])
        dropped = before - len(df)
        if dropped:
            logger.warning("%s: dropped %d rows with null PK (%s)", table_name, dropped, pk)

    df = df.drop_duplicates()
    rows_out = len(df)

    logger.info(
        "Transformed %-20s %7d → %7d rows",
        table_name, rows_in, rows_out,
    )
    return df


def transform_all() -> dict[str, int]:
    """
    Transform all processed tables in place.
    Returns a dict of {table_name: row_count} after cleaning.
    """
    counts: dict[str, int] = {}

    for table_name in DATE_COLS:
        df = transform_table(table_name)
        out_path = os.path.join(PROCESSED_DIR, f"{table_name}.csv")
        df.to_csv(out_path, index=False)
        counts[table_name] = len(df)

    total = sum(counts.values())
    logger.info("Transform complete — %d clean rows across %d tables", total, len(counts))
    return counts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
    result = transform_all()
    print("\nTransform summary:")
    for table, n in result.items():
        print(f"  {table:<25} {n:>7,} rows")
    print(f"  {'TOTAL':<25} {sum(result.values()):>7,} rows")

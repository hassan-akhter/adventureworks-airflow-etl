-- ============================================================
--  AdventureWorks  –  DDL (Data Definition Language)
--  Column names match the Python COLUMN_MAP exactly (snake_case)
--  Run this ONCE before triggering the Airflow DAG
-- ============================================================
 
-- Drop tables in reverse-dependency order (safe re-run)
DROP TABLE IF EXISTS salesorderdetail    CASCADE;
DROP TABLE IF EXISTS salesorderheader    CASCADE;
DROP TABLE IF EXISTS product             CASCADE;
DROP TABLE IF EXISTS productsubcategory  CASCADE;
DROP TABLE IF EXISTS productcategory     CASCADE;
DROP TABLE IF EXISTS customer            CASCADE;
DROP TABLE IF EXISTS person              CASCADE;
DROP TABLE IF EXISTS address             CASCADE;
DROP TABLE IF EXISTS stateprovince       CASCADE;
DROP TABLE IF EXISTS countryregion       CASCADE;
 
-- ────────────────────────────────────────────
--  1. Country Region
-- ────────────────────────────────────────────
CREATE TABLE countryregion (
    country_region_code     VARCHAR(3)      PRIMARY KEY,
    country_region_name     VARCHAR(100)    NOT NULL,
    modified_date           TIMESTAMP
);
 
-- ────────────────────────────────────────────
--  2. State Province
-- ────────────────────────────────────────────
CREATE TABLE stateprovince (
    state_province_id           INTEGER         PRIMARY KEY,
    state_province_code         VARCHAR(10)     NOT NULL,
    country_region_code         VARCHAR(3)      NOT NULL,
    is_only_state_province_flag BOOLEAN,
    state_province_name         VARCHAR(100)    NOT NULL,
    territory_id                INTEGER,
    row_guid                    UUID,
    modified_date               TIMESTAMP,
    FOREIGN KEY (country_region_code) REFERENCES countryregion(country_region_code)
);
 
-- ────────────────────────────────────────────
--  3. Address
-- ────────────────────────────────────────────
CREATE TABLE address (
    address_id          INTEGER         PRIMARY KEY,
    street_address      VARCHAR(200)    NOT NULL,
    street_address_2    VARCHAR(200),
    city                VARCHAR(100)    NOT NULL,
    state_province_id   INTEGER         NOT NULL,
    postal_code         VARCHAR(20),
    spatial_location    TEXT,
    row_guid            UUID,
    modified_date       TIMESTAMP,
    FOREIGN KEY (state_province_id) REFERENCES stateprovince(state_province_id)
);
 
-- ────────────────────────────────────────────
--  4. Person
-- ────────────────────────────────────────────
CREATE TABLE person (
    business_entity_id      INTEGER         PRIMARY KEY,
    person_type             VARCHAR(10),
    name_style              BOOLEAN,
    title                   VARCHAR(20),
    first_name              VARCHAR(50)     NOT NULL,
    middle_name             VARCHAR(50),
    last_name               VARCHAR(50)     NOT NULL,
    suffix                  VARCHAR(20),
    email_promotion         INTEGER,
    additional_contact_info TEXT,
    demographics            TEXT,
    row_guid                UUID,
    modified_date           TIMESTAMP
);
 
-- ────────────────────────────────────────────
--  5. Customer
-- ────────────────────────────────────────────
CREATE TABLE customer (
    customer_id     INTEGER     PRIMARY KEY,
    person_id       INTEGER,
    store_id        INTEGER,
    territory_id    INTEGER,
    account_number  VARCHAR(20),
    row_guid        UUID,
    modified_date   TIMESTAMP,
    FOREIGN KEY (person_id) REFERENCES person(business_entity_id)
);
 
-- ────────────────────────────────────────────
--  6. Product Category
-- ────────────────────────────────────────────
CREATE TABLE productcategory (
    product_category_id     INTEGER         PRIMARY KEY,
    category_name           VARCHAR(100)    NOT NULL,
    row_guid                UUID,
    modified_date           TIMESTAMP
);
 
-- ────────────────────────────────────────────
--  7. Product Subcategory
-- ────────────────────────────────────────────
CREATE TABLE productsubcategory (
    product_subcategory_id  INTEGER         PRIMARY KEY,
    product_category_id     INTEGER         NOT NULL,
    subcategory_name        VARCHAR(100)    NOT NULL,
    row_guid                UUID,
    modified_date           TIMESTAMP,
    FOREIGN KEY (product_category_id) REFERENCES productcategory(product_category_id)
);
 
-- ────────────────────────────────────────────
--  8. Product
-- ────────────────────────────────────────────
CREATE TABLE product (
    product_id                  INTEGER         PRIMARY KEY,
    product_name                VARCHAR(200)    NOT NULL,
    product_number              VARCHAR(50),
    make_flag                   BOOLEAN,
    finished_goods_flag         BOOLEAN,
    color                       VARCHAR(50),
    safety_stock_level          INTEGER,
    reorder_point               INTEGER,
    standard_cost               NUMERIC(18,4),
    list_price                  NUMERIC(18,4),
    size                        VARCHAR(10),
    size_unit_measure_code      VARCHAR(10),
    weight_unit_measure_code    VARCHAR(10),
    weight                      NUMERIC(8,2),
    days_to_manufacture         INTEGER,
    product_line                VARCHAR(5),
    class                       VARCHAR(5),
    style                       VARCHAR(5),
    product_subcategory_id      INTEGER,
    product_model_id            INTEGER,
    sell_start_date             TIMESTAMP,
    sell_end_date               TIMESTAMP,
    discontinued_date           TIMESTAMP,
    row_guid                    UUID,
    modified_date               TIMESTAMP,
    FOREIGN KEY (product_subcategory_id) REFERENCES productsubcategory(product_subcategory_id)
);
 
-- ────────────────────────────────────────────
--  9. Sales Order Header
-- ────────────────────────────────────────────
CREATE TABLE salesorderheader (
    sales_order_id              INTEGER         PRIMARY KEY,
    revision_number             SMALLINT,
    order_date                  TIMESTAMP       NOT NULL,
    due_date                    TIMESTAMP,
    ship_date                   TIMESTAMP,
    status                      SMALLINT,
    online_order_flag           BOOLEAN,
    sales_order_number          VARCHAR(25),
    purchase_order_number       VARCHAR(25),
    account_number              VARCHAR(20),
    customer_id                 INTEGER         NOT NULL,
    salesperson_id              INTEGER,
    territory_id                INTEGER,
    bill_to_address_id          INTEGER,
    ship_to_address_id          INTEGER,
    ship_method_id              INTEGER,
    credit_card_id              INTEGER,
    credit_card_approval_code   VARCHAR(15),
    currency_rate_id            INTEGER,
    sub_total                   NUMERIC(18,4),
    tax_amount                  NUMERIC(18,4),
    freight                     NUMERIC(18,4),
    total_due                   NUMERIC(18,4),
    comment                     TEXT,
    row_guid                    UUID,
    modified_date               TIMESTAMP,
    FOREIGN KEY (customer_id)           REFERENCES customer(customer_id),
    FOREIGN KEY (bill_to_address_id)    REFERENCES address(address_id),
    FOREIGN KEY (ship_to_address_id)    REFERENCES address(address_id)
);
 
-- ────────────────────────────────────────────
--  10. Sales Order Detail
-- ────────────────────────────────────────────
CREATE TABLE salesorderdetail (
    sales_order_detail_id   INTEGER         PRIMARY KEY,
    sales_order_id          INTEGER         NOT NULL,
    carrier_tracking_number VARCHAR(25),
    order_qty               INTEGER         NOT NULL,
    product_id              INTEGER         NOT NULL,
    special_offer_id        INTEGER,
    unit_price              NUMERIC(18,4)   NOT NULL,
    unit_price_discount     NUMERIC(18,4)   DEFAULT 0,
    line_total              NUMERIC(18,4),
    row_guid                UUID,
    modified_date           TIMESTAMP,
    FOREIGN KEY (sales_order_id) REFERENCES salesorderheader(sales_order_id),
    FOREIGN KEY (product_id)     REFERENCES product(product_id)
);
 
-- ────────────────────────────────────────────
--  Indexes for query performance
-- ────────────────────────────────────────────
CREATE INDEX idx_salesorderheader_customer  ON salesorderheader(customer_id);
CREATE INDEX idx_salesorderheader_orderdate ON salesorderheader(order_date);
CREATE INDEX idx_salesorderheader_territory ON salesorderheader(territory_id);
CREATE INDEX idx_salesorderdetail_order     ON salesorderdetail(sales_order_id);
CREATE INDEX idx_salesorderdetail_product   ON salesorderdetail(product_id);
CREATE INDEX idx_address_stateprovince      ON address(state_province_id);
CREATE INDEX idx_stateprovince_country      ON stateprovince(country_region_code);
CREATE INDEX idx_customer_person            ON customer(person_id);
CREATE INDEX idx_product_subcategory        ON product(product_subcategory_id);
 
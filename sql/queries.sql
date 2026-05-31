-- ============================================================
--  AdventureWorks  –  Analytics Queries
--  Run these after the Airflow DAG has loaded all tables
-- ============================================================
 
 
-- ── 1. Total revenue by year ──────────────────────────────────────────────────
SELECT
    EXTRACT(YEAR FROM order_date)  AS order_year,
    COUNT(DISTINCT sales_order_id)  AS total_orders,
    ROUND(SUM(sub_total), 0) AS total_revenue,
    ROUND(SUM(tax_amount), 0)  AS total_tax,
    ROUND(SUM(total_due), 0)  AS total_due
FROM salesorderheader
GROUP BY order_year
ORDER BY order_year;
 
 
-- ── 2. Top 10 best-selling products by quantity ───────────────────────────────
SELECT
    p.product_name,
    p.product_number,
    ROUND(SUM(d.order_qty), 0)  AS total_qty_sold,
    ROUND(SUM(d.line_total), 0)   AS total_revenue,
    ROUND(AVG(d.unit_price)::NUMERIC, 2)   AS avg_unit_price
FROM salesorderdetail d
JOIN product p ON d.product_id = p.product_id
GROUP BY p.product_id, p.product_name, p.product_number
ORDER BY total_qty_sold DESC
LIMIT 10;
 
 
-- ── 3. Revenue by product category ───────────────────────────────────────────
SELECT
    pc.category_name,
    COUNT(DISTINCT d.sales_order_id)    AS total_orders,
    ROUND(SUM(d.order_qty), 0)  AS total_units_sold,
    ROUND(SUM(d.line_total), 0)     AS total_revenue
FROM salesorderdetail d
JOIN product p          ON d.product_id = p.product_id
JOIN productsubcategory ps ON p.product_subcategory_id = ps.product_subcategory_id
JOIN productcategory pc ON ps.product_category_id = pc.product_category_id
GROUP BY pc.category_name
ORDER BY total_revenue DESC;
 
 
-- ── 4. Online vs in-store orders ─────────────────────────────────────────────
SELECT
    CASE WHEN online_order_flag THEN 'Online' ELSE 'In-Store' END AS channel,
    COUNT(*)            AS total_orders,
    ROUND(SUM(total_due), 0)      AS total_revenue,
    ROUND(AVG(total_due), 0)      AS avg_order_value
FROM salesorderheader
GROUP BY online_order_flag;
 
 
-- ── 5. Top 10 customers by lifetime value ─────────────────────────────────────
SELECT
    c.customer_id,
    p.first_name || ' ' || p.last_name   AS customer_name,
    COUNT(DISTINCT h.sales_order_id)   AS total_orders,
    ROUND(SUM(h.total_due), 0)   AS lifetime_value
FROM customer c
JOIN person p  ON c.person_id = p.business_entity_id
JOIN salesorderheader h   ON c.customer_id = h.customer_id
GROUP BY c.customer_id, customer_name
ORDER BY lifetime_value DESC
LIMIT 10;
 
 
-- ── 6. Monthly revenue trend ──────────────────────────────────────────────────
SELECT
    TO_CHAR(order_date, 'YYYY-MM')  AS month,
    COUNT(sales_order_id)  AS total_orders,
    ROUND(SUM(total_due), 0)   AS monthly_revenue
FROM salesorderheader
GROUP BY month
ORDER BY month;
 
 
-- ── 7. Sales by country / region ─────────────────────────────────────────────
SELECT
    cr.country_region_name,
    COUNT(DISTINCT h.sales_order_id)    AS total_orders,
    ROUND(SUM(h.total_due), 0)   AS total_revenue
FROM salesorderheader h
JOIN address a          ON h.ship_to_address_id = a.address_id
JOIN stateprovince sp   ON a.state_province_id  = sp.state_province_id
JOIN countryregion cr   ON sp.country_region_code = cr.country_region_code
GROUP BY cr.country_region_name
ORDER BY total_revenue DESC;
 
 
-- ── 8. Products with no sales (inventory check) ───────────────────────────────
SELECT
    p.product_id,
    p.product_name,
    p.product_number,
    p.list_price
FROM product p
LEFT JOIN salesorderdetail d ON p.product_id = d.product_id
WHERE d.product_id IS NULL
ORDER BY p.product_name;
 
 
-- ── 9. Average discount by product category ───────────────────────────────────
SELECT
    pc.category_name,
    ROUND(AVG(d.unit_price_discount) * 100, 2) AS avg_discount_pct
FROM salesorderdetail d
JOIN product p   ON d.product_id = p.product_id
JOIN productsubcategory ps ON p.product_subcategory_id = ps.product_subcategory_id
JOIN productcategory pc ON ps.product_category_id = pc.product_category_id
GROUP BY pc.category_name
ORDER BY avg_discount_pct DESC;
 
 
-- ── 10. Orders per customer distribution ─────────────────────────────────────
SELECT
    orders_per_customer,
    COUNT(*) AS customer_count
FROM (
    SELECT customer_id, COUNT(sales_order_id) AS orders_per_customer
    FROM salesorderheader
    GROUP BY customer_id
) sub
GROUP BY orders_per_customer
ORDER BY orders_per_customer;
 
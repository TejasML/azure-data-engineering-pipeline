-- =====================================================
-- SQL Warehouse Validation Script
-- Project: Azure Data Engineering Pipeline
-- =====================================================

SHOW CATALOGS;

USE CATALOG databricks_supplychain_dev;

SHOW SCHEMAS;

SHOW TABLES IN databricks_supplychain_dev.gold;

SELECT *
FROM gold.fact_trips
LIMIT 10;

SELECT *
FROM gold.dim_date
LIMIT 10;

SELECT *
FROM gold.dim_pickup_zone
LIMIT 10;

SELECT *
FROM gold.dim_dropoff_zone
LIMIT 10;

SELECT *
FROM gold.dim_payment_type
LIMIT 10;

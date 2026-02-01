/*
===============================================================================
DDL Script: Create analytics_data Views
===============================================================================
Script Purpose:
    This script creates views for the analytics_data layer in the data warehouse. 
    The analytics_data layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: analytics_data.dim_customers
-- =============================================================================
IF OBJECT_ID('analytics_data.dim_customers', 'V') IS NOT NULL
    DROP VIEW analytics_data.dim_customers;
GO

CREATE VIEW Analytics_Data.dim_customers as 
SELECT ROW_NUMBER() over(order by cust_id) as Customer_Key,
       ci.cust_id as customer_id,
       ci.cust_key as customer_number,
       ci.cust_firstname as First_name,
       ci.cust_lastname as Last_name,
       la.cntry as Country,
       ci.cust_material_status as Marital_status,
       CASE WHEN ci.cust_gndr!='N/A' THEN ci.cust_gndr --CRM IS THE MASTER FOR GENDER INFO.
            ELSE coalesce(ca.gen,'N/A') 
       END AS Gender,
       ca.bdate as Birth_Date,
       ci.cust_create_date as Create_Date
FROM Base_Data.crm_cust_info ci
LEFT JOIN Base_Data.erp_cust_az12 ca
ON        ci.cust_key =ca.cid
LEFT JOIN Base_Data.erp_loc_a101 la
ON        ci.cust_key=la.cid

-- =============================================================================
-- Create Dimension: analytics_data.dim_products
-- =============================================================================
IF OBJECT_ID('analytics_data.dim_products', 'V') IS NOT NULL
    DROP VIEW analytics_data.dim_products;
GO


CREATE VIEW analytics_data.dim_products as 
SELECT 
	   row_number() over (order by pn.prod_start_dt,pn.prod_key) as Product_key, --surogate key
       pn.prod_id        as product_id,
       pn.prod_key       as product_Number,
       pn.prod_nm        as product_name,
       pn.cat_id         as category_id,
       pc.cat            as category,
       pc.subcat         as subcategory,
       pc.maintenance    as Maintenance,
       pn.prod_cost      as Cost,
       pn.prod_line      as product_line,
       pn.prod_start_dt  as prod_start_date
FROM Base_Data.crm_prod_info pn
LEFT JOIN Base_Data.erp_px_cat_g1v2 pc
ON   pn.cat_id=pc.id
where prod_end_dt is null;  --Filter out all historical data

-- =============================================================================
-- Create Fact Table: analytics_Data.fact_sales
-- =============================================================================

IF OBJECT_ID('analytics_Data.fact_sales', 'V') IS NOT NULL
    DROP VIEW analytics_Data.fact_sales;
GO

CREATE VIEW analytics_Data.fact_sales as
select 
       sd.sals_ord_num      As Order_Number,
       pr.product_key       AS product_key,
       cu.customer_key      AS customer_key,
       sd.sals_order_dt     AS Order_date,
       sd.sals_ship_dt      AS Shipping_date,
       sd.sals_due_dt       AS Sales_Date,
       sd.sals_sales        As sales_Amount,
       sd.sals_quantity     As Sales_quantity,
       sd.sals_price        AS price
from Base_Data.crm_sales_details sd
LEFT JOIN Analytics_Data.dim_products pr
ON sd.sals_prd_key=pr.product_number
LEFT JOIN Analytics_Data.dim_customers cu
on sd.sals_cust_id=cu.customer_key

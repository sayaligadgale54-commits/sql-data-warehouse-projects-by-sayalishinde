/*
===============================================================================
DDL Script: Create BASE_Data Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'BASE_data' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'BASE_data' Tables
===============================================================================
*/
---------------------------------------------

IF OBJECT_ID('BASE_DATA.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE BASE_DATA.crm_cust_info;
GO
----------------------------------------------------
create table BASE_DATA.crm_cust_info(
cust_id int,
cust_key nvarchar(50),
cust_firstname nvarchar(50),
cust_lastname nvarchar(50),
cust_material_status nvarchar(50),
cust_gndr nvarchar(50),
cust_create_date date,
dwh_create_date DATETIME2 DEFAULT GETDATE() --DWH DATA WAREHOUSE ENGINEER USER DIEFINED COLUMN.

)


IF OBJECT_ID('BASE_DATA.crm_prod_info', 'U') IS NOT NULL
    DROP TABLE BASE_DATA.crm_prod_info;
GO
CREATE TABLE BASE_DATA.crm_prod_info (
    prod_id       INT,
    cat_id        Nvarchar(50),
    prod_key      NVARCHAR(50),
    prod_nm       NVARCHAR(50),
    prod_cost     INT,
    prod_line     NVARCHAR(50),
    prod_start_dt DATE,
    prod_end_dt   DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE() --DWH DATA WAREHOUSE ENGINEER USER DIEFINED COLUMN.

);

IF OBJECT_ID('BASE_DATA.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE BASE_DATA.crm_sales_details;
GO
CREATE TABLE BASE_DATA.crm_sales_details (
    sals_ord_num  NVARCHAR(50),
    sals_prd_key  NVARCHAR(50),
    sals_cust_id  INT,
    sals_order_dt DATE,
    sals_ship_dt  DATE,
    sals_due_dt   DATE,
    sals_sales    INT,
    sals_quantity INT,
    sals_price    INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE() --DWH DATA WAREHOUSE ENGINEER USER DIEFINED COLUMN.

);
GO

IF OBJECT_ID('BASE_DATA.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE BASE_DATA.erp_loc_a101;
GO

CREATE TABLE BASE_DATA.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);
GO

IF OBJECT_ID('BASE_DATA.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE BASE_DATA.erp_cust_az12;
GO

CREATE TABLE BASE_DATA.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE() --DWH DATA WAREHOUSE ENGINEER USER DIEFINED COLUMN.

);
GO

IF OBJECT_ID('BASE_DATA.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE BASE_DATA.erp_px_cat_g1v2;
GO

CREATE TABLE BASE_DATA.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE() --DWH DATA WAREHOUSE ENGINEER USER DIEFINED COLUMN.

);
GO

-------------------------------------------------------------

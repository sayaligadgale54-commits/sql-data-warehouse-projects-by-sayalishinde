create table Raw_Data.crm_cust_info(
cust_id int,
cust_key nvarchar(50),
cust_firstname nvarchar(50),
cust_lastname nvarchar(50),
cust_material_status nvarchar(50),
cust_gndr nvarchar(50),
cust_create_date date

)


CREATE TABLE Raw_Data.crm_prod_info (
    prod_id       INT,
    prod_key      NVARCHAR(50),
    prod_nm       NVARCHAR(50),
    prod_cost     INT,
    prod_line     NVARCHAR(50),
    prod_start_dt DATETIME,
    prod_end_dt   DATETIME
);
GO

IF OBJECT_ID('Raw_Data.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE Raw_Data.crm_sales_details;
GO

CREATE TABLE Raw_Data.crm_sales_details (
    sals_ord_num  NVARCHAR(50),
    sals_prd_key  NVARCHAR(50),
    sals_cust_id  INT,
    sals_order_dt INT,
    sals_ship_dt  INT,
    sals_due_dt   INT,
    sals_sales    INT,
    sals_quantity INT,
    sals_price    INT
);
GO

IF OBJECT_ID('Raw_Data.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE Raw_Data.erp_loc_a101;
GO

CREATE TABLE Raw_Data.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);
GO

IF OBJECT_ID('Raw_Data.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE Raw_Data.erp_cust_az12;
GO

CREATE TABLE Raw_Data.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);
GO

IF OBJECT_ID('Raw_Data.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE Raw_Data.erp_px_cat_g1v2;
GO

CREATE TABLE Raw_Data.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
GO


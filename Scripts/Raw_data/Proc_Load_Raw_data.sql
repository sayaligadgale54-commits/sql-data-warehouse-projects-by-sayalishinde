/*
===============================================================================
Stored Procedure: Load Raw_data Layer (Source -> Raw_data)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'Raw_Data' schema from external CSV files. 
    It performs the following actions:
    - Truncates the Raw_Data tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to Raw_Data tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Raw_Data.load_raw_data;
===============================================================================
*/
create or alter procedure raw_data.load_raw_Data as 
begin 
 declare @start_time datetime,@start_batch datetime,@end_batch datetime, @end_time datetime;
begin try
print '========================================================='
print 'Loading Raw data layer'
print '========================================================='

print '---------------------------------------------------------'
print 'Loading CRM Tables'
print '---------------------------------------------------------'

set @start_time=getdate();
set @start_batch=getdate();

print '>>Truncating the tables :=Raw_Data.crm_cust_info'

truncate table Raw_Data.crm_cust_info

print '>>inserting into the tables :=Raw_Data.crm_cust_info'

bulk insert Raw_Data.crm_cust_info
from 'C:\Users\SayaliVyankteshGadga\Downloads\sql-data-databank_project\sql-data-DataBank-project\datasets\source_crm\cust_info.csv'
with (
   firstrow=2,
   fieldterminator =',',
   tablock 

);

set @end_time=getdate();
print '>> Load Duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar) +' Seconds';
print '%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%'


set @start_time=getdate();

print '>>Truncating the tables :=Raw_Data.crm_prod_info'

truncate table Raw_Data.crm_prod_info

print '>>inserting into the tables :=Raw_Data.crm_prod_info'

bulk insert Raw_Data.crm_prod_info
from 'C:\Users\SayaliVyankteshGadga\Downloads\sql-data-databank_project\sql-data-DataBank-project\datasets\source_crm\prd_info.csv'
with (
   firstrow=2,
   fieldterminator =',',
   tablock 

);
set @end_time=getdate();
print '>> Load Duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar) +' Seconds';
print '%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%'



set @start_time=getdate();

print '>>Truncating the tables :=Raw_Data.crm_sales_details'

truncate table Raw_Data.crm_sales_details


print '>>inserting into the tables :=Raw_Data.crm_sales_details'

bulk insert raw_data.crm_sales_details
from 'C:\Users\SayaliVyankteshGadga\Downloads\sql-data-databank_project\sql-data-DataBank-project\datasets\source_crm\sales_details.csv'
with (
firstrow=2,
fieldterminator=',',
tablock
)
set @end_time=getdate();

print '>> Load Duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar) +' Seconds';
print '%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%'


print '---------------------------------------------------------'
print 'Loading ERP Tables'
print '---------------------------------------------------------'

set @start_time=getdate();

print '>>Truncating the tables :=Raw_Data.erp_cust_az12'

truncate table Raw_Data.erp_cust_az12

print '>>inserting into the tables :=Raw_Data.erp_cust_az12'

bulk insert raw_data.erp_cust_az12
from 'C:\Users\SayaliVyankteshGadga\Downloads\sql-data-databank_project\sql-data-DataBank-project\datasets\source_erp\cust_AZ12.csv'
with (
firstrow=2,
fieldterminator=',',
tablock
)

set @end_time=getdate();
print '>> Load Duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar) +' Seconds';
print '%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%'



set @start_time=getdate();

print '>>Truncating the tables :=Raw_Data.erp_loc_a101'

truncate table Raw_Data.erp_loc_a101

print '>>inserting into the tables :=Raw_Data.erp_loc_a101'

bulk insert Raw_data.erp_loc_a101
from 'C:\Users\SayaliVyankteshGadga\Downloads\sql-data-databank_project\sql-data-DataBank-project\datasets\source_erp\loc_a101.csv'
with (
firstrow=2,
fieldterminator=',',
tablock

);
set @end_time=getdate();
print '>> Load Duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar) +' Seconds';
print '%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%'

set @start_time=getdate();

print '>>Truncating the tables :=Raw_Data.erp_px_cat_g1v2'

truncate table Raw_Data.erp_px_cat_g1v2

print '>>inserting into the tables :=Raw_Data.erp_px_cat_g1v2'

bulk insert RAW_Data.erp_px_cat_g1v2
from 'C:\Users\SayaliVyankteshGadga\Downloads\sql-data-databank_project\sql-data-DataBank-project\datasets\source_erp\PX_CAT_G1V2.csv'
with (
firstrow=2,
fieldterminator=',',
tablock
);

set @end_time=getdate();
print '>> Load Duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar) +' Seconds';

print '$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$'
set @end_batch=getdate();
print '>> Load Duration for whole batch: '+ cast(datediff(second,@start_batch,@end_batch) as nvarchar) +' Seconds';

end try
 begin catch
    print '==========================================================='
    print 'Error occured during loading raw_Data layer'
    print 'Error message'  +error_message();
    print 'Error Message' + cast( Error_number() as nvarchar);
    print 'Error Message' + cast( Error_state() as nvarchar);

    print '============================================================'
 end catch

end

---------------------------------------------------------------------

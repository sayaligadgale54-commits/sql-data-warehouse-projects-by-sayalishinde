/*
===============================================================================
Stored Procedure: Load BASE_DATA Layer (RAW_DATA -> BASE_DATA)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'BASE_DATA' schema tables from the 'RAW_DATA' schema.
	Actions Performed:
		- Truncates BASE_DATA tables.
		- Inserts transformed and cleansed data from RAW_DATA into BASE_DATA tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC BASE_DATA.load_BASE_dATA;
===============================================================================
*/

   
   CREATE or ALTER PROCEDURE Base_Data.load_Base_data AS
BEGIN 
  DECLARE @start_time datetime,@end_time datetime,@start_batch datetime,@end_batch datetime;
BEGIN TRY  
    set @start_batch=GETDATE();

    print '========================================================='
    print 'Loading Raw data layer'
    print '========================================================='
    
    print '---------------------------------------------------------'
    print 'Loading CRM Tables'
    print '---------------------------------------------------------'
    
    ------ Loading Base_Data.crm_cust_info 
    set @start_time= getdate();
    print '>> Truncating the table :Base_Data.crm_cust_info ';
    truncate table Base_Data.crm_cust_info
    print '>> Inserting the data into:Base_Data.crm_cust_info';
    insert into Base_Data.crm_cust_info(
           cust_id ,
           cust_key ,
           cust_firstname,
           cust_lastname,
           cust_material_status,
           cust_gndr ,
           cust_create_date 
            )  
           
    select 
          cust_id,cust_key,trim(cust_firstname) as cust_firstname,
          trim(cust_lastname) as cust_lastname,
          case when upper(trim(cust_material_status))='M' then 'Married'
               when upper(trim(cust_material_status))='S' then 'Single'
               else 'N/A'  
          end as cust_marital_status, --Normalize Maritale status  values to redable format
          case when upper(trim(cust_gndr)) ='M' then 'Male'
               when upper(trim(cust_gndr)) ='F' then 'Female'
               else 'N/A'
          end as cust_gndr, --Normalize gender values to redable format
          cust_create_date
              from (
                select *,
                row_number () over (partition by cust_id order by cust_create_date desc) as flag_last
                from Raw_Data.crm_cust_info )t where flag_last=1-- Select the most recent record per customer 
                and cust_id is not null

     set @end_time =getdate();
     print '>> Load Duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar) +' Seconds';
     print '>> --------------------------'; 
     
     ---- loading into Base_Data.crm_prod_info

     set @start_time =getdate();

     print '>> Truncating the table Base_Data.crm_prod_info';
     truncate table Base_Data.crm_prod_info
     print '>> Inserting the data into:Base_Data.crm_prod_info';
     insert into Base_Data.crm_prod_info(
         prod_id  ,
         cat_id,
         prod_key ,
         prod_nm  ,
         prod_cost,
         prod_line,
         prod_start_dt,
         prod_end_dt  )

       select 
         prod_id  ,
         replace (substring (prod_key,1,5),'-','_') as cat_id, -- Extract Category Id
         substring(prod_key,7,len(prod_key)) as prod_key, --Extract Product Key
         prod_nm  ,
         ISNULL (prod_cost,0)  as prod_cost,-- handle null values 
         Case upper(trim(prod_line)) 
              when 'M' then 'Mountain'
              when 'R' then 'Road'
              when 'S' then 'other Sales'
              when 'T' then 'Touring'
              else 'N/A'
         end as prod_line, --Map product line code to be descriptive values
         cast(prod_start_dt as date) as prod_start_dt,
         cast(
              lead(prod_start_dt) over(partition by prod_key order by prod_start_dt)-1 
                 as date 
              ) as prod_end_dt --calculate the end date as one day before the next start date.
           from RAW_DATA.crm_prod_info 
           set @end_time =getdate();
           print '>> Load Duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar) +' Seconds';
           print '>>-------------------------------';

           -- Loading crm_sales_details
           set @start_time =getdate();
           
           print '>> Truncating the table Base_Data.crm_sales_details';
           truncate table Base_Data.crm_sales_details
           print '>> Inserting the data into:Base_Data.crm_sales_details';
           insert into BASE_DATA.crm_sales_details (
               sals_ord_num ,
               sals_prd_key ,
               sals_cust_id ,
               sals_order_dt,
               sals_ship_dt ,
               sals_due_dt  ,
               sals_sales   ,
               sals_quantity,
               sals_price  
            )

            select 
              sals_ord_num ,
              sals_prd_key ,
              sals_cust_id ,
              case 
                   when sals_order_dt=0 or len(sals_order_dt)!=8 then null
                   else cast(cast(sals_order_dt as varchar) as date)
              end as sals_order_dt, -- checked date parameter 
              case
                   when sals_ship_dt=0 or len(sals_ship_dt) !=8 then null
                   else cast(cast(sals_ship_dt as varchar) as date) 
              end as sals_ship_dt,
              case 
                   when sals_due_dt=0 or len(sals_due_dt) !=8 then null
                   else cast(cast(sals_due_dt as varchar) as date) 
              end as sals_due_dt,
              case  
                    when sals_sales is null or sals_sales <=0  or sals_sales!=sals_quantity* abs(sals_price) then sals_quantity * abs(sals_Price)
                    else sals_sales
              end as sals_sales,
              sals_quantity,    
              case
                  when sals_price <=0  or sals_price is null
                      then sals_sales/nullif (sals_quantity,0)
                  else sals_price
              end as sals_price
          from raw_data.crm_sales_details;
          set @end_time = getdate();
          print '>> Load Duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar) +' Seconds';
          print '>>-----------------------------';

          -- Loading erp_cust_az12
          set @start_time= getdate();
          print '>> Truncating the table: Base_Data.erp_cust_az12';
          truncate table Base_Data.erp_cust_az12
          print '>> Inserting the data into:Base_Data.erp_cust_az12';
          insert into Base_Data.erp_cust_az12(
                cid,
                bdate,
                gen
          )
          select  
            case 
                 when cid  like 'NAS%' then substring(cid,4,len(cid)) -- Remove 'NAS' prefix if present.
                 else cid 
            end as cid,
            case 
                 when bdate > getdate() then null
                 else bdate
            end as bdate, -- Set future birthdates to null 
            case 
                 when upper(trim(gen)) in ('M','male') then 'Male'
                 when upper(trim(gen)) in ('F', 'female') then 'Female'
                 else 'N/A'
            end as gen -- Normalize gender values and handle unkown cases
        from raw_DATA.erp_cust_az12 ;
            
        set @end_time= getdate();
        print '>> Load Duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar) +' Seconds';
        print '>> ---------------------------------------';
            
        print '>> ---------------------------------------';
        print '>> Loading ERP tables';
        print '>> ---------------------------------------';

        print '>> Loading erp_loc_a101 tables';

        set @start_time= getdate();
        print '>> Truncating the table : Base_Data.erp_loc_a101';
        truncate table Base_Data.erp_loc_a101
        print '>> Inserting the data into:Base_Data.erp_loc_a101';
        insert into base_data.erp_loc_a101 (
              cid,
              cntry
          )
          select 
           replace(cid ,'-','') as cid, -- handles invalid value 
           case 
                when upper(trim(cntry))='DE' then 'Germany'
                when upper(trim(cntry)) in ('US' , 'USA') then 'United state'
                when upper(trim(cntry))='' or cntry is null then 'N/A'
                else trim(cntry)
           end as cntry --Normalize and handle missing or blank country codes.
       from raw_DATA.erp_loc_a101 
       set @end_time= getdate();
       print '>> Load Duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar) +' Seconds';
       print '>>---------------------------------------';
       
   -------- Loading erp_px_cat_g1v2.

       set @start_time= getdate();
       print '>> Truncating the table:Base_Data.erp_px_cat_g1v2';
       truncate table Base_Data.erp_px_cat_g1v2
       print '>> Inserting the data into:Base_Data.erp_px_cat_g1v2';    
       insert into BASE_DATA.erp_px_cat_g1v2(
            id ,
            cat ,
            subcat,
            maintenance
         )
       select id ,
              cat  ,      
              subcat ,
              maintenance
       from raw_DATA.erp_px_cat_g1v2 
       set @end_time= getdate();
        print '>> Load Duration: '+ cast(datediff(second,@start_time,@end_time) as nvarchar) +' Seconds'
        print '>>--------------------------------------------';

        set @end_batch =getdate();
        print '================================================'
        print ' Loading Base_layer is completed'
        print '   - Total Load duration :' + cast(datediff(second,@start_batch,@end_batch) as nvarchar) +' Seconds'
        print '================================================'
   END  TRY
   BEGIN CATCH 
        print '================================================'
        print 'Error occurred during base_layer '
        print 'Error Message' +ERROR_MESSAGE();
        print 'Error Message' +CAST(ERROR_NUMBER() AS NVARCHAR);
        print 'Error Message' +CAST(ERROR_STATE() AS NVARCHAR);
        print '================================================'
   END CATCH
END

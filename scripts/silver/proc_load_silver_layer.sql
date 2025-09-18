--To execute the script:
--Exec silver.load_silver

CREATE OR ALTER PROCEDURE silver.load_silver as
BEGIN
	declare @start_time datetime, @end_time datetime;
	declare @start_time_layer datetime, @end_time_layer datetime;
	BEGIN TRY

		set @start_time_layer = getdate();
		print 'Loading Silver Layer';

		print '----------------------------------';
		print 'Loading CRM tables';
		print '----------------------------------';

		-- LOAD SCRIPT 1 
		-- Remove duplicates and nulls from the cst_id
		set @start_time = getdate();
		print '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		print '>> Inserting Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)

		select cst_id, cst_key, TRIM(cst_firstname) as cst_firstname, TRIM(cst_lastname) as cst_lastname, 
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' 
			 ELSE 'n/a'
		END cst_marital_status, --normalize gender and marital status values to readable format
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male' 
			 ELSE 'n/a'
		END cst_gndr,
		cst_create_date 
		from ( --removing duplicates and null values
		select *,
		row_number() over (partition by cst_id order by cst_create_date DESC) as flag_last
		from bronze.crm_cust_info
		where cst_id is not null
		)t where flag_last = 1; -- data filtering (selecting the most recent record per customer)

		set @end_time= getdate();
		print '>> Load duration: '+ cast( datediff(second, @start_time, @end_time)as nvarchar) + ' seconds' ;
		print '---------------------------------------';
		----------------------------------------------------------------
		--LOAD SCRIPT 2
		set @start_time = getdate();
		print '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		print '>> Inserting Data Into: silver.crm_prd_info';
		Insert into silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt)
	
		Select prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id, --extract category id (Derived columns)
		SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key, --extract product key
		prd_nm,
		ISNULL(prd_cost, 0 ) as prd_cost,
		CASE UPPER(TRIM(prd_line)) 
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END as prd_line, --data normalization -map product line codes to descriptive values
		prd_start_dt,
		DATEADD(day, -1, LEAD(prd_start_dt) OVER (PARTITION by prd_key ORDER BY prd_start_dt ASC)) as prd_end_dt --calculate end date as one day before the next start date
		from bronze.crm_prd_info;

		set @end_time= getdate();
		print '>> Load duration: '+ cast( datediff(second, @start_time, @end_time)as nvarchar) + ' seconds' ;
		print '---------------------------------------';
		----------------------------------------------------
		--LOAD SCRIPT 3
		set @start_time = getdate();
		print '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		print '>> Inserting Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price)
	
		SELECT sls_ord_num,
		sls_prd_key,
		sls_cust_id,--Below handling invalid data and data casting 
		CASE WHEN sls_order_dt = 0 OR LEN (sls_order_dt) != 8 THEN NULL
			else cast(CAST(sls_order_dt as varchar) as date)
		END as sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN (sls_ship_dt) != 8 THEN NULL
			else cast(CAST(sls_ship_dt as varchar) as date)
		END as sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN (sls_due_dt) != 8 THEN NULL
			else cast(CAST(sls_due_dt as varchar) as date)
		END as sls_due_dt,
		case when sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
			then sls_quantity * ABS(sls_price)
			else sls_sales
		END as sls_sales,--recalculate sales if original value is missing or incorrect 
		sls_quantity,
		case when sls_price IS NULL OR sls_price <= 0
			then sls_sales / NULLIF(sls_quantity, 0)
			else sls_price
		END as sls_price-- derive price if original value is invalid 
		from bronze.crm_sales_details;

		set @end_time= getdate();
		print '>> Load duration: '+ cast( datediff(second, @start_time, @end_time)as nvarchar) + ' seconds' ;
		print '---------------------------------------';
		-------------------------------------
		--LOAD SCRIPT 4

		print '----------------------------------';
		print 'Loading ERP tables';
		print '----------------------------------';

		set @start_time = getdate();
		print '>> Truncating Table: silver.erp_CUST_AZ12';
		TRUNCATE TABLE silver.erp_CUST_AZ12;
		print '>> Inserting Data Into: silver.erp_CUST_AZ12';
		Insert into silver.erp_CUST_AZ12 (
			cid, bdate, gen)

		select
		case when cid like 'NAS%' then substring(cid,4,len(cid)) --remove 'NAS' prefix if present
			else cid
		end as cid,
		case when bdate > getdate() then null
			else bdate
		end as bdate,-- set future bdates to NULL
		case when upper(trim(gen)) IN ('F', 'FEMALE') then 'Female'
			 when upper(trim(gen)) IN ('M', 'MALE') then 'Male'
			 else 'n/a'
		end as gen --normalize gender values and handle unknown cases
		from bronze.erp_CUST_AZ12;

		set @end_time= getdate();
		print '>> Load duration: '+ cast( datediff(second, @start_time, @end_time)as nvarchar) + ' seconds' ;
		print '---------------------------------------';

		set @start_time = getdate();
		print '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		print '>> Inserting Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101(
		cid, cntry)

		select distinct
		replace(cid, '-', '') cid, 
		case when upper(trim(cntry)) = 'DE' then 'Germany'
			 when upper(trim(cntry)) IN ('US', 'USA') then 'United States'
			 when upper(trim(cntry)) = '' OR cntry is NULL then 'n/a'
			 else trim(cntry)
		end as cntry-- Normalize and handle missing or blank country codes
		from bronze.erp_loc_a101;

		set @end_time= getdate();
		print '>> Load duration: '+ cast( datediff(second, @start_time, @end_time)as nvarchar) + ' seconds' ;
		print '---------------------------------------';

		set @start_time = getdate();
		print '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		print '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(
		id, cat, subcat, maintenance)

		select id,cat,subcat,maintenance 
		from bronze.erp_px_cat_g1v2;

		set @end_time= getdate();
		print '>> Load duration: '+ cast( datediff(second, @start_time, @end_time)as nvarchar) + ' seconds' ;
		print '---------------------------------------';


		set @end_time_layer = getdate();
		print '>> Silver layer load duration: '+ cast( datediff(second, @start_time_layer, @end_time_layer)as nvarchar) + ' seconds' ;
		print '---------------------------------------';
	END TRY
	BEGIN CATCH
		PRINT '===================================';
		PRINT 'ERROR OCCURED DURING SILVER LAYER';
		PRINT 'Error Message' + error_message();
		print 'ERROR message' + cast (error_number() as nvarchar);
	END CATCH
END

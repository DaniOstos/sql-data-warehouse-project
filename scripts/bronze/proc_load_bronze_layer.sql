--Usage Example:
  --exec bronze.load_bronze

Create or Alter Procedure bronze.load_bronze as
BEGIN
	declare @start_time datetime, @end_time datetime;
	declare @start_time_layer datetime, @end_time_layer datetime;
	BEGIN TRY
		set @start_time_layer = getdate();
		print 'Loading Bronze Layer';

		print '----------------------------------';
		print 'Loading CRM tables';
		print '----------------------------------';

		set @start_time = getdate();
		print '>>Truncating table: bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;

		print '>>Inserting into table: bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'C:\Users\dostosrivas\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		set @end_time= getdate();
		print '>> Load duration: '+ cast( datediff(second, @start_time, @end_time)as nvarchar) + ' seconds' ;
		print '---------------------------------------';

		set @start_time = getdate();
		print '>>Truncating table: bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;

		print '>>Inserting into table: bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'C:\Users\dostosrivas\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		)
		set @end_time= getdate();
		print '>> Load duration: '+ cast( datediff(second, @start_time, @end_time)as nvarchar) + ' seconds' ;
		print '---------------------------------------';

		set @start_time = getdate();
		print '>>Truncating table: bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;

		print '>>Inserting into table: bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'C:\Users\dostosrivas\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time= getdate();
		print '>> Load duration: '+ cast( datediff(second, @start_time, @end_time)as nvarchar) + ' seconds' ;
		print '---------------------------------------';

		print '----------------------------------';
		print 'Loading ERP tables';
		print '----------------------------------';

		set @start_time = getdate();
		print '>>Truncating table: bronze.erp_CUST_AZ12';
		truncate table bronze.erp_CUST_AZ12;

		print '>>Inserting into table: bronze.erp_CUST_AZ12';
		bulk insert bronze.erp_CUST_AZ12
		from 'C:\Users\dostosrivas\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time= getdate();
		print '>> Load duration: '+ cast( datediff(second, @start_time, @end_time)as nvarchar) + ' seconds' ;
		print '---------------------------------------';

		set @start_time = getdate();
		print '>>Truncating table: bronze.erp_LOC_A101';
		truncate table bronze.erp_LOC_A101;

		print '>>Inserting into table: bronze.erp_LOC_A101';
		bulk insert bronze.erp_LOC_A101
		from 'C:\Users\dostosrivas\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time= getdate();
		print '>> Load duration: '+ cast( datediff(second, @start_time, @end_time)as nvarchar) + ' seconds' ;
		print '---------------------------------------';

		set @start_time= getdate();
		print '>>Truncating table: bronze.erp_PX_CAT_G1V2';
		truncate table bronze.erp_PX_CAT_G1V2;

		print '>>Inserting into table: bronze.erp_PX_CAT_G1V2';
		bulk insert bronze.erp_PX_CAT_G1V2
		from 'C:\Users\dostosrivas\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time= getdate();
		print '>> Load duration: '+ cast( datediff(second, @start_time, @end_time)as nvarchar) + ' seconds' ;
		print '---------------------------------------';

		set @end_time_layer = getdate();
		print '>> Bronze layer load duration: '+ cast( datediff(second, @start_time_layer, @end_time_layer)as nvarchar) + ' seconds' ;
		print '---------------------------------------';
	END TRY
	BEGIN CATCH 
		PRINT '===================================';
		PRINT 'ERROR OCCURED DURING BRONZE LAYER';
		PRINT 'Error Message' + error_message();
		print 'ERROR message' + cast (error_number() as nvarchar);
	END CATCH
END

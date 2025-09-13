if object_id ('bronze.crm_cust_info', 'U') is not null
	drop table bronze.crm_cust_info;
create table bronze.crm_cust_info (
	cst_id	int,
	cst_key	varchar(50),
	cst_firstname	varchar(50),
	cst_lastname	varchar(50),
	cst_marital_status	varchar(50),
	cst_gndr	varchar(50),
	cst_create_date	date
);

if object_id ('bronze.crm_prd_info', 'U') is not null
	drop table bronze.crm_prd_info;
create table bronze.crm_prd_info (
	prd_id	int,
	prd_key	varchar(50),
	prd_nm	varchar(50),
	prd_cost	int,
	prd_line	varchar(50),
	prd_start_dt	date,
	prd_end_dt	date
);

if object_id ('bronze.crm_sales_details', 'U') is not null
	drop table bronze.crm_sales_details;
create table bronze.crm_sales_details (
	sls_ord_num varchar(50),
	sls_prd_key varchar(50),
	sls_cust_id	int,
	sls_order_dt	int,
	sls_ship_dt	int,
	sls_due_dt	int,
	sls_sales	int,
	sls_quantity int,
	sls_price int
);

if object_id ('bronze.erp_CUST_AZ12', 'U') is not null
	drop table bronze.erp_CUST_AZ12;
create table bronze.erp_CUST_AZ12 (
	CID	varchar(50),
	BDATE	date,
	GEN	varchar(50)
);

if object_id ('bronze.erp_LOC_A101', 'U') is not null
	drop table bronze.erp_LOC_A101;
create table bronze.erp_LOC_A101 (
	CID	varchar(50),
	CNTRY	varchar(50)
);

if object_id ('bronze.erp_PX_CAT_G1V2', 'U') is not null
	drop table bronze.erp_PX_CAT_G1V2;
create table bronze.erp_PX_CAT_G1V2 (
	ID	varchar(50),
	CAT	varchar(50),
	SUBCAT	varchar(50),
	MAINTENANCE varchar(50)
);

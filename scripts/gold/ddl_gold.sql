create view gold.dim_customers as 
select 
	row_number() over (order by cst_id) as customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	lo.cntry as country,
	ci.cst_marital_status as marital_status,
	case when ci.cst_gndr != 'n/a' then ci.cst_gndr --CRM is the Master for gender Info
		else COALESCE(cu.gen, 'n/a')
	end as gender,
	cu.bdate as birthday,
	ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_CUST_AZ12 cu 
on ci.cst_key = cu.CID
left join silver.erp_loc_a101 lo
on ci.cst_key = lo.cid

create view gold.dim_products as 
select 
	row_number() over (order by pn.prd_start_dt, pn.prd_key) as product_key,
	pn.prd_id as product_id,
	pn.prd_key as product_number,	
	pn.prd_nm as product_name,
	pn.cat_id as category_id,
	ca.cat as category,
	ca.subcat as subcategory,
	ca.maintenance,
	pn.prd_cost as cost,
	pn.prd_line as product_line,
	pn.prd_start_dt as start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 ca
on pn.cat_id = ca.id
where pn.prd_end_dt is NULL -- filter out all historical data (so only keep the current record for each product#)

create view gold.fact_sales as
select 
sd.sls_ord_num as order_number,
pr.product_key,
cs.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customers cs
on sd.sls_cust_id = cs.customer_id

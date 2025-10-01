-- Empolyee Table
create view gold.dim_customers AS
select 
	ROW_NUMBER() over (order by cst_id) as customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.CNTRY as country,
	ci.cst_marital_status as marital_status,
	case when ci.cst_gndr != 'n/a' then ci.cst_gndr -- cst_gen is mastr for gender info
		 else Coalesce(ca.GEN,'n/a')
	end as gender,
	ca.BDATE as birthdate,
	ci.cst_create_date as create_date
from Silver.crm_cust_info ci
left join Silver.erp_cust_AZ12 ca on ci.cst_key=ca.cid
left join Silver.erp_loc_A101 la on ci.cst_key=la.cid


-- product data
create view gold.dim_product AS
select 
	row_number() over (order by pn.prd_start_dt,pn.prd_key) as product_key,
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.prd_nm as product_name,
	pn.cat_id as category_id,
	pc.CAT as category,
	pc.SUBCAT as subcategory,
	pc.MAINTENANCE,
	pn.prd_cost as cost,
	pn.prd_line as product_line,
	pn.prd_start_dt as start_date
from Silver.crm_prd_info pn
left join Silver.erp_px_cat_g1v2 pc on pn.cat_id = pc.id
where pn.prd_end_dt is null -- filter out all historical data and displaying only current data


-- Sales
create view gold.fact_sales AS
select 
	sd.sls_ord_num as Order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt as order_date,
	sd.sls_ship_dt as shipping_date,
	sd.sls_due_dt as due_date,
	sd.sls_sales as sales_amount,
	sd.sls_quantity as quantity,
	sd.sls_price as price
from Silver.crm_sales_details sd
left join Gold.dim_product pr on pr.product_number = sd.sls_prd_key
left join Gold.dim_customers cu on cu.customer_id = sd.sls_cust_id

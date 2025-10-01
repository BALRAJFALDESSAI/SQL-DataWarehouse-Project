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


select * from Gold.dim_product
select * from Gold.dim_customers

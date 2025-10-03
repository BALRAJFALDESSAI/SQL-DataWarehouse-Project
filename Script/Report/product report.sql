-- View will be created
/*
1 Gather essential fields such as product names, category, subcategory and Cost detalis.
2 segment product by revenue to identify high-performance,mid-range,low-performance
3 aggregate product level metrics
 a total order
 b total sales
 c total quantity sold
 d total customer
 e lifespan
4 Calculate valueable KPI 
	a recency (month since last order)
	b average order revenue
	c average monthly revenue
*/
create view gold.report_product as
with base_query as (
select 
	f.Order_number,
	f.order_date,
	f.customer_key,
	f.sales_amount,
	f.quantity,
	p.product_key,
	p.product_name,
	p.category,
	p.subcategory,
	p.cost 
from Gold.fact_sales f 
left join Gold.dim_product p on p.product_key=f.product_key
where f.order_date is not null)

,mid_query as (
	select 
		product_key,
		product_name,
		category,
		subcategory,
		cost,
		DATEDIFF(month,min(order_date),max(order_date)) as life_span,
		MAX(order_date) as Last_sale_Date,
		COUNT(distinct Order_number) as total_order,
		COUNT(distinct customer_key) as total_customer,
		SUM(sales_amount) as total_sales,
		SUM(quantity) as quantity,
		round(avg(cast(sales_amount as Float)/nullif(quantity,0)),1) as avg_selling_price
	from base_query 
	group by product_key,product_name,category,subcategory,cost )

select 
		product_key,
		product_name,
		category,
		subcategory,
		cost,
		Last_sale_Date,
		life_span,
		total_sales,
		DATEDIFF(month,Last_sale_Date,getdate()) as Recency,
		case when Total_sales>50000 then 'High-Performance'
			 when Total_sales>=10000 then 'Mid-Range'
			 else 'Low-Performance'
		end customer_segment,
		total_order,
		total_customer,
		quantity,
		avg_selling_price,
		case when Total_Order =0 then 0
			 else Total_sales/Total_Order 
		end as Avg_Order_Revenue,
		case when life_span =0 then Total_sales
			 else Total_sales/life_span 
		end as Avg_monthly_revenue
from mid_query

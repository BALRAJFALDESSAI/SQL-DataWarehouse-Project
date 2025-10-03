-- View will be created
/*
1 Gather essential fields such as names, ages and transcation detalis.
2 segment customer into category and age group
3 aggregate customer level metrics
 a total order
 b total sales
 c total quantity purchased
 d total products
 e lifespan
4 Calculate valueable KPI 
	a recency (month since last order)
	b average order value
	c average monthly spend
*/
create view gold.report_customer as
with base_query as(
select 
f.Order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
CONCAT(c.first_name,' ',c.last_name) as Customer_Name,
c.birthdate,
DATEDIFF(year,c.birthdate,getdate()) as age
from Gold.fact_sales f 
left join Gold.dim_customers c on c.customer_key=f.customer_key 
where f.Order_number is not null )
,mid_query as(
select 
customer_key,
customer_number,
Customer_Name,
age,
COUNT( distinct Order_number) as Total_Order,
SUM(sales_amount) as Total_sales,
sum(quantity) as Total_quantity,
count(distinct product_key) as total_product,
MAX(order_date) as Last_order_Date,
DATEDIFF(month,min(order_date),max(order_date)) as life_span
from base_query group by customer_key,customer_number,Customer_Name,age)

select
	customer_key,
	customer_number,
	Customer_Name,
	age,
	case when life_span>12 and Total_sales>5000 then 'VIP'
		 when life_span>12 and Total_sales<=5000 then 'Regular'
		 else 'New'
	end customer_segment,
	case when age<20 then 'Under 20'
		 when age between 20 and 29 then '20-29'
		 when age between 30 and 39 then '30-39'
		 when age between 40 and 49 then '40-49'
		 else '50 and above'
	end Age_group,
	Total_Order,
	Total_sales,
	Total_quantity,
	total_product,
	Last_order_Date,
	DATEDIFF(month,Last_order_Date,getdate()) as Recency,
	life_span,
	case when Total_Order =0 then 0
		 else Total_sales/Total_Order 
	end as Avg_Order_Value,
	case when life_span =0 then Total_sales
		 else Total_sales/life_span 
	end as Avg_monthly_spend
from mid_query

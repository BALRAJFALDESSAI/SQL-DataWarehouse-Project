-- step1 :Change Over Time
-- Helps to track the trends [measure] by [Date Dimension]

-- Sales performace over time
-- year
select Year(order_date) as Year,Sum(sales_amount) as Total_Sales,COUNT(distinct customer_key) as Total_Customer from Gold.fact_sales 
where order_date is not NUll group by Year(order_date) order by Year(order_date)
-- month
select Month(order_date) as Month,Sum(sales_amount) as Total_Sales,COUNT(distinct customer_key) as Total_Customer from Gold.fact_sales 
where order_date is not NUll group by Month(order_date) order by Month(order_date)
-- year,month
select Year(order_date) as Year,Month(order_date) as Month,Sum(sales_amount) as Total_Sales,COUNT(distinct customer_key) as Total_Customer from Gold.fact_sales 
where order_date is not NUll group by Year(order_date),Month(order_date) order by Year(order_date),Month(order_date)

-- Step 2 : Cumulative Analysis
-- aggregate data progressively over the time

-- calculate the total sales per month and the running sales over time

select Order_Month,Total_Sales,SUM(Total_Sales) over(order by Order_Month) as Running_Sales from (
select Datetrunc(MONTH,order_date) as Order_Month,Sum(sales_amount) as Total_Sales from Gold.fact_sales 
where order_date is not NUll group by Datetrunc(MONTH,order_date))t 
-- running total for each year
select Order_Month,Total_Sales,SUM(Total_Sales) over(partition by Order_Month order by Order_Month) as Running_Sales from (
select Datetrunc(MONTH,order_date) as Order_Month,Sum(sales_amount) as Total_Sales from Gold.fact_sales 
where order_date is not NUll group by Datetrunc(MONTH,order_date))t 

-- Step 3 : Performance analysis
-- Comparing current value with target value {Current[Measure]-Target[Measure]}
-- Analyze the yearly performance of product by comparing each product sales to both it's avg sales and the previous years sales
WITH year_Product_sales As (
select YEAR(fc.order_date) as Order_year,dp.product_name,SUM(fc.sales_amount) as Total_sales from Gold.fact_sales fc
left join Gold.dim_product dp on fc.product_key=dp.product_key where YEAR(fc.order_date) is not null 
group by YEAR(fc.order_date),dp.product_name )

select Order_year,product_name,Total_sales,avg(Total_sales) over (Partition by product_name) as Avg_Sales,
Total_sales-avg(Total_sales) over (Partition by product_name) as Diff_With_Avg_Sales,
case when Total_sales-avg(Total_sales) over (Partition by product_name) > 0 then 'Above Average'
	 when Total_sales-avg(Total_sales) over (Partition by product_name) < 0 then 'Below Average'
	 Else 'Avg'
End Avg_Change,
Lag(Total_sales) over (partition by product_name order by Order_year) As Py_Sales,
Total_sales- Lag(Total_sales) over (partition by product_name order by Order_year) as diff_py_sales,
case when Total_sales- Lag(Total_sales) over (partition by product_name order by Order_year) > 0 then 'Increase'
	 when Total_sales- Lag(Total_sales) over (partition by product_name order by Order_year) < 0 then 'Decrease'
	 Else 'No change'
End Avg_Change
from year_Product_sales order by product_name,Order_year

-- step 4 : Part to whole analysis
-- analyse how an individual part is performing compared to overall
-- Which category contribute most to overall sales
with sales_contri as (
select d.category as category,sum(f.sales_amount)as Sales from gold.fact_sales f 
left join Gold.dim_product d on f.product_key=d.product_key group by d.category )

select category,Sales,SUM(sales) over() as Overall_Sales, concat(Round((Cast(Sales as float)/SUM(sales) over())*100,2),'%') as Category_Contri
from sales_contri

-- Step 5 : Data Segmentation
-- segment product into cost range and count how many product fall into each segment 
with product_category as (
select product_key,product_name,cost ,
case when cost<100 then 'Below 100'
	 when cost between 100 and 500 then '100-500'
	 when cost between 500 and 1000 then '500-1000'
	 when cost between 1000 and 1500 then '1000-1500'
	 when cost between 1500 and 2000 then '1500-2000'
	 else 'Above 2000'
end as Segment
from Gold.dim_product)

select Segment, COUNT(product_key) As No_of_Product from product_category group by Segment

/* group cust into 3 segment based on thier spending behaviour
- vip : at least 12 months of history and spending more than 5000
- regular : at least 12 months of history and spending 5000 or less
- new : Lifespan less than 12 months */
with cust_spend as (
select 
c.customer_key,
Sum(s.sales_amount) as Total_sales,
min(s.order_date) as first_order,
max(s.order_date) as last_order,
DATEDIFF(month,min(s.order_date),max(s.order_date)) as life_span
from gold.dim_customers c 
left join Gold.fact_sales s on c.customer_key = s.customer_key group by c.customer_key)

select customer_segment,count(customer_key) as Total_customer from(
select customer_key,Total_sales,life_span,
case when life_span>12 and Total_sales>5000 then 'VIP'
	 when life_span>12 and Total_sales<=5000 then 'Regular'
	 else 'New'
end customer_segment
from cust_spend)t group by customer_segment order by Total_customer



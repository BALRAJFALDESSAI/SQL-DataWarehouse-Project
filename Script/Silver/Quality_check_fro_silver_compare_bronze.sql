/* Cleaning data from silver table and inserting in into silver table */

Select * from silver.crm_cust_info
-- Check for null and duplicate in primary key 
-- expectation : No Result

select cst_id,COUNT(*)  from silver.crm_cust_info group by cst_id having COUNT(*)>1 or cst_id IS NULL


-- Check for unwanted spaces
-- expectation : No Result

Select cst_firstname from silver.crm_cust_info where cst_firstname != TRIM(cst_firstname)
Select cst_lastname from silver.crm_cust_info where cst_lastname != TRIM(cst_lastname)
Select cst_marital_status from silver.crm_cust_info where cst_marital_status != TRIM(cst_marital_status)
Select cst_gndr from silver.crm_cust_info where cst_gndr != TRIM(cst_gndr)
-- Remove all spaces


-- Datastandardization and Consistency
Select distinct cst_gndr from silver.crm_cust_info
Select distinct cst_marital_status from silver.crm_cust_info

Select * from silver.crm_cust_info


-----------------------------------------------------------------------------------------------------------
--crm_prd_info
Select prd_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt from silver.crm_prd_info


-- Check for null and duplicate in primary key 
-- expectation : No Result

select prd_id,COUNT(*) from silver.crm_prd_info group by prd_id having count(*)>1 or prd_id is NULL


-- Check for unwanted spaces
-- expectation : No Result
select prd_key from silver.crm_prd_info where prd_key != TRIM(prd_key)
select prd_nm from silver.crm_prd_info where prd_nm != TRIM(prd_nm)

-- check for null or -Ve Number
Select prd_cost  from silver.crm_prd_info where prd_cost<0 or prd_cost is NULL

-- checking for short form
select prd_line,count(*) from silver.crm_prd_info group by prd_line

-- checking for wrong date
select *  from silver.crm_prd_info where prd_end_dt < prd_start_dt



----------------------------------------------------------------------------------------------------------------------
--crm_Sales_details
Select sls_ord_num ,sls_prd_key ,sls_cust_id ,sls_order_dt,sls_ship_dt ,sls_due_dt ,sls_sales ,sls_quantity ,sls_price 
from silver.crm_Sales_details

-- check fro spaces
Select 
	sls_ord_num ,
	sls_prd_key ,
	sls_cust_id ,
	sls_order_dt,
	sls_ship_dt ,
	sls_due_dt ,
	sls_sales ,
	sls_quantity ,
	sls_price 
from silver.crm_Sales_details where sls_ord_num != TRIM(sls_ord_num)

-- Check for invalid dates
Select 
	Nullif(sls_order_dt,0) as sls_order_dt
from silver.crm_Sales_details where sls_order_dt <= 0 or len(sls_order_dt) != 8

Select * from silver.crm_Sales_details where sls_order_dt> sls_ship_dt or sls_order_dt > sls_due_dt

-- Check for -ve or 0
Select 
	sls_sales as old_sls_sales,
	sls_quantity ,
	sls_price as old_sls_price,
	case when sls_sales is null or sls_sales <= 0 or sls_sales !=  sls_quantity * ABS(sls_price) Then sls_quantity * ABS(sls_price)
		 else sls_sales
	end as sls_sales,
	case when sls_price is null or sls_price <= 0 Then sls_sales / NULLIF(sls_quantity,0)
		 else sls_price
	end as sls_price
from silver.crm_Sales_details where sls_sales != sls_quantity * sls_price or 
sls_sales is NULL or sls_quantity is NULL or sls_price is NULL or
sls_sales <=0 or sls_quantity <=0 or sls_price <=0


-----------------------------------------------------------------------------------------------
-- erp_cust_AZ12

Select BDATE from silver.erp_cust_AZ12 where BDATE < '1924-01-01' or BDATE > GETDATE()
Select Distinct(GEN) from silver.erp_cust_AZ12

-----------------------------------------------------------------------------------------------
-- erp_loc_A101
Select * from silver.erp_loc_A101

Select 
REPLACE(CID,'-','') AS CID ,
case when TRIM(CNTRY) = 'DE' then 'Germany'
	 when TRIM(CNTRY) IN ('US','USA') then 'United States'
	 When TRIM(CNTRY) = '' or CNTRY is NULL then 'n/a'
	 else TRIM(CNTRY)
end CNTRY 
from silver.erp_loc_A101

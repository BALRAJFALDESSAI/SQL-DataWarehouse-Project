-- Transformaton
Exec silver.load_silver
Create or Alter Procedure silver.load_silver AS
Begin
	declare @start_timeb DATETIME,@end_timeb DATETIME;
	declare @start_time DATETIME,@end_time DATETIME;
	set @start_timeb = GETDATE();
	Begin Try
		print '===========================================================';
		print 'Loading Silver Layer';
		print '===========================================================';

		Print '-----------------------------------------------------------';
		print 'Loading CRM Tables'
		Print '-----------------------------------------------------------';
		--1 Silver.crm_cust_info
		/*
		Trim()- we have removed unwanted spaces
		we did data normalization for cst_marital_status and cst_gndr
		We have removed the duplicate value and null value from primary key in case of duplicates we have taken the most recent data
		*/
		set @start_time = GETDATE();
		Print '>> Truncate Table Silver.crm_cust_info'
		Truncate table Silver.crm_cust_info
		Print '>> Insert Data Into Table Silver.crm_cust_info'
		Insert into Silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date 
		)
		Select 
		cst_id,
		cst_key,
		trim(cst_firstname) as cst_firstname,
		trim(cst_lastname) as cst_lastname,
		Case when Upper(Trim(cst_marital_status)) = 'S' Then 'Single'
			 when Upper(Trim(cst_marital_status)) = 'M' Then 'Married'
			 Else 'n/a'
		End cst_marital_status,
		Case when Upper(Trim(cst_gndr)) = 'F' Then 'Female'
			 when Upper(Trim(cst_gndr)) = 'M' Then 'Male'
			 Else 'n/a'
		End cst_gndr,
		cst_create_date 
		from (
		Select *, ROW_NUMBER() Over(Partition by cst_id order by cst_create_date desc) as Flag from Bronze.crm_cust_info where cst_id IS Not NULL
		)t where Flag = 1 
		set @end_time = GETDATE();
		print '>> Load Time : ' + CAST(datediff(second,@start_time,@end_time) as nvarchar)+ ' Sec';
		print '|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||'

		--2 Silver.crm_prd_info
		set @start_time = GETDATE();
		/*
		1st we have derived new column for mpping 
		we checked if cost is a null value is so replace by 0
		we did data normalization for prd_line
		we did data enrichment for end date
		*/
		Print '>> Truncate Table Silver.crm_prd_info'
		Truncate table Silver.crm_prd_info
		Print '>> Insert Data Into Table Silver.crm_prd_info'
		Insert into silver.crm_prd_info 
		(
			prd_id ,
			cat_id ,
			prd_key ,
			prd_nm ,
			prd_cost ,
			prd_line ,
			prd_start_dt ,
			prd_end_dt 
		)
		select 
		prd_id,
		Replace(SUBSTRING(prd_key,1,5),'-','_') as Cat_Id,
		SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
		prd_nm,
		isnull(prd_cost,0) as prd_cost,
		case when upper(trim(prd_line)) = 'M' then 'Mountain'
			 when upper(trim(prd_line)) = 'R' then 'Roads'
			 when upper(trim(prd_line)) = 'S' then 'Other Sales'
			 when upper(trim(prd_line)) = 'T' then 'Touring'
			 else 'n/a'
		end prd_line,
		cast(prd_start_dt as Date) as prd_start_dt,
		cast(LEAD(prd_start_dt) OVER (partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt 
		from Bronze.crm_prd_info

		set @end_time = GETDATE();
		print '>> Load Time : ' + CAST(datediff(second,@start_time,@end_time) as nvarchar)+ ' Sec';
		print '|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||'


		-- 3 Silver.crm_Sales_details
		set @start_time = GETDATE();
		Print '>> Truncate Table Silver.crm_Sales_details'
		Truncate table Silver.crm_Sales_details
		Print '>> Insert Data Into Table Silver.crm_Sales_details'
		Insert into Silver.crm_Sales_details
		(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		Select 
			sls_ord_num ,
			sls_prd_key ,
			sls_cust_id ,
			Case when sls_order_dt = 0 or len(sls_order_dt) != 8 Then Null
				 Else cast(CAST(sls_order_dt as varchar) as DATE) 
			END as sls_order_dt,
			Case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 Then Null
				 Else cast(CAST(sls_ship_dt as varchar) as DATE) 
			END as sls_ship_dt,
			Case when sls_due_dt = 0 or len(sls_due_dt) != 8 Then Null
				 Else cast(CAST(sls_due_dt as varchar) as DATE) 
			END as sls_due_dt,
			case when sls_sales is null or sls_sales <= 0 or sls_sales !=  sls_quantity * ABS(sls_price) Then sls_quantity * ABS(sls_price)
				 else sls_sales
			end as sls_sales,
			sls_quantity ,
			case when sls_price is null or sls_price <= 0 Then sls_sales / NULLIF(sls_quantity,0)
				 else sls_price
			end as sls_price 
		from Bronze.crm_Sales_details
		set @end_time = GETDATE();
		print '>> Load Time : ' + CAST(datediff(second,@start_time,@end_time) as nvarchar)+ ' Sec';
		print '|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||'

		-- 4 Silver.erp_cust_AZ12
		set @start_time = GETDATE();
		/* 
		Just removed the bdate greater than the currenet date 
		and normalize the GEN column
		*/
		Print '>> Truncate Table Silver.erp_cust_AZ12'
		Truncate table Silver.erp_cust_AZ12
		Print '>> Insert Data Into Table Silver.erp_cust_AZ12'
		insert into Silver.erp_cust_AZ12 (CID,BDATE,GEN)
		Select 
		case when CID LIKE 'NAS%' Then Substring(CID,4,LEN(CID))
			Else CID
		END CID,
		case when BDATE > getdate() then NULL
			 else BDATE
		end as BDATE,
		case when Upper(trim(GEN)) in ('F','FEMALE') Then 'Female'
			 when Upper(trim(GEN)) in ('M','MALE') Then 'Male'
			 ELSE 'n/a'
		end GEN 
		from Bronze.erp_cust_AZ12
		set @end_time = GETDATE();
		print '>> Load Time : ' + CAST(datediff(second,@start_time,@end_time) as nvarchar)+ ' Sec';
		print '|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||'

		-- 5 Silver.erp_loc_A101
		/*
		removing - fromCID
		normalize the CNTRY column
		*/
		set @start_time = GETDATE();
		Print '>> Truncate Table Silver.erp_loc_A101'
		Truncate table Silver.erp_loc_A101
		Print '>> Insert Data Into Table Silver.erp_loc_A101'
		insert into Silver.erp_loc_A101 (CID,CNTRY)
		Select 
		REPLACE(CID,'-','') AS CID ,
		case when TRIM(CNTRY) = 'DE' then 'Germany'
			 when TRIM(CNTRY) IN ('US','USA') then 'United States'
			 When TRIM(CNTRY) = '' or CNTRY is NULL then 'n/a'
			 else TRIM(CNTRY)
		end CNTRY 
		from Bronze.erp_loc_A101
		set @end_time = GETDATE();
		print '>> Load Time : ' + CAST(datediff(second,@start_time,@end_time) as nvarchar)+ ' Sec';
		print '|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||'

		-- 6 erp_PX_CAT_G1V2
		set @start_time = GETDATE();
		Print '>> Truncate Table Silver.erp_PX_CAT_G1V2'
		Truncate table Silver.erp_PX_CAT_G1V2
		Print '>> Insert Data Into Table Silver.erp_PX_CAT_G1V2'
		Insert into Silver.erp_PX_CAT_G1V2 (ID,
		CAT,
		SUBCAT,
		MAINTENANCE )
		Select 
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE 
		From Bronze.erp_PX_CAT_G1V2
		set @end_time = GETDATE();
		print '>> Load Time : ' + CAST(datediff(second,@start_time,@end_time) as nvarchar)+ ' Sec';
		print '|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||'
	End Try
	Begin Catch 
		Print 'Error --- Error --- Error --- Error'
	End Catch
	set @end_timeb = GETDATE();
	print '====================================================================================================';
	print '====================================================================================================';
	print '>> Silver Layer Load Time : ' + CAST(datediff(second,@start_timeb,@end_timeb) as nvarchar)+ ' Sec';
	print '====================================================================================================';
	print '====================================================================================================';
END


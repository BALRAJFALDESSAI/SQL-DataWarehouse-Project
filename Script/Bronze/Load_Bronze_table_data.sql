/*
Inserting data into bronze tables
*/

exec Bronze.load_bronze

Create or Alter procedure bronze.load_bronze AS
Begin
	declare @start_timeb DATETIME,@end_timeb DATETIME;
	declare @start_time DATETIME,@end_time DATETIME;
	set @start_timeb = GETDATE();
	Begin Try
		print '===========================================================';
		print 'Loading Bronze Layer';
		print '===========================================================';

		Print '-----------------------------------------------------------';
		print 'Loading CRM Tables'
		Print '-----------------------------------------------------------';
		-- Bulk Insert

		-- 1 Bronze.crm_cust_info
		set @start_time = GETDATE();
		print '>>> Truncate Table : Bronze.crm_cust_info';
		truncate table Bronze.crm_cust_info
	
		print '>>> Insert Data Into : Bronze.crm_cust_info';
		Bulk Insert Bronze.crm_cust_info
		from 'C:\Users\Balraj Fal Desaai\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
		firstrow = 2,
		fieldterminator=',',
		tablock
		)
		set @end_time = GETDATE();
		print '>> Load Time : ' + CAST(datediff(second,@start_time,@end_time) as nvarchar)+ ' Sec';
		print '|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||'
		-- 2 Bronze.crm_prd_info
		set @start_time = GETDATE();
		print '>>> Truncate Table : Bronze.crm_prd_info';
		truncate table Bronze.crm_prd_info
	
		print '>>> Insert Data Into : Bronze.crm_prd_info';
		Bulk Insert Bronze.crm_prd_info
		from 'C:\Users\Balraj Fal Desaai\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
		firstrow = 2,
		fieldterminator=',',
		tablock
		)
		set @end_time = GETDATE();
		print '>> Load Time : ' + CAST(datediff(second,@start_time,@end_time) as nvarchar)+ ' Sec';
		print '|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||'

		-- 3 Bronze.crm_Sales_details
		set @start_time = GETDATE();
		print '>>> Truncate Table : Bronze.crm_Sales_details';
		truncate table Bronze.crm_Sales_details
	
		print '>>> Insert Data Into : Bronze.crm_Sales_details';
		Bulk Insert Bronze.crm_Sales_details
		from 'C:\Users\Balraj Fal Desaai\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
		firstrow = 2,
		fieldterminator=',',
		tablock
		)
		set @end_time = GETDATE();
		print '>> Load Time : ' + CAST(datediff(second,@start_time,@end_time) as nvarchar)+ ' Sec';
		print '|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||'

		Print '-----------------------------------------------------------';
		print 'Loading ERP Tables'
		Print '-----------------------------------------------------------';
		-- 4 Bronze.erp_cust_AZ12
		set @start_time = GETDATE();
		print '>>> Truncate Table : Bronze.erp_cust_AZ12';
		truncate table Bronze.erp_cust_AZ12
	
		print '>>> Insert Data Into : Bronze.erp_cust_AZ12';
		Bulk Insert Bronze.erp_cust_AZ12
		from 'C:\Users\Balraj Fal Desaai\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
		firstrow = 2,
		fieldterminator=',',
		tablock
		)
		set @end_time = GETDATE();
		print '>> Load Time : ' + CAST(datediff(second,@start_time,@end_time) as nvarchar)+ ' Sec';
		print '|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||'

		-- 5 Bronze.erp_loc_A101
		set @Start_time = GETDATE();
		print '>>> Truncate Table : Bronze.erp_loc_A101';
		truncate table Bronze.erp_loc_A101
	
		print '>>> Insert Data Into : Bronze.erp_loc_A101';
		Bulk Insert Bronze.erp_loc_A101
		from 'C:\Users\Balraj Fal Desaai\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
		firstrow = 2,
		fieldterminator=',',
		tablock
		)
		set @end_time = GETDATE();
		print '>> Load Time : ' + CAST(datediff(second,@start_time,@end_time) as nvarchar)+ ' Sec';
		print '|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||'

		-- 6 Bronze.erp_PX_CAT_G1V2
		set @start_time = GETDATE();
		print '>>> Truncate Table : Bronze.erp_PX_CAT_G1V2';
		truncate table Bronze.erp_PX_CAT_G1V2
	
		print '>>> Insert Data Into : Bronze.erp_PX_CAT_G1V2';
		Bulk Insert Bronze.erp_PX_CAT_G1V2
		from 'C:\Users\Balraj Fal Desaai\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
		firstrow = 2,
		fieldterminator=',',
		tablock
		)
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
	print '>> Bronze Layer Load Time : ' + CAST(datediff(second,@start_timeb,@end_timeb) as nvarchar)+ ' Sec';
	print '====================================================================================================';
	print '====================================================================================================';
End

--1. Empty table to make sure that it is not loaded twice
--2. Load Data from CSV to SQL Server
--3. Check Data quality to make sure that records match their columns
--4. Create as a stored procedure to reuse it
--5. Handle errors using Try..Catch
--6. Monitor loading performance by using start and end time variables
--7. Execute the stored procedure

create or alter procedure bronze.load_bronze as 
begin
	declare @batch_start_time datetime, @batch_end_time datetime;
	begin try
		set @batch_start_time = GETDATE();
		truncate table bronze.crm_cust_info;
		bulk insert bronze.crm_cust_info 
			from 'D:\Microsoft Data Engineer\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'	
			with (
			firstrow = 2, 
			fieldterminator = ',',
			tablock	--lock the table while inserting data in
			);
		select * from bronze.crm_cust_info;

		truncate table bronze.crm_prd_info;
		bulk insert bronze.crm_prd_info 
			from 'D:\Microsoft Data Engineer\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'	
			with (
			firstrow = 2, 
			fieldterminator = ',',
			tablock	--lock the table while inserting data in
			);
		select * from bronze.crm_prd_info;

		truncate table bronze.crm_sales_details;
		bulk insert bronze.crm_sales_details 
			from 'D:\Microsoft Data Engineer\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'	
			with (
			firstrow = 2, 
			fieldterminator = ',',
			tablock	--lock the table while inserting data in
			);
		select * from bronze.crm_sales_details;



		truncate table bronze.erp_CUST_AZ12;
		bulk insert bronze.erp_CUST_AZ12 
			from 'D:\Microsoft Data Engineer\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'	
			with (
			firstrow = 2, 
			fieldterminator = ',',
			tablock	--lock the table while inserting data in
			);
		select * from bronze.erp_CUST_AZ12;


		truncate table bronze.erp_LOC_A101;
		bulk insert bronze.erp_LOC_A101 
			from 'D:\Microsoft Data Engineer\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'	
			with (
			firstrow = 2, 
			fieldterminator = ',',
			tablock	--lock the table while inserting data in
			);
		select * from bronze.erp_LOC_A101;


		truncate table bronze.erp_PX_CAT_G1V2;
		bulk insert bronze.erp_PX_CAT_G1V2 
			from 'D:\Microsoft Data Engineer\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'	
			with (
			firstrow = 2, 
			fieldterminator = ',',
			tablock	--lock the table while inserting data in
			);
		select * from bronze.erp_PX_CAT_G1V2;

		set @batch_end_time = GETDATE();
		print('Load duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as varchar));
	end try
	begin catch
		print('Error occured during loading bronze layer');
		print(error_message());
	end catch
end


exec bronze.load_bronze;

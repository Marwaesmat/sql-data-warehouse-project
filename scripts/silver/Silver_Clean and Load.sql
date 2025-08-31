
--select count(*) Duplicates from silver.crm_cust_info group by cust_id having count(*)>1; --Check Duplicates
/*
1. Check for Nulls or Duplicates in PKs
2. Since there are duplicates and because of historical records, to get the latest updated row of each record
3. Trim any spaces by getting all values that have spaces (first name, last name)
5. Data standardization and consistency. store meaningful and clear data than abbreviated ones (gender)
6. Extract data from a column that are merged and has separated values in columns of other tables (prd_key)
7. Check data consistency between tables for columns that will be used for join (added data in cat_id, prd_key from prd_key column) sls_prd_key of sales_details
8. Check for nulls of negative values with cols that must have value (prd_cost) by at least replace nulls by 0
9. Resolve incosistencies of date periods (start/end dates) such as overlapped records or nulled start dates
10. Covert numeric values of dates into DATE data type, with converting 0 to Null and confirm the proper length of the values (%date cols in sales_details)
11. Ensure that data follows bussiness rules properly (sales=quantity*price in sales_details) with no Negative or Null values 
12. Ensure that birth date is consistent, not in future (CUST_AZ12 table)
13, Apply truncate table to avoid repeated insert
14. Insert cleaned data into silver table
15. Create Stored Procedure
16. Handle Errors
17. Check loading performance
18. Execute the stored procedure
*/

create or alter procedure silver.load_silver as
begin
	declare @batch_start_time datetime, @batch_end_time datetime;
	begin try
		set @batch_start_time=GETDATE();
		truncate table silver.crm_cust_info;

		insert into silver.crm_cust_info (
			cust_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
			)
		select cust_id,
			cst_key,
			trim(cst_firstname) as cst_firstname, 
			trim(cst_lastname) as cst_lastname,
			case when upper(trim(cst_marital_status))='S' then 'Single'
				when upper(trim(cst_marital_status))='M' then 'Married'
				else 'N/A'
			end cst_marital_status,
			case when upper(trim(cst_gndr))='F' then 'Female'
				when upper(trim(cst_gndr))='M' then 'Male'
				else 'N/A'
			end cst_gndr,
			cst_create_date
			from (
			select *, ROW_NUMBER() over (partition by cust_id order by cst_create_date desc) as flag_last --collect rows by id ordered by create_date
			from bronze.crm_cust_info where cust_id is not null) t --t is alias for derived table
			where flag_last=1;	--Make sure that the last updated record is resulted


		truncate table silver.crm_prd_info;

		insert into silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_date,
			prd_end_date)
		select prd_id,
			--Filter out unmatched data
			REPLACE(SUBSTRING(prd_key,1,5), '-', '_') as cat_id,	--to be similar to values of ID of PX_CAT
			SUBSTRING(prd_key,7,len(prd_key)) as prd_key,	--to be similar to values of ID of PX_CAT
			prd_nm,
			ISNULL(prd_cost, 0) as prd_cost,
			case  upper(trim(prd_line))
				when 'M' then 'Mountain'
				when 'R' then 'Road'
				when 'S' then 'Other sales'
				when 'T' then 'Touring'
				else 'N/A'
			end as prd_line,
			prd_start_date,
			--Use Dateadd to allow subtract date value while subtract is only allowed with int values
			DATEADD(DAY, -1, LEAD(prd_start_date) OVER (PARTITION BY prd_key ORDER BY prd_start_date)) AS prd_end_date --Change end date value by end_date to be same of start_date of previous record (lead)
			from bronze.crm_prd_info;



		truncate table silver.crm_sales_details;

		insert into silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key, 
			sls_cst_id, 
			sls_order_dt, 
			sls_ship_dt, 
			sls_due_dt, 
			sls_sales, 
			sls_quantity, 
			sls_price
			)
		select sls_ord_num,
			sls_prd_key,
			sls_cst_id, 
			case when sls_order_dt=0 or len(sls_order_dt)!=8 then null
				else cast(cast(sls_order_dt as varchar) as date)
			end sls_order_dt, 
			case when sls_ship_dt=0 or len(sls_ship_dt)!=8 then null
				else cast(cast(sls_ship_dt as varchar) as date)
			end sls_ship_dt, 
			case when sls_due_dt=0 or len(sls_due_dt)!=8 then null
				else cast(cast(sls_due_dt as varchar) as date)
			end sls_due_dt,
			--Rules: if sales is 0, derive it from quant*price. if price is 0 or null, derive it from sales/quantity. if price is negative, make it positive
			CASE 
				WHEN sls_sales IS NULL 
				  OR sls_sales <= 0 
				  OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales 
			END AS sls_sales,
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0 
				THEN 
					(
						CASE 
							WHEN sls_sales IS NULL 
							  OR sls_sales <= 0 
							  OR sls_sales != sls_quantity * ABS(sls_price)
							THEN sls_quantity * ABS(sls_price)
							ELSE sls_sales
						END
					) / NULLIF(sls_quantity, 0)  -- divide by non-zero, non-null quantity
				ELSE ABS(sls_price)
			END AS sls_price
			from bronze.crm_sales_details;



		truncate table silver.erp_CUST_AZ12;

		insert into silver.erp_CUST_AZ12 (
			CID,
			BDATE,
			GEN)
		select 
			case when CID like 'NAS%' then SUBSTRING(CID, 4, LEN(CID))
			else CID
			end as CID,
			case when BDATE>GETDATE() then null
			else BDATE
			end as BDATE,
			case when UPPER(TRIM(GEN)) in ('F', 'FEMALE') then 'Female' 
				when UPPER(TRIM(GEN)) in ('M', 'MALE') then 'Male' 
				else 'N/A'
			end as GEN
		from bronze.erp_CUST_AZ12;



		truncate table silver.erp_LOC_A101;

		insert into silver.erp_LOC_A101 (
			CID, 
			CNTRY)
		select 
			REPLACE(CID, '-', '') CID,
			case when trim(CNTRY) = 'DE' then 'Germany' 
				when trim(CNTRY) in ('US', 'USA') then 'United States' 
				when trim(CNTRY) = '' or CNTRY is null then 'N/A' 
				else trim(CNTRY)
			end as CNTRY
		from bronze.erp_LOC_A101;


		truncate table silver.erp_PX_CAT_G1V2;

		insert into silver.erp_PX_CAT_G1V2 (
			ID,
			CAT, 
			SUBCAT,
			MAINTENANCE)
		select 
			ID,
			CAT, 
			SUBCAT,
			MAINTENANCE
		from bronze.erp_PX_CAT_G1V2;

		set @batch_end_time=GETDATE();
		print('Loading duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as varchar) + ' seconds');
	end try
	begin catch
		print('Error occured while loading silver layer');
		print(ERROR_MESSAGE());
	end catch
end

exec silver.load_silver;
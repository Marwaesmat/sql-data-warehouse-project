/*
1. Get all data of Customers by left join to get all data of customers, not only the matched ids
2. Since gender is duplicated between two tables with non-consistent data, they need to be cleaned and integrated. CRM is the original one.
COALESCE(gen, 'n/a') return the first non-NULL value from a list of expressions.
3. Create surrogate key that will be derived by ROW_NUMBER() function.
4. in Products table, since there are historical data, we need to get only the latest ones (end_date is null)
5. Create Dim table using View for both customers and products
5. Create Fact table for sales
*/
create view gold.dim_customers as 
	select 
	ROW_NUMBER() over (order by cust_id) as Customer_Key,
	ci.cust_id as Customer_ID, 
	ci.cst_key as Customer_Number, 
	ci.cst_firstname as First_Name, 
	ci.cst_lastname as Last_Name,
	la.CNTRY as Country,
	ci.cst_marital_status as Marital_Status, 
	case when ci.cst_gndr != 'N/A' then ci.cst_gndr
		else COALESCE(ca.gen, 'N/A')
	end as Gender,  
	ca.BDATE as Birth_Date,
	ci.cst_create_date as Create_Date
	from silver.crm_cust_info ci
	left join silver.erp_CUST_AZ12 ca 
	on ci.cst_key=ca.CID
	left join silver.erp_LOC_A101 la
	on ci.cst_key=la.CID;

select * from gold.dim_customers;


create view gold.dim_products as 
	select
	ROW_NUMBER() over (order by pn.prd_start_date, pn.prd_key) as Product_Key,
	pn.prd_id as Product_ID,
	pn.prd_key as Product_Number,
	pn.prd_nm as Product_Name,
	pn.cat_id as Category_ID,
	pc.CAT as Category,
	pc.SUBCAT as Subcategory,
	pc.MAINTENANCE as Maintenance,
	pn.prd_cost as Cost, 
	pn.prd_line as Line,
	pn.prd_start_date as StartDate
	from silver.crm_prd_info pn
	left join silver.erp_PX_CAT_G1V2 pc
	on pn.cat_id=pc.ID
	where prd_end_date is null; -- Get only latest data and filter out the historical ones

select * from gold.dim_products;

create view gold.fact_sales as 
	select 
	sd.sls_ord_num as Order_Number,
	pr.Product_Key, --instead of sd.sls_prd_key
	cu.Customer_ID, --instead of sd.sls_cst_id,
	sd.sls_order_dt as Order_Date,
	sd.sls_ship_dt as Shippment_Date,
	sd.sls_due_dt as Due_Date,
	sd.sls_sales as Sales,
	sd.sls_quantity as Quantity,
	sd.sls_price as Price
	from silver.crm_sales_details sd 
	left join gold.dim_products pr 
	on sd.sls_prd_key=pr.Product_Number
	left join gold.dim_customers cu 
	on sd.sls_cst_id=cu.Customer_ID

select * from gold.fact_sales;

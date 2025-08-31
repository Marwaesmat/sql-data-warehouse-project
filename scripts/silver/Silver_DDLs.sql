--Create CRM Tables

if OBJECT_ID('silver.crm_cust_info', 'U') is not null
	drop table silver.crm_cust_info;
create table silver.crm_cust_info 
	(cust_id int,
	cst_key nvarchar(50),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_marital_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date date,
	dwh_create_date datetime2 default getdate()
	);

if OBJECT_ID('silver.crm_prd_info', 'U') is not null
	drop table silver.crm_prd_info;
create table silver.crm_prd_info 
	(prd_id int,
	cat_id nvarchar(50),
	prd_key nvarchar(50),
	prd_nm nvarchar(50),
	prd_cost int,
	prd_line nvarchar(50),
	prd_start_date date,
	prd_end_date date,
	dwh_create_date datetime2 default getdate()
	);

if OBJECT_ID('silver.crm_sales_details', 'U') is not null
	drop table silver.crm_sales_details;
create table silver.crm_sales_details 
	(sls_ord_num nvarchar(50),
	sls_prd_key nvarchar(50),
	sls_cst_id int,
	sls_order_dt date,
	sls_ship_dt date,
	sls_due_dt date,
	sls_sales int,
	sls_quantity int,
	sls_price int,
	dwh_create_date datetime2 default getdate()
	);


--Create ERP Tables

if OBJECT_ID('silver.erp_CUST_AZ12', 'U') is not null
	drop table silver.erp_CUST_AZ12;
create table silver.erp_CUST_AZ12 
	(CID nvarchar(50),
	BDATE date,
	GEN nvarchar(50),
	dwh_create_date datetime2 default getdate()
	);

if OBJECT_ID('silver.erp_LOC_A101', 'U') is not null
	drop table silver.erp_LOC_A101;
create table silver.erp_LOC_A101 
	(CID nvarchar(50),
	CNTRY nvarchar(50),
	dwh_create_date datetime2 default getdate()
	);

if OBJECT_ID('silver.erp_PX_CAT_G1V2', 'U') is not null
	drop table silver.erp_PX_CAT_G1V2;
create table silver.erp_PX_CAT_G1V2
	(ID nvarchar(50),
	CAT nvarchar(50),
	SUBCAT nvarchar(50),
	MAINTENANCE nvarchar(50),
	dwh_create_date datetime2 default getdate()
	);
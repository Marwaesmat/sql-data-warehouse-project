-- Create Database 'DataWarehouse'
use master;
create database DataWarehouse;
use DataWarehouse;

-- Create Schemas (accessed through Security Folder). MUST be executed one by one or you have to add GO after each create statement
create schema bronze;
create schema silver;
create schema gold;

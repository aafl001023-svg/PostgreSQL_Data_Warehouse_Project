/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists using psql. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

psql -U {username}

--You will be promo to enter the postgre's password  

-- Drop and recreate the 'DataWarehouse' database
DROP DATABASE IF EXISTS datawarehouse;

-- Create the 'DataWarehouse' database and connect to it
CREATE DATABASE datawarehouse;
\c datawarehouse

--Create schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze();
===============================================================================
*/
CREATE OR REPLACE PROCEDURE bronze.load_bronze() 
AS $$
DECLARE
    start_time  TIMESTAMPTZ;
    end_time    TIMESTAMPTZ;
	execution_time INTERVAL;
	batch_start_time TIMESTAMPTZ;
	batch_end_time TIMESTAMPTZ;
	execution_time_batch INTERVAL;
	
BEGIN
	batch_start_time := clock_timestamp();
	RAISE NOTICE '==============================================';
	RAISE NOTICE 'Loading Bronce layer';
	RAISE NOTICE '==============================================';
	
	RAISE NOTICE '----------------------------------------------';
	RAISE NOTICE 'Loading CRM Tables';
	RAISE NOTICE '----------------------------------------------';

	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating table: bronze.crm_cust_info';
	TRUNCATE TABLE  bronze.crm_cust_info;

	RAISE NOTICE '>> Inserting data into: bronze.crm_cust_info';
	COPY bronze.crm_cust_info
	FROM  'C:\Users\wuich\Projects\dwh_project\datasets\source_crm\cust_info.csv'
	WITH (FORMAT CSV, HEADER);
	end_time := clock_timestamp();
	execution_time := end_time - start_time;
	RAISE NOTICE 'Execution time = %', execution_time;
	RAISE NOTICE '-----';

	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating table: bronze.crm_prd_info';
	TRUNCATE TABLE  bronze.crm_prd_info;

	RAISE NOTICE '>> Inserting data into: bronze.crm_cust_info';
	COPY bronze.crm_prd_info
	FROM  'C:\Users\wuich\Projects\dwh_project\datasets\source_crm\prd_info.csv'
	WITH (FORMAT CSV, HEADER);
	end_time := clock_timestamp();
	execution_time := end_time - start_time;
	RAISE NOTICE 'Execution time = %', execution_time;
	RAISE NOTICE '-----';

	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating table: bronze.crm_sales_details';
	TRUNCATE TABLE  bronze.crm_sales_details;

	RAISE NOTICE '>> Inserting data into: bronze.crm_sales_details';
	COPY bronze.crm_sales_details
	FROM  'C:\Users\wuich\Projects\dwh_project\datasets\source_crm\sales_details.csv'
	WITH (FORMAT CSV, HEADER);
	end_time := clock_timestamp();
	execution_time := end_time - start_time;
	RAISE NOTICE 'Execution time = %', execution_time;
	RAISE NOTICE '-----';
	

	RAISE NOTICE '----------------------------------------------';
	RAISE NOTICE 'Loading ERP Tables';
	RAISE NOTICE '----------------------------------------------';

	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating table: bronze.erp_cust_az12';
	TRUNCATE TABLE bronze.erp_cust_az12;

	RAISE NOTICE '>> Inserting data into: bronze.erp_cust_az12';
	COPY bronze.erp_cust_az12
	FROM 'C:\Users\wuich\Projects\dwh_project\datasets\source_erp\CUST_AZ12.csv'
	WITH (FORMAT CSV, HEADER);
	end_time := clock_timestamp();
	execution_time := end_time - start_time;
	RAISE NOTICE 'Execution time = %', execution_time;
	RAISE NOTICE '-----';

	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating table: bronze.erp_loc_a101';
	TRUNCATE TABLE bronze.erp_loc_a101;

	RAISE NOTICE '>> Inserting data into: bronze.erp_loc_a101';
	COPY bronze.erp_loc_a101
	FROM 'C:\Users\wuich\Projects\dwh_project\datasets\source_erp\LOC_A101.csv'
	WITH (FORMAT CSV, HEADER);
	end_time := clock_timestamp();
	execution_time := end_time - start_time;
	RAISE NOTICE 'Execution time = %', execution_time;
	RAISE NOTICE '-----';
	
	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating table: bronze.erp_px_cat_g1v2';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;

	RAISE NOTICE '>> Inserting data into: bronze.erp_px_cat_g1v2';
	COPY bronze.erp_px_cat_g1v2
	FROM 'C:\Users\wuich\Projects\dwh_project\datasets\source_erp\PX_CAT_G1V2.csv'
	WITH (FORMAT CSV, HEADER);
	end_time := clock_timestamp();
	execution_time := end_time - start_time;
	RAISE NOTICE 'Execution time = %', execution_time;
	RAISE NOTICE '-----';
	
	batch_end_time := clock_timestamp();
	execution_time_batch := batch_end_time - batch_start_time;
	RAISE NOTICE '===============================';
	RAISE NOTICE 'LOADING BROZE LAYER IS COMPLETED';
	RAISE NOTICE 'TOTAL DURATION = %', execution_time_batch;
	RAISE NOTICE '===============================';
EXCEPTION 
WHEN OTHERS THEN 
	RAISE NOTICE '==============================';
	RAISE NOTICE 'An error occurred: %', SQLERRM;
	RAISE NOTICE 'Error Code: %', SQLSTATE;
	RAISE NOTICE '==============================';
END;
$$
LANGUAGE plpgsql;

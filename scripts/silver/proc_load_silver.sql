/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL silver.load_silver();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver() 
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
	RAISE NOTICE 'Loading Silver layer';
	RAISE NOTICE '==============================================';

	RAISE NOTICE '----------------------------------------------';
	RAISE NOTICE 'Loading CRM Tables';
	RAISE NOTICE '----------------------------------------------';

	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_material_status,
		cst_gndr,
		cst_create_date
	)
	SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname)AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
			 WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
			 ELSE 'n/a'
		END cst_material_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 ELSE 'n/a'
		END cst_gndr,
		cst_create_date
	FROM (
	SELECT *, 
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info) AS t
	WHERE flag_last = 1;
	end_time := clock_timestamp();
	execution_time := end_time - start_time;
	RAISE NOTICE 'Execution time = %', execution_time;
	RAISE NOTICE '-----';

	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating table: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		
	)
	SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
		SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
		prd_nm,
		COALESCE(prd_cost, 0) AS prd_cost,
		CASE UPPER(TRIM(prd_line)) 
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'Other Sales'
			 WHEN 'T' THEN 'Touring'
			 ELSE 'n/a' 
		END prd_line,
		prd_start_dt::DATE,
		(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1)::DATE AS prd_end_dt
	FROM bronze.crm_prd_info;
	end_time := clock_timestamp();
	execution_time := end_time - start_time;
	RAISE NOTICE 'Execution time = %', execution_time;
	RAISE NOTICE '-----';

	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating table: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';
	INSERT INTO silver.crm_sales_details(
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
	SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
			 ELSE CAST(sls_order_dt::TEXT AS DATE)
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
			 ELSE CAST(sls_ship_dt::TEXT AS DATE)
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
			 ELSE CAST(sls_due_dt::TEXT AS DATE)
		END AS sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
		 THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <= 0
		 THEN sls_sales/ sls_quantity
		 ELSE sls_price
	END AS sls_price
	FROM bronze.crm_sales_details;
	end_time := clock_timestamp();
	execution_time := end_time - start_time;
	RAISE NOTICE 'Execution time = %', execution_time;
	RAISE NOTICE '-----';

	RAISE NOTICE '----------------------------------------------';
	RAISE NOTICE 'Loading ERP Tables';
	RAISE NOTICE '----------------------------------------------';

	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating table: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
	SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
		 ELSE cid
	END AS cid,
	CASE WHEN bdate > NOW() THEN NULL
		 ELSE bdate
	END AS bdate,
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		 ELSE 'n/a'
	END AS gen
	FROM bronze.erp_cust_az12;
	end_time := clock_timestamp();
	execution_time := end_time - start_time;
	RAISE NOTICE 'Execution time = %', execution_time;
	RAISE NOTICE '-----';

	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating table: silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	RAISE NOTICE '>> Inserting Data Into: silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101(cid, cntry)
	SELECT 
	REPLACE(cid, '-', '') AS cid,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		 ELSE TRIM(cntry)
	END cntry
	FROM bronze.erp_loc_a101;
	end_time := clock_timestamp();
	execution_time := end_time - start_time;
	RAISE NOTICE 'Execution time = %', execution_time;
	
	start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating table: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2
	(id, cat, subcat, maintenance)
	SELECT 
	id, 
	cat, 
	subcat, 
	maintenance
	FROM bronze.erp_px_cat_g1v2;
	end_time := clock_timestamp();
	execution_time := end_time - start_time;
	RAISE NOTICE 'Execution time = %', execution_time;

	batch_end_time := clock_timestamp();
	execution_time_batch := batch_end_time - batch_start_time;
	RAISE NOTICE '===============================';
	RAISE NOTICE 'LOADING SILVER LAYER IS COMPLETED';
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

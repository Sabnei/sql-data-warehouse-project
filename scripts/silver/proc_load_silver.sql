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
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
BEGIN

batch_start_time := now();

RAISE NOTICE '================================================';
RAISE NOTICE 'Loading Silver Layer';
RAISE NOTICE '================================================';

RAISE NOTICE 'Loading CRM Tables';

-- =====================================================
-- crm_cust_info
-- =====================================================

start_time := now();
TRUNCATE TABLE silver.crm_cust_info;

INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END AS cst_marital_status,
    CASE
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t
WHERE flag_last = 1;

end_time := now();
RAISE NOTICE 'crm_cust_info duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

-- =====================================================
-- crm_prd_info
-- =====================================================

start_time := now();

TRUNCATE TABLE silver.crm_prd_info;

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
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, --Extract category from prd_key and replace '-' with '_'
    SUBSTRING(prd_key, 7) AS prd_key, --Extract product code from prd_key
    prd_nm,
    COALESCE(prd_cost, 0) AS prd_cost, --Replace NULL prd_cost with 0
    CASE UPPER(TRIM(prd_line)) 
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'n/a'
    END AS prd_line,
    prd_start_dt::DATE AS prd_start_dt,
    (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day')::DATE AS prd_end_dt --Set prd_end_dt to one day before the next prd_start_dt for the same prd_key, or NULL if it's the last record
FROM bronze.crm_prd_info;

end_time := now();
RAISE NOTICE 'crm_prd_info duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

-- =====================================================
-- crm_sales_details
-- =====================================================

start_time := now();

TRUNCATE TABLE silver.crm_sales_details;

INSERT INTO silver.crm_sales_details (
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
    CASE
        WHEN sls_order_dt = 0 OR length(sls_order_dt::text) != 8 THEN NULL
        ELSE TO_DATE(sls_order_dt::text, 'YYYYMMDD')
    END,
    CASE
        WHEN sls_ship_dt = 0 OR length(sls_ship_dt::text) != 8 THEN NULL
        ELSE TO_DATE(sls_ship_dt::text, 'YYYYMMDD')
    END,
    CASE
        WHEN sls_due_dt = 0 OR length(sls_due_dt::text) != 8 THEN NULL
        ELSE TO_DATE(sls_due_dt::text, 'YYYYMMDD')
    END,
    CASE
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END,
    sls_quantity,
    CASE
        WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END
FROM bronze.crm_sales_details;

end_time := now();
RAISE NOTICE 'crm_sales_details duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

-- =====================================================
-- ERP tables
-- =====================================================

RAISE NOTICE 'Loading ERP Tables';

-- =====================================================
-- erp_cust_az12
-- =====================================================

start_time := now();

TRUNCATE TABLE silver.erp_cust_az12;

INSERT INTO silver.erp_cust_az12 (
    cid,
    bdate,
    gen
)
SELECT
    CASE
        WHEN cid LIKE 'NAS%' THEN substring(cid, 4)
        ELSE cid
    END,
    CASE
        WHEN bdate > now() THEN NULL
        ELSE bdate
    END,
    CASE
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END
FROM bronze.erp_cust_az12;

end_time := now();
RAISE NOTICE 'erp_cust_az12 duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

-- =====================================================
-- erp_loc_a101
-- =====================================================

start_time := now();

TRUNCATE TABLE silver.erp_loc_a101;

INSERT INTO silver.erp_loc_a101 (
    cid,
    cntry
)
SELECT
    REPLACE(cid, '-', ''),
    CASE
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END
FROM bronze.erp_loc_a101;

end_time := now();
RAISE NOTICE 'erp_loc_a101 duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

-- =====================================================
-- erp_px_cat_g1v2
-- =====================================================

start_time := now();

TRUNCATE TABLE silver.erp_px_cat_g1v2;

INSERT INTO silver.erp_px_cat_g1v2
SELECT * FROM bronze.erp_px_cat_g1v2;

end_time := now();
RAISE NOTICE 'erp_px_cat_g1v2 duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

batch_end_time := now();

RAISE NOTICE '==========================================';
RAISE NOTICE 'Loading Silver Layer Completed';
RAISE NOTICE 'Total duration: % seconds',
EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
RAISE NOTICE '==========================================';

EXCEPTION
WHEN OTHERS THEN
RAISE NOTICE 'ERROR OCCURRED: %', SQLERRM;

END;
$$;

CALL silver.load_silver();
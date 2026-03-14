/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Description:
    This stored procedure loads raw data from CSV files into the bronze layer
    tables of the data warehouse. It handles data from two source systems:
    - CRM (Customer Relationship Management)
    - ERP (Enterprise Resource Planning)

    The procedure truncates existing data in the bronze tables and reloads
    from the source CSV files located in the mounted datasets directory.

Parameters:
    None

Tables Affected:
    - bronze.crm_cust_info
    - bronze.crm_prd_info
    - bronze.crm_sales_details
    - bronze.erp_loc_a101
    - bronze.erp_cust_az12
    - bronze.erp_px_cat_g1v2

Prerequisites:
    - PostgreSQL database with bronze schema created
    - CSV files available in /datasets/source_crm/ and /datasets/source_erp/
    - Tables must exist with matching column structures

Error Handling:
    - Catches and logs any errors during loading

Performance Notes:
    - Uses TRUNCATE for fast table clearing
    - Measures and logs load times for each table
    - Batch timing for overall process

===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
BEGIN

    -- Record start time for the entire batch
    batch_start_time := clock_timestamp();

    RAISE NOTICE '=====================================';
    RAISE NOTICE 'Loading Bronze Layer...';
    RAISE NOTICE '=====================================';

    -- =====================================================
    -- CRM tables
    -- =====================================================

    RAISE NOTICE '----------------------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables...';
    RAISE NOTICE '----------------------------------------------------------------';

    -- =====================================================
    -- crm_cust_info
    -- =====================================================

    start_time := clock_timestamp();

    TRUNCATE TABLE bronze.crm_cust_info;

    COPY bronze.crm_cust_info
    FROM '/datasets/source_crm/cust_info.csv'
    WITH (
        FORMAT csv,
        HEADER true
    );

    end_time := clock_timestamp();
    RAISE NOTICE 'crm_cust_info duration: % seconds',
        EXTRACT(EPOCH FROM (end_time - start_time));


    -- =====================================================
    -- crm_prd_info
    -- =====================================================

    start_time := clock_timestamp();

    TRUNCATE TABLE bronze.crm_prd_info;

    COPY bronze.crm_prd_info
    FROM '/datasets/source_crm/prd_info.csv'
    WITH (
        FORMAT csv,
        HEADER true
    );

    end_time := clock_timestamp();
    RAISE NOTICE 'crm_prd_info duration: % seconds',
        EXTRACT(EPOCH FROM (end_time - start_time));

    -- =====================================================
    -- crm_sales_details
    -- =====================================================

    start_time := clock_timestamp();

    TRUNCATE TABLE bronze.crm_sales_details;

    COPY bronze.crm_sales_details
    FROM '/datasets/source_crm/sales_details.csv'
    WITH (
        FORMAT csv,
        HEADER true
    );

    end_time := clock_timestamp();
    RAISE NOTICE 'crm_sales_details duration: % seconds',
        EXTRACT(EPOCH FROM (end_time - start_time));

    -- =====================================================
    -- ERP tables
    -- =====================================================

    RAISE NOTICE '----------------------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables...';
    RAISE NOTICE '----------------------------------------------------------------';
    
    -- =====================================================
    -- erp_loc_a101
    -- =====================================================
    
    start_time := clock_timestamp();

    TRUNCATE TABLE bronze.erp_loc_a101;

    COPY bronze.erp_loc_a101
    FROM '/datasets/source_erp/LOC_A101.csv'
    WITH (
        FORMAT csv,
        HEADER true
    );

    end_time := clock_timestamp();
    RAISE NOTICE 'erp_loc_a101 duration: % seconds',
        EXTRACT(EPOCH FROM (end_time - start_time));

    -- =====================================================
    -- erp_cust_az12
    -- =====================================================

    start_time := clock_timestamp();

    TRUNCATE TABLE bronze.erp_cust_az12;
    
    COPY bronze.erp_cust_az12
    FROM '/datasets/source_erp/CUST_AZ12.csv'
    WITH (
        FORMAT csv,
        HEADER true
    );

    end_time := clock_timestamp();
    RAISE NOTICE 'erp_cust_az12 duration: % seconds',
        EXTRACT(EPOCH FROM (end_time - start_time));
    
    -- =====================================================
    -- erp_loc_a101
    -- =====================================================
    
    start_time := clock_timestamp();

    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    COPY bronze.erp_px_cat_g1v2
    FROM '/datasets/source_erp/PX_CAT_G1V2.csv'
    WITH (
        FORMAT csv,
        HEADER true
    );

    end_time := clock_timestamp();
    RAISE NOTICE 'erp_px_cat_g1v2 duration: % seconds',
        EXTRACT(EPOCH FROM (end_time - start_time));

    ------------------------------------------------------------

    -- Record end time and calculate total batch duration
    batch_end_time := clock_timestamp();
    
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Bronze Layer Completed';
    RAISE NOTICE 'Total Load Duration: % seconds',
        EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
    RAISE NOTICE '==========================================';

EXCEPTION
    WHEN OTHERS THEN
        -- Log any errors that occurred during the process
        RAISE NOTICE 'Error occurred during Bronze loading: %', SQLERRM;
END;
$$;

-- Execute the stored procedure to load the Bronze layer
RAISE NOTICE 'Executing stored procedure to load Bronze layer...';
CALL bronze.load_bronze();
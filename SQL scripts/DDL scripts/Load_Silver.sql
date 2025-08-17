USE AnalyticsVault
GO

CREATE OR ALTER PROCEDURE Silver.Load_Silver AS
BEGIN
    DECLARE @StartTime DATETIME, @EndTime DATETIME, @StartTimeBatch DATETIME, @EndTimeBatch DATETIME
    SET @StartTimeBatch = GETDATE()

    BEGIN TRY
        --------------------------------------------
        -- Load crm_customer
        --------------------------------------------
        PRINT 'Starting load for Silver.crm_customer...'
        SET @StartTime = GETDATE()

        TRUNCATE TABLE Silver.crm_customer

        INSERT INTO Silver.crm_customer (
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
            TRIM(cst_firstname) as trimmed_firstname,
            TRIM(cst_lastname) as trimmed_lastname,
            CASE
                WHEN cst_marital_status = 'M' THEN 'Married'
                WHEN cst_marital_status = 'S' THEN 'Single'
                ELSE 'n/a'
            END as cst_marital_status,  
            CASE
                WHEN cst_gndr = 'M' THEN 'Male'
                WHEN cst_gndr = 'F' THEN 'Female'
                ELSE 'n/a'
            END as cst_gndr, 
            cst_create_date
        FROM (
            SELECT *,
                   RANK() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
            FROM Bronze.crm_customer
        ) AS SubqueryAlias
        WHERE flag_last = 1 AND cst_id IS NOT NULL;

        SET @EndTime = GETDATE()
        PRINT 'Finished loading Silver.crm_customer. Duration: ' 
              + CAST(DATEDIFF(second, @StartTime, @EndTime) AS VARCHAR) + ' seconds.'

        --------------------------------------------
        -- Load prd_info
        --------------------------------------------
        PRINT 'Starting load for Silver.prd_info...'
        SET @StartTime = GETDATE()

        TRUNCATE TABLE Silver.prd_info

        INSERT INTO Silver.prd_info (
            prd_id,
            prd_key,
            prd_cat_id,
            prd_sls_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT 
            prd_id,
            prd_key,
            REPLACE(SUBSTRING(prd_key, 1, CHARINDEX('-', prd_key, 4) - 1), '-', '_') as cat_id,
            SUBSTRING(prd_key, CHARINDEX('-', prd_key, 4) + 1, LEN(prd_key) - 6) sls_prd_key, 
            prd_nm,
            ISNULL(prd_cost, 0) as prd_cost,
            CASE prd_line
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'    
                ELSE 'n/a'
            END as prd_line,
            prd_start_dt,
            DATEADD(DAY, -1, LEAD(prd_start_dt, 1) OVER (PARTITION BY prd_key ORDER BY prd_start_dt ASC)) as prd_end_dt
        FROM Bronze.prd_info;

        SET @EndTime = GETDATE()
        PRINT 'Finished loading Silver.prd_info. Duration: ' 
              + CAST(DATEDIFF(second, @StartTime, @EndTime) AS VARCHAR) + ' seconds.'

        --------------------------------------------
        -- Load sales_details
        --------------------------------------------
        PRINT 'Starting load for Silver.sales_details...'
        SET @StartTime = GETDATE()

        TRUNCATE TABLE Silver.sales_details

        INSERT INTO Silver.sales_details (
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
                WHEN sls_order_dt <= 0 OR sls_order_dt > 20300101 OR sls_order_dt < 19000101 OR LEN(sls_order_dt) != 8 
                     THEN NULL 
                ELSE CAST(sls_order_dt as DATE)
           END AS sls_order_dt,
           CASE 
                WHEN sls_ship_dt <= 0 OR sls_ship_dt > 20300101 OR sls_ship_dt < 19000101 OR LEN(sls_ship_dt) != 8 
                     THEN NULL 
                ELSE CAST(sls_ship_dt as DATE)
           END AS sls_ship_dt,
           CASE 
                WHEN sls_due_dt <= 0 OR sls_due_dt > 20300101 OR sls_due_dt < 19000101 OR LEN(sls_due_dt) != 8 
                     THEN NULL 
                ELSE CAST(sls_due_dt as DATE)
           END AS sls_due_dt,
           CASE 
                WHEN sls_sales IS NULL OR sls_sales <= 0 THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
           END as sls_sales,
           sls_quantity,
           CASE 
                WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0) 
                ELSE sls_price
            END as sls_price
        FROM Bronze.sales_details;

        SET @EndTime = GETDATE()
        PRINT 'Finished loading Silver.sales_details. Duration: ' 
              + CAST(DATEDIFF(second, @StartTime, @EndTime) AS VARCHAR) + ' seconds.'

        --------------------------------------------
        -- Load erp_cust_az12
        --------------------------------------------
        PRINT 'Starting load for Silver.erp_cust_az12...'
        SET @StartTime = GETDATE()

        TRUNCATE TABLE Silver.erp_cust_az12

        INSERT INTO Silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT 
            CASE 
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid) - 3)
                ELSE cid
            END as cid,
            CASE 
                WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END AS bdate,
            CASE 
                WHEN gen = 'F' THEN 'Female'
                WHEN gen = 'M' THEN 'Male'
                WHEN gen = ' ' OR gen IS NULL THEN 'n/a'
                ELSE gen
            END AS gen
        FROM Bronze.erp_cust_az12;

        SET @EndTime = GETDATE()
        PRINT 'Finished loading Silver.erp_cust_az12. Duration: ' 
              + CAST(DATEDIFF(second, @StartTime, @EndTime) AS VARCHAR) + ' seconds.'

        --------------------------------------------
        -- Load erp_loc_a101
        --------------------------------------------
        PRINT 'Starting load for Silver.erp_loc_a101...'
        SET @StartTime = GETDATE()

        TRUNCATE TABLE Silver.erp_loc_a101

        INSERT INTO Silver.erp_loc_a101 (
            cid, 
            cntry
        )
        SELECT 
            REPLACE(cid, '-', '') as cid,
            CASE    
                WHEN cntry = 'DE' THEN 'Germany'
                WHEN cntry IN ('US', 'USA') THEN 'United States'
                WHEN cntry = '' OR cntry IS NULL THEN 'n/a'
                ELSE cntry
            END AS cntry
        FROM Bronze.erp_loc_a101;

        SET @EndTime = GETDATE()
        PRINT 'Finished loading Silver.erp_loc_a101. Duration: ' 
              + CAST(DATEDIFF(second, @StartTime, @EndTime) AS VARCHAR) + ' seconds.'

        --------------------------------------------
        -- Load erp_px_cat_g1v2
        --------------------------------------------
        PRINT 'Starting load for Silver.erp_px_cat_g1v2...'
        SET @StartTime = GETDATE()

        TRUNCATE TABLE Silver.erp_px_cat_g1v2

        INSERT INTO Silver.erp_px_cat_g1v2 (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT * FROM Bronze.erp_px_cat_g1v2;

        SET @EndTime = GETDATE()
        PRINT 'Finished loading Silver.erp_px_cat_g1v2. Duration: ' 
              + CAST(DATEDIFF(second, @StartTime, @EndTime) AS VARCHAR) + ' seconds.'

        --------------------------------------------
        -- Whole Batch Duration
        --------------------------------------------
        SET @EndTimeBatch = GETDATE();
        PRINT '=================================================='
        PRINT 'Load duration of entire Silver Layer: ' 
              + CAST(DATEDIFF(second, @StartTimeBatch, @EndTimeBatch) AS VARCHAR) + ' seconds.'
        PRINT '=================================================='

    END TRY
    BEGIN CATCH
        PRINT '====================================';
        PRINT 'There were errors during loading';
        PRINT 'Error message: ' + ERROR_MESSAGE();
        PRINT 'Error number: ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'Error state: ' + CAST(ERROR_STATE() AS VARCHAR);
    END CATCH
END

EXEC Silver.Load_Silver

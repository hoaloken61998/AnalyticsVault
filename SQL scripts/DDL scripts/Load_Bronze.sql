/*
================================================================================
Stored Procedure Name : Bronze.load_bronze
Purpose              :
    This stored procedure automates the data loading process from source 
    systems (CRM and ERP) into the Bronze layer of the AnalyticsVault data 
    warehouse. It performs the following actions:
        - Truncates target Bronze tables to ensure a clean load.
        - Loads CSV source files from the local file system into Bronze tables 
          using BULK INSERT.
        - Captures and logs execution time for each table load.
        - Separately handles CRM and ERP data sources.
        - Implements TRY...CATCH for error handling and logs error details.
        - Provides total batch load duration.

Data Sources         :
    - CRM System:
        1. cust_info.csv        → Bronze.crm_customer
        2. prd_info.csv         → Bronze.prd_info
        3. sales_details.csv    → Bronze.sales_details
    - ERP System:
        1. LOC_A101.csv         → Bronze.erp_loc_a101
        2. CUST_AZ12.csv        → Bronze.erp_cust_az12
        3. PX_CAT_G1V2.csv      → Bronze.erp_px_cat_g1v2

Execution            :
    EXEC Bronze.load_bronze

Notes                :
    - Ensure that the source CSV files exist in the specified local file paths.
    - The BULK INSERT requires appropriate SQL Server permissions and 
      access to the file system.
    - Intended for periodic or batch ingestion as part of the ETL pipeline.

================================================================================
*/



USE AnalyticsVault
Go

CREATE OR ALTER PROCEDURE Bronze.load_bronze as 
BEGIN
DECLARE @StartTime DATETIME, @EndTime DATETIME, @StartTimeBatch DATETIME, @EndTimeBatch DATETIME
	SET @StartTimeBatch = GETDATE();
	BEGIN TRY
		PRINT '==================================='
		PRINT 'Load data from CRM system into Bronze layer';
		PRINT '==================================='

		SET @StartTime = GETDATE();
		TRUNCATE TABLE Bronze.crm_customer;
		BULK INSERT Bronze.crm_customer
		FROM 'C:\Users\Acer\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK
		)
		SET @EndTime = GETDATE();
		PRINT 'Load duration:' + CAST(DATEDIFF(second, @StartTime, @EndTime) AS NVARCHAR) + 'seconds'

		SET @StartTime = GETDATE();
		TRUNCATE TABLE Bronze.prd_info;
		BULK INSERT Bronze.prd_info
		FROM 'C:\Users\Acer\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @EndTime = GETDATE();
		PRINT 'Load duration:' + CAST(DATEDIFF(second, @StartTime, @EndTime) AS NVARCHAR) + 'seconds'

		SET @StartTime = GETDATE();
		TRUNCATE TABLE Bronze.sales_details;
		BULK INSERT Bronze.sales_details
		FROM 'C:\Users\Acer\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @EndTime = GETDATE();
		PRINT 'Load duration:' + CAST(DATEDIFF(second, @StartTime, @EndTime) AS NVARCHAR) + 'seconds'

		PRINT '==================================='
		PRINT 'Load data from ERP system into Bronze layer';
		PRINT '==================================='

		SET @StartTime = GETDATE();
		TRUNCATE TABLE Bronze.erp_loc_a101;
		BULK INSERT Bronze.erp_loc_a101
		FROM 'C:\Users\Acer\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @EndTime = GETDATE();
		PRINT 'Load duration:' + CAST(DATEDIFF(second, @StartTime, @EndTime) AS NVARCHAR) + 'seconds'

		SET @StartTime = GETDATE()
		TRUNCATE TABLE Bronze.erp_cust_az12;
		BULK INSERT Bronze.erp_cust_az12
		FROM 'C:\Users\Acer\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @EndTime = GETDATE();
		PRINT 'Load duration:' + CAST(DATEDIFF(second, @StartTime, @EndTime) AS NVARCHAR) + 'seconds'

		SET @StartTime = GETDATE();
		TRUNCATE TABLE Bronze.erp_px_cat_g1v2;
		BULK INSERT Bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Acer\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @EndTime = GETDATE();
		PRINT 'Load duration:' + CAST(DATEDIFF(second, @StartTime, @EndTime) AS NVARCHAR) + 'seconds'

	END TRY
	BEGIN CATCH
		PRINT '===================================='
		PRINT 'There are errors during loading data into Bronze layer'
		PRINT 'Error message:' + ERROR_MESSAGE();
		PRINT 'Error number:' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error state:' + CAST(ERROR_STATE() AS NVARCHAR); 
		PRINT '===================================='
	END CATCH
	SET @EndTimeBatch = GETDATE();
	PRINT '>> Total Load duration:' + CAST(DATEDIFF(second, @StartTimeBatch, @EndTimeBatch) as NVARCHAR) + 'seconds'
END

EXEC Bronze.load_bronze



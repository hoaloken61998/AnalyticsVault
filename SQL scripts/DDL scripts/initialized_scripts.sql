/*
Overview:
This script automates the setup of the AnalyticsVault database for a Medallion architecture-based data warehouse.

Steps performed:
1. Checks if the database 'AnalyticsVault' exists; if yes, sets it to SINGLE_USER mode to disconnect users and then drops it.
2. Creates a new 'AnalyticsVault' database.
3. Switches to the new database context.
4. Creates three schemas—Bronze, Silver, and Gold—representing the layers of the Medallion architecture:
   - Bronze: Raw data ingestion layer
   - Silver: Cleaned and transformed data layer
   - Gold: Aggregated, analytics-ready data layer

This prepares the foundational structure for implementing ETL pipelines and analytics workflows.
*/


-- Drop datawarehouse AnalyticsVault if exist and then recreate it
IF EXISTS (SELECT * FROM sys.databases WHERE name = 'AnalyticsVault')
BEGIN
    -- Set the database to SINGLE_USER mode to disconnect all other users immediately
    -- ROLLBACK IMMEDIATE will rollback any open transactions by those users
    ALTER DATABASE [AnalyticsVault] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    -- Drop the existing database now that it has no active connections
    DROP DATABASE [AnalyticsVault];
END;
Go

-- Switch context to the master database before creating a new database
Use master;
Create Database AnalyticsVault;
Go

-- Switch context to the newly created database
Use AnalyticsVault;
Go

-- Create schema 'Bronze' for raw data layer in the Medallion architecture
-- Create schema 'Gold' for aggregated, analytics-ready data layer
-- Create schema 'Silver' for cleaned and transformed data layer
Create Schema Bronze;
Go
Create Schema Gold; 
Go
Create Schema Silver; 
Go

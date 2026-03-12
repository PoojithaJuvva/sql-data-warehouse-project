/*=====================================================================
Stored Procedure: bronze.load_bronze
-----------------------------------------------------------------------
Purpose:
    Loads data into the 'bronze' schema from external CSV files.

Actions Performed:
    - Truncates the bronze tables before loading data.
    - Uses BULK INSERT to load data from CSV files into bronze tables.

Parameters:
    None

Returns:
    None

Error Handling:
    Implemented with TRY...CATCH to capture and log errors.

Usage Example:
    EXEC bronze.load_bronze;
=====================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE @start_time DATETIME = GETDATE(),
            @end_time DATETIME,
            @batch_start_time DATETIME,
            @batch_end_time DATETIME;

    BEGIN TRY
        PRINT '-----------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '-----------------------------------------------';

        SET @batch_start_time = GETDATE();

        -- CRM Customer Info
        TRUNCATE TABLE bronze.crm_cust_info;
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\pooji\OneDrive\Desktop\sql\dwh_proj\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        -- CRM Product Info
        TRUNCATE TABLE bronze.crm_prd_info;
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\pooji\OneDrive\Desktop\sql\dwh_proj\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        -- CRM Sales Details
        TRUNCATE TABLE bronze.crm_sales_details;
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\pooji\OneDrive\Desktop\sql\dwh_proj\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @batch_end_time = GETDATE();
        PRINT '>> CRM LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';

        PRINT '-----------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '-----------------------------------------------';

        SET @batch_start_time = GETDATE();

        -- ERP Customer AZ12
        TRUNCATE TABLE bronze.erp_cust_az12;
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\pooji\OneDrive\Desktop\sql\dwh_proj\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        -- ERP Location A101
        TRUNCATE TABLE bronze.erp_loc_a101;
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\pooji\OneDrive\Desktop\sql\dwh_proj\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        -- ERP PX Category G1V2
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\pooji\OneDrive\Desktop\sql\dwh_proj\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @batch_end_time = GETDATE();
        PRINT '>> ERP LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';

        SET @end_time = GETDATE();
        PRINT '-----------------------------------------------';
        PRINT 'TOTAL LOAD DURATION: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '-----------------------------------------------';
    END TRY
    BEGIN CATCH
        PRINT '*** ERROR OCCURRED DURING LOAD ***';
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Line: ' + CAST(ERROR_LINE() AS NVARCHAR);
        PRINT 'Error Message: ' + ERROR_MESSAGE();
    END CATCH
END;

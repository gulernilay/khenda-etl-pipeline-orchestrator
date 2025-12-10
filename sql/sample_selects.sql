/* ===================================================================
   SAMPLE SELECT QUERIES FOR DEBUGGING ETL OUTPUT
   Author: Chef Seasons – Data Engineering Team
   =================================================================== */

-- Last 50 rows loaded into Table1 (Pipeline 1)
SELECT TOP 50 *
FROM Table1_ETL
ORDER BY source_load_time DESC;
GO

-- Last 50 rows loaded into Table2 (Pipeline 2)
SELECT TOP 50 *
FROM Table2_ETL
ORDER BY source_load_time DESC;
GO
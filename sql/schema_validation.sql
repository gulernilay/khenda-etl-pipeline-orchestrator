/* ===================================================================
   SCHEMA VALIDATION FOR EXISTING ETL TABLES
   (Talep Üzerine Create Table KALDIRILDI)
   Author: Chef Seasons – Data Engineering Team
   =================================================================== */

----------------------------------------------------
-- Validate Table 1 Column Structure
----------------------------------------------------
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'Table1_ETL'
ORDER BY ORDINAL_POSITION;
GO

----------------------------------------------------
-- Validate Table 2 Column Structure
----------------------------------------------------
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'Table2_ETL'
ORDER BY ORDINAL_POSITION;
GO
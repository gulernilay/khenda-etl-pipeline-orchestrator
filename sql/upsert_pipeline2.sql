/* ===================================================================
   UPSERT Script for Pipeline 2 → Target Table: khenda_hygiene
   Author: Chef Seasons – Data Engineering Team
   =================================================================== */

-- 1) STAGING TABLE OLUŞTUR
IF OBJECT_ID('tempdb..#P2_Staging') IS NOT NULL
    DROP TABLE #P2_Staging;

CREATE TABLE #P2_Staging (
    id BIGINT NOT NULL,
    hygieneId INT NOT NULL,
    datetime DATETIME NOT NULL,
    valid BIT NOT NULL,
    duration FLOAT NOT NULL
);

------------------------------------------------------------
-- 2) ETL TARAFINDAN GELECEK BATCH BURAYA INSERT EDİLİR
------------------------------------------------------------
/*
INSERT INTO #P2_Staging (id, hygieneId, datetime, valid, duration)
VALUES
    (1, 101, '2025-01-01 10:00:00', 1, 12.5),
    (2, 102, '2025-01-01 10:05:00', 1, 8.7),
    ...
;
*/
------------------------------------------------------------

------------------------------------------------------------
-- 3) MERGE İLE UPSERT İŞLEMİ
------------------------------------------------------------
MERGE ChefsAI.dbo.khenda_hygiene AS TARGET
USING #P2_Staging AS SOURCE
    ON TARGET.id = SOURCE.id

WHEN MATCHED THEN
    UPDATE SET
        TARGET.hygieneId = SOURCE.hygieneId,
        TARGET.datetime  = SOURCE.datetime,
        TARGET.valid     = SOURCE.valid,
        TARGET.duration  = SOURCE.duration

WHEN NOT MATCHED BY TARGET THEN
    INSERT (id, hygieneId, datetime, valid, duration)
    VALUES (SOURCE.id, SOURCE.hygieneId, SOURCE.datetime, SOURCE.valid, SOURCE.duration);

-- Eğer API tarafında artık dönmeyen kayıtları silmek istersen:
-- WHEN NOT MATCHED BY SOURCE THEN
--     DELETE;

------------------------------------------------------------
-- 4) ETKİLENEN SATIR SAYISI
------------------------------------------------------------
SELECT @@ROWCOUNT AS RowsAffected;
GO
/* ===================================================================
   UPSERT Script for Pipeline 1 → Target Table: Khenda_Uretim_Cevrim
   Author: Chef Seasons – Data Engineering Team
   =================================================================== */

-- TEMP STAGING TABLE
IF OBJECT_ID('tempdb..#P1_Staging') IS NOT NULL
    DROP TABLE #P1_Staging;

CREATE TABLE #P1_Staging (
    Id INT NOT NULL,
    LineId INT NULL,
    IsEmriNo BIGINT NULL,
    Tarih NVARCHAR(200) NULL,
    Musteri NVARCHAR(200) NULL,
    UrunKodu NVARCHAR(100) NULL,
    UrunAdi NVARCHAR(510) NULL,
    PartiNo NVARCHAR(100) NULL,
    OdaNo INT NULL,

    HedefCevrimSure FLOAT NULL,
    HedefKisiSayisi INT NULL,
    UretimBirimMiktar FLOAT NULL,
    PlanlananMiktar FLOAT NULL,
    GerceklesenUretimMiktar FLOAT NULL,
    GerceklesenUretimMiktarKhenda FLOAT NULL,
    GerceklesenUretimMiktarFarki FLOAT NULL,
    PlanlananIsGucu FLOAT NULL,
    GerceklesenIsGucuKhenda FLOAT NULL,
    PlanlananBirimIsGucu FLOAT NULL,
    GerceklesenBirimIsGucuKhenda FLOAT NULL,
    GerceklesenCevrimSure FLOAT NULL,
    GerceklesenCevrimSureKhenda FLOAT NULL,

    LoadTime DATETIME2 DEFAULT SYSDATETIME()
);

--------------------------------------------------------------
-- INSERT INTO STAGING (Python ETL tarafından doldurulacak)
--------------------------------------------------------------
/*
INSERT INTO #P1_Staging (...)
VALUES (...), (...), ...
*/
--------------------------------------------------------------

--------------------------------------------------------------
-- MERGE INTO TARGET TABLE
--------------------------------------------------------------
MERGE ChefsAI.dbo.Khenda_Uretim_Cevrim AS TARGET
USING #P1_Staging AS SOURCE
    ON TARGET.Id = SOURCE.Id   -- Primary key eşleşmesi

WHEN MATCHED THEN
    UPDATE SET
        TARGET.LineId = SOURCE.LineId,
        TARGET.IsEmriNo = SOURCE.IsEmriNo,
        TARGET.Tarih = SOURCE.Tarih,
        TARGET.Musteri = SOURCE.Musteri,
        TARGET.UrunKodu = SOURCE.UrunKodu,
        TARGET.UrunAdi = SOURCE.UrunAdi,
        TARGET.PartiNo = SOURCE.PartiNo,
        TARGET.OdaNo = SOURCE.OdaNo,

        TARGET.HedefCevrimSure = SOURCE.HedefCevrimSure,
        TARGET.HedefKisiSayisi = SOURCE.HedefKisiSayisi,
        TARGET.UretimBirimMiktar = SOURCE.UretimBirimMiktar,
        TARGET.PlanlananMiktar = SOURCE.PlanlananMiktar,
        TARGET.GerceklesenUretimMiktar = SOURCE.GerceklesenUretimMiktar,
        TARGET.GerceklesenUretimMiktarKhenda = SOURCE.GerceklesenUretimMiktarKhenda,
        TARGET.GerceklesenUretimMiktarFarki = SOURCE.GerceklesenUretimMiktarFarki,
        TARGET.PlanlananIsGucu = SOURCE.PlanlananIsGucu,
        TARGET.GerceklesenIsGucuKhenda = SOURCE.GerceklesenIsGucuKhenda,
        TARGET.PlanlananBirimIsGucu = SOURCE.PlanlananBirimIsGucu,
        TARGET.GerceklesenBirimIsGucuKhenda = SOURCE.GerceklesenBirimIsGucuKhenda,
        TARGET.GerceklesenCevrimSure = SOURCE.GerceklesenCevrimSure,
        TARGET.GerceklesenCevrimSureKhenda = SOURCE.GerceklesenCevrimSureKhenda,

        TARGET.LoadTime = SYSDATETIME()

WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        Id, LineId, IsEmriNo, Tarih, Musteri, UrunKodu, UrunAdi, PartiNo, OdaNo,
        HedefCevrimSure, HedefKisiSayisi, UretimBirimMiktar, PlanlananMiktar,
        GerceklesenUretimMiktar, GerceklesenUretimMiktarKhenda, GerceklesenUretimMiktarFarki,
        PlanlananIsGucu, GerceklesenIsGucuKhenda, PlanlananBirimIsGucu,
        GerceklesenBirimIsGucuKhenda, GerceklesenCevrimSure, GerceklesenCevrimSureKhenda,
        LoadTime
    )
    VALUES (
        SOURCE.Id, SOURCE.LineId, SOURCE.IsEmriNo, SOURCE.Tarih, SOURCE.Musteri,
        SOURCE.UrunKodu, SOURCE.UrunAdi, SOURCE.PartiNo, SOURCE.OdaNo,
        SOURCE.HedefCevrimSure, SOURCE.HedefKisiSayisi, SOURCE.UretimBirimMiktar,
        SOURCE.PlanlananMiktar, SOURCE.GerceklesenUretimMiktar, SOURCE.GerceklesenUretimMiktarKhenda,
        SOURCE.GerceklesenUretimMiktarFarki, SOURCE.PlanlananIsGucu, SOURCE.GerceklesenIsGucuKhenda,
        SOURCE.PlanlananBirimIsGucu, SOURCE.GerceklesenBirimIsGucuKhenda,
        SOURCE.GerceklesenCevrimSure, SOURCE.GerceklesenCevrimSureKhenda,
        SYSDATETIME()
    );

--------------------------------------------------------------
SELECT @@ROWCOUNT AS RowsAffected;
GO

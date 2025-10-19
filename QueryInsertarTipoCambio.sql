USE [DW_Sales];
GO


-- 0 Vaciar DIM_Time y reiniciar el IDENTITY
USE [DW_Sales];
GO

DELETE FROM [dbo].[DIM_Time];
GO

DBCC CHECKIDENT ('[dbo].[DIM_Time]', RESEED, 0);
GO


-- 1 Crear tabla temporal como VARCHAR
IF OBJECT_ID('tempdb..#TiposCambio') IS NOT NULL DROP TABLE #TiposCambio;

CREATE TABLE #TiposCambio (
    Fecha VARCHAR(50),
    TipoCambio_USD_CRC VARCHAR(20)
);

--------------------------- 2 Cargar CSV ---------------------------
BULK INSERT #TiposCambio
FROM 'D:\VsCode\TallerDW\TipoCambioDW.csv'   ----------------------- ← Ruta
WITH (
    FIELDTERMINATOR = ';',  
    ROWTERMINATOR = '\n',  
    FIRSTROW = 2,           
    CODEPAGE = '65001',      
    TABLOCK
);
GO

-- 3 Insertar en DIM_Time limpiando fechas
INSERT INTO [dbo].[DIM_Time] (
    [date], [day], [month], [year],
    [tipoCambio], [numberMonth], [quarter]
)
SELECT
    FechaLimpia AS [date],
    DAY(FechaLimpia) AS [day],
    DATENAME(MONTH, FechaLimpia) AS [month],
    YEAR(FechaLimpia) AS [year],
    CAST(REPLACE(TipoCambio_USD_CRC, ',', '.') AS DECIMAL(10,2)) AS [tipoCambio],
    MONTH(FechaLimpia) AS [numberMonth],
    DATEPART(QUARTER, FechaLimpia) AS [quarter]
FROM (
    SELECT 
        TRY_CONVERT(date, REPLACE(LTRIM(RTRIM(Fecha)), '/', '-')) AS FechaLimpia,
        TipoCambio_USD_CRC
    FROM #TiposCambio
) AS C
WHERE FechaLimpia IS NOT NULL
ORDER BY FechaLimpia;
GO

-- 4 Revisar filas que todavía no pudieron convertirse
SELECT *
FROM #TiposCambio
WHERE TRY_CONVERT(date, REPLACE(LTRIM(RTRIM(Fecha)), '/', '-')) IS NULL;
GO

-- 5 Verificar datos insertados
SELECT * FROM [dbo].[DIM_Time] ORDER BY [date];
GO

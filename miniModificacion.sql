ALTER TABLE [DW_Sales].[dbo].[DIM_SalesPerson] 
ADD 
    [active] [char](1) NOT NULL DEFAULT 'Y',
    [gestor] [nvarchar](100) NULL;
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[SKW_UPSERT_FLIGHT_UTILIZATION] 
AS
BEGIN     


--Calculate FCyc 

--count # of occurences of each serial_num
    -- INSERT INTO [dbo].[SKW_FLIGHT_UTILIZATION] (ID, Serial_num, Flight_duration_hrs, Flight_date)
    -- SELECT ID, Serial_num, Flight_duration_hrs, Flight_date
    -- FROM [dbo].[SKW_STAGE_FLIGHT_UTILIZATION]
    

-- WITH CTE_WITH_FCyc AS (
--     SELECT Serial_num, count(Serial_num) as FCyc
--     FROM [dbo].[SKW_STAGE_FLIGHT_UTILIZATION]
--     group by Serial_num
--     )

   

    -- MERGE [dbo].[SKW_STAGE_FLIGHT_UTILIZATION] AS target
    -- USING [CTE_WITH_FCyc] AS source
    -- ON (
    --     target.Serial_num = source.Serial_num
    --     )
    -- WHEN MATCHED THEN
    -- UPDATE SET 
    -- FCyc = source.FCyc;
    
    -- set flight cycles 
    UPDATE SKW_STAGE_FLIGHT_UTILIZATION
    SET FCyc = 1;


--Change date format , works without it, but can do later


--Remove duplicate rows in staging table
    WITH CTE AS (
        SELECT 
            *, 
            ROW_NUMBER() OVER (
                PARTITION BY 
                    ID
                ORDER BY 
                    ID
            ) ROW_NUM
            FROM [dbo].[SKW_STAGE_FLIGHT_UTILIZATION]
    )
    DELETE FROM CTE
    WHERE ROW_NUM > 1;


--Upsert Logic    
    MERGE [dbo].[SKW_FLIGHT_UTILIZATION] AS target
    USING [dbo].[SKW_STAGE_FLIGHT_UTILIZATION] AS source
    ON (
        target.ID = source.ID
        )
    
    WHEN MATCHED THEN
    UPDATE SET 
    Flight_date = source.Flight_date,
    ID = source.ID,
    Serial_num = source.Serial_num,
    Flight_duration_hrs = source.Flight_duration_hrs,
    FCyc = source.FCyc
    
    
    WHEN NOT matched THEN
        INSERT (
    Flight_date,
    ID,
    Serial_num,
    Flight_duration_hrs,
    FCyc
        )
    VALUES (
    source.Flight_date,
    source.ID,
    source.Serial_num,
    source.Flight_duration_hrs,
    source.FCyc
        );
    
    TRUNCATE TABLE [dbo].[SKW_STAGE_FLIGHT_UTILIZATION]

END
GO

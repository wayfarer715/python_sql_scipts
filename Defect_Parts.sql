SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[SKW_UPSERT_DEFECT_PARTS] 
AS
BEGIN     
    
    --Remove duplicate rows from staging 
    WITH CTE AS (
        SELECT 
            *, 
            ROW_NUMBER() OVER (
                PARTITION BY 
                    id, 
                    LastModifiedDate
                ORDER BY 
                    id, 
                    LastModifiedDate
            ) ROW_NUM
            FROM [dbo].[SKW_STAGE_DEFECT_PARTS]
    )
    DELETE FROM CTE
    WHERE ROW_NUM > 1;

    -- only keep the lastest record 
    DELETE n1 FROM SKW_STAGE_DEFECT_PARTS n1, SKW_STAGE_DEFECT_PARTS n2 WHERE n1.LastModifiedDate < n2.LastModifiedDate AND n1.id = n2.id
       


    MERGE [dbo].[SKW_DEFECT_PARTS] AS target
    USING [dbo].[SKW_STAGE_DEFECT_PARTS] AS source
    ON (
        target.id= source.id 
        )
    WHEN MATCHED THEN
        UPDATE SET 
        PartOn = source.PartOn,
        SerialNum = source.SerialNum,
        PartOff = source.PartOff,
        SerialOff = source.SerialOff,
        PartOnDescription = source.PartOnDescription,
        PartOffDescription = source.PartOffDescription,
        PartOnLocation = source.PartOnLocation,
        PartOffLocation = source.PartOffLocation,
        Position = source.Position,
        Scheduled = source.Scheduled,
        InspectionAction = source.InspectionAction,
        ReasonForRemoval = source.ReasonForRemoval,
        Comments = source.Comments,
        report_date_time = source.report_date_time,
        ParentCampID = source.ParentCampID,
        LastModifiedDate = source.LastModifiedDate
        
    WHEN NOT MATCHED THEN
        INSERT (
        id,
        PartOn,
        SerialNum,
        PartOff,
        SerialOff,
        PartOnDescription,
        PartOffDescription,
        PartOnLocation,
        PartOffLocation,
        Position,
        Scheduled,
        InspectionAction,
        ReasonForRemoval,
        Comments,
        report_date_time,
        ParentCampID,
        LastModifiedDate
            )
        VALUES (
        source.id,
        source.PartOn,
        source.SerialNum,
        source.PartOff,
        source.SerialOff,
        source.PartOnDescription,
        source.PartOffDescription,
        source.PartOnLocation,
        source.PartOffLocation,
        source.Position,
        source.Scheduled,
        source.InspectionAction,
        source.ReasonForRemoval,
        source.Comments,
        source.report_date_time,
        source.ParentCampID,
        source.LastModifiedDate
            );
        
        TRUNCATE TABLE [dbo].[SKW_STAGE_DEFECT_PARTS]
END


GO

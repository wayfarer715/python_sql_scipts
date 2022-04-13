SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[SKW_UPSERT_DEFECT_INFO] 
AS
BEGIN     
       

        --Remove duplicate rows from staging 
        WITH CTE AS (
            SELECT 
                *, 
                ROW_NUMBER() OVER (
                    PARTITION BY 
                        defect_report_defect,
                        last_modified_date
                      
                    ORDER BY 
                        defect_report_defect,
                        last_modified_date
                      
                ) ROW_NUM
                FROM [dbo].[SKW_STAGE_DEFECT_INFO]
        )
        DELETE FROM CTE
        WHERE ROW_NUM > 1;


        --Only keep records with latest date in staging, delete the rest 
        DELETE n1 FROM SKW_STAGE_DEFECT_INFO n1, SKW_STAGE_DEFECT_INFO n2 WHERE n1.last_modified_date < n2.last_modified_date AND n1.defect_report_defect = n2.defect_report_defect

        
        -- populate the defect report status column based on resolved date values 
        UPDATE dbo.SKW_STAGE_DEFECT_INFO 
        SET defect_report_status = CASE WHEN resolved_date = '1900-01-01 00:00:00' THEN 'OPEN' ELSE 'CLOSED' END;

        --Change P to Pilot and M to Maintenance
        UPDATE dbo.SKW_STAGE_DEFECT_INFO 
        SET defect_report_defect_type = CASE WHEN defect_report_defect_type = 'P' THEN 'Pilot Reports' ELSE 'Maintenance Reports' END;

        --add 0s in front of 3 digit ATA 
        UPDATE
            dbo.SKW_STAGE_DEFECT_INFO
        SET
            ata_humber = '0' + ata_humber
        WHERE
            LEN(ata_humber) = 3;



        MERGE [dbo].[SKW_DEFECT_INFO] AS target
        USING [dbo].[SKW_STAGE_DEFECT_INFO] AS source
        ON (
            target.defect_report_defect = source.defect_report_defect 
            )
        
        WHEN MATCHED THEN
        UPDATE SET 
        defect_report_defect = source.defect_report_defect,
        CRJ_series = source.CRJ_series,
        registration_num = source.registration_num,
        ata = source.ata,
        ata_humber = source.ata_humber,
        defect_report_defect_type = source.defect_report_defect_type,
        defect_report_status = source.defect_report_status,
        description = source.description,
        defect_report_defect_descripti = source.defect_report_defect_descripti,
        corrective_action = source.corrective_action,
        defect_report_resolution_descr = source.defect_report_resolution_descr,
        reported_date = source.reported_date,
        resolved_date = source.resolved_date,
        minutes_of_delay = source.minutes_of_delay,
        cancels= source.cancels,
        delay_frequency = source.delay_frequency,
        defect_report_station = source.defect_report_station,
        defect_report_resolved_locatio = source.defect_report_resolved_locatio,
        part_count = source.part_count,
        defect_report_mel_number = source.defect_report_mel_number,
        AOG = source.AOG,
        last_modified_date  = source.last_modified_date,
        entry_origin = source.entry_origin,
        created_date = source.created_date
     
        WHEN NOT matched THEN
            INSERT (
        defect_report_defect, 
        CRJ_series ,
        registration_num ,
        ata ,
        ata_humber ,
        defect_report_defect_type,
        defect_report_status,
        description,
        defect_report_defect_descripti,
        corrective_action ,
        defect_report_resolution_descr ,
        reported_date ,
        resolved_date ,
        minutes_of_delay,
        cancels,
        delay_frequency,
        defect_report_station,
        defect_report_resolved_locatio,
        part_count ,
        defect_report_mel_number,
        AOG,
        last_modified_date,
        entry_origin,
        created_date
            )
        VALUES (
        source.defect_report_defect,
        source.CRJ_series,
        source.registration_num,
        source.ata,
        source.ata_humber,
        source.defect_report_defect_type,
        source.defect_report_status,
        source.description,
        source.defect_report_defect_descripti,
        source.corrective_action,
        source.defect_report_resolution_descr,
        source.reported_date,
        source.resolved_date,
        source.minutes_of_delay,
        source.cancels,
        source.delay_frequency,
        source.defect_report_station,
        source.defect_report_resolved_locatio,
        source.part_count,
        source.defect_report_mel_number,
        source.AOG,
        source.last_modified_date,
        source.entry_origin,
        source.created_date
        
            );
        
        TRUNCATE TABLE [dbo].[SKW_STAGE_DEFECT_INFO]

END


GO

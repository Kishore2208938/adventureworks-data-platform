USE AW_ETL_Framework;
GO

INSERT INTO control.Watermark
(
    SourceTableID,
    WatermarkColumn,
    LastSuccessfulValue,
    LastRunStatus
)
SELECT
    SourceTableID,
    WatermarkColumn,
    NULL,
    'NOT_STARTED'
FROM metadata.TableLoadConfiguration
WHERE LoadType='INCREMENTAL';
GO
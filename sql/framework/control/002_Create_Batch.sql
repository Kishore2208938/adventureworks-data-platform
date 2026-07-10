-- ============================================================
-- Script  : 002_Create_Batch.sql
-- Schema  : control
-- Purpose : Tracks pipeline batch/run instances
-- ============================================================

USE AW_ETL_Framework;
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'control' AND t.name = 'Batch'
)
BEGIN
    CREATE TABLE control.Batch
    (
        BatchID             INT             NOT NULL IDENTITY(1,1),
        BatchName           NVARCHAR(200)   NOT NULL,
        PipelineRunID       NVARCHAR(200)   NULL,   -- ADF RunId
        BatchStatus         NVARCHAR(50)    NOT NULL DEFAULT 'Running',  -- Running | Completed | Failed
        StartTime           DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
        EndTime             DATETIME2       NULL,
        CreatedBy           NVARCHAR(100)   NOT NULL DEFAULT SYSTEM_USER,

        CONSTRAINT PK_Batch PRIMARY KEY (BatchID)
    );
END
GO

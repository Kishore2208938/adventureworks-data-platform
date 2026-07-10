-- ============================================================
-- Script  : usp_StartPipeline.sql
-- Schema  : framework
-- Purpose : Creates a new ExecutionLog entry when a pipeline starts
--           Returns the new ExecutionLogID to the caller (ADF / Databricks)
-- ============================================================

USE AW_ETL_Framework;
GO

CREATE OR ALTER PROCEDURE framework.usp_StartPipeline
    @PipelineName   NVARCHAR(200),
    @BatchID        INT             = NULL,
    @SourceTableID  INT             = NULL,
    @ExecutionLogID INT             OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO logging.ExecutionLog
        (BatchID, PipelineName, SourceTableID, ExecutionStatus, StartTime)
    VALUES
        (@BatchID, @PipelineName, @SourceTableID, 'Running', SYSUTCDATETIME());

    SET @ExecutionLogID = SCOPE_IDENTITY();
END
GO

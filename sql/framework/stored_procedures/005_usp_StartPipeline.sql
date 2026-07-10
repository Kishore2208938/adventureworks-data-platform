USE AW_ETL_Framework;
GO

CREATE OR ALTER PROCEDURE logging.usp_StartPipeline
(
      @PipelineName NVARCHAR(200)
    , @PipelineRunID NVARCHAR(100)
    , @BatchID INT = NULL
    , @ExecutedBy NVARCHAR(100) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO logging.ExecutionLog
    (
        BatchID,
        PipelineName,
        PipelineRunID,
        ExecutionStatus,
        StartTime,
        ExecutedBy
    )
    VALUES
    (
        @BatchID,
        @PipelineName,
        @PipelineRunID,
        'RUNNING',
        SYSUTCDATETIME(),
        @ExecutedBy
    );

    SELECT SCOPE_IDENTITY() AS ExecutionLogID;
END
GO
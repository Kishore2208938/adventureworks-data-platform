USE AW_ETL_Framework;
GO

CREATE OR ALTER PROCEDURE logging.usp_EndPipeline
(
      @ExecutionLogID BIGINT
    , @ExecutionStatus NVARCHAR(20)
    , @ErrorMessage NVARCHAR(MAX)=NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE logging.ExecutionLog
    SET
        ExecutionStatus = @ExecutionStatus,
        EndTime = SYSUTCDATETIME(),
        DurationInSeconds =
            DATEDIFF
            (
                SECOND,
                StartTime,
                SYSUTCDATETIME()
            ),
        ErrorMessage = @ErrorMessage
    WHERE ExecutionLogID=@ExecutionLogID;

END
GO
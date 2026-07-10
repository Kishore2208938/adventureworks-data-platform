USE AW_ETL_Framework;
GO

CREATE OR ALTER PROCEDURE logging.usp_LogError
(
      @ExecutionLogID BIGINT
    , @ErrorMessage NVARCHAR(MAX)
)
AS
BEGIN

SET NOCOUNT ON;

UPDATE logging.ExecutionLog
SET
    ExecutionStatus='FAILED',
    ErrorMessage=@ErrorMessage,
    EndTime=SYSUTCDATETIME(),
    DurationInSeconds=
        DATEDIFF
        (
            SECOND,
            StartTime,
            SYSUTCDATETIME()
        )
WHERE ExecutionLogID=@ExecutionLogID;

END
GO
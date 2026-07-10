USE AW_ETL_Framework;
GO

CREATE OR ALTER PROCEDURE logging.usp_LogExecutionDetail
(
      @ExecutionLogID BIGINT
    , @SourceTableID INT
    , @PipelineName NVARCHAR(200)
    , @NotebookName NVARCHAR(200)=NULL
    , @LayerName NVARCHAR(20)
    , @ExecutionStatus NVARCHAR(20)
    , @RowsRead BIGINT=NULL
    , @RowsInserted BIGINT=NULL
    , @RowsUpdated BIGINT=NULL
    , @RowsDeleted BIGINT=NULL
    , @RowsRejected BIGINT=NULL
    , @ErrorMessage NVARCHAR(MAX)=NULL
)
AS
BEGIN

SET NOCOUNT ON;

INSERT INTO logging.ExecutionDetail
(
    ExecutionLogID,
    SourceTableID,
    PipelineName,
    NotebookName,
    LayerName,
    ExecutionStatus,
    RowsRead,
    RowsInserted,
    RowsUpdated,
    RowsDeleted,
    RowsRejected,
    StartTime,
    EndTime,
    DurationInSeconds,
    ErrorMessage
)
VALUES
(
    @ExecutionLogID,
    @SourceTableID,
    @PipelineName,
    @NotebookName,
    @LayerName,
    @ExecutionStatus,
    @RowsRead,
    @RowsInserted,
    @RowsUpdated,
    @RowsDeleted,
    @RowsRejected,
    SYSUTCDATETIME(),
    SYSUTCDATETIME(),
    0,
    @ErrorMessage
);

END
GO
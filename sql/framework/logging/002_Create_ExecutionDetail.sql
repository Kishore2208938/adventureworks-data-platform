/*==============================================================================
 Script Name : 002_Create_ExecutionDetail.sql
 Purpose     : Stores execution details for each source table processed.
==============================================================================*/

USE AW_ETL_Framework;
GO

IF OBJECT_ID('logging.ExecutionDetail','U') IS NULL
BEGIN

CREATE TABLE logging.ExecutionDetail
(
    ExecutionDetailID        BIGINT IDENTITY(1,1) NOT NULL,

    ExecutionLogID           BIGINT NOT NULL,

    SourceTableID            INT NOT NULL,

    PipelineName             NVARCHAR(200) NOT NULL,

    NotebookName             NVARCHAR(200) NULL,

    LayerName                NVARCHAR(20) NOT NULL,
    -- RAW
    -- BRONZE
    -- SILVER
    -- GOLD

    ExecutionStatus          NVARCHAR(20) NOT NULL
        CONSTRAINT DF_ExecutionDetail_Status
        DEFAULT('RUNNING'),
    -- RUNNING
    -- SUCCESS
    -- FAILED

    RowsRead                 BIGINT NULL,

    RowsInserted             BIGINT NULL,

    RowsUpdated              BIGINT NULL,

    RowsDeleted              BIGINT NULL,

    RowsRejected             BIGINT NULL,

    StartTime                DATETIME2(0) NOT NULL
        CONSTRAINT DF_ExecutionDetail_StartTime
        DEFAULT SYSUTCDATETIME(),

    EndTime                  DATETIME2(0) NULL,

    DurationInSeconds        INT NULL,

    ErrorMessage             NVARCHAR(MAX) NULL,

    CreatedDate              DATETIME2(0) NOT NULL
        CONSTRAINT DF_ExecutionDetail_CreatedDate
        DEFAULT SYSUTCDATETIME(),

    CONSTRAINT PK_ExecutionDetail
        PRIMARY KEY CLUSTERED (ExecutionDetailID),

    CONSTRAINT FK_ExecutionDetail_ExecutionLog
        FOREIGN KEY (ExecutionLogID)
        REFERENCES logging.ExecutionLog(ExecutionLogID),

    CONSTRAINT FK_ExecutionDetail_SourceTable
        FOREIGN KEY (SourceTableID)
        REFERENCES metadata.SourceTable(SourceTableID)
);

END
GO

CREATE INDEX IX_ExecutionDetail_ExecutionLog
ON logging.ExecutionDetail(ExecutionLogID);
GO

CREATE INDEX IX_ExecutionDetail_SourceTable
ON logging.ExecutionDetail(SourceTableID);
GO

CREATE INDEX IX_ExecutionDetail_Status
ON logging.ExecutionDetail(ExecutionStatus);
GO
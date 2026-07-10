/*==============================================================================
 Script Name : 001_Create_ExecutionLog.sql
 Purpose     : Stores execution summary for every pipeline execution.
==============================================================================*/

USE AW_ETL_Framework;
GO

IF OBJECT_ID('logging.ExecutionLog','U') IS NULL
BEGIN

CREATE TABLE logging.ExecutionLog
(
    ExecutionLogID          BIGINT IDENTITY(1,1) NOT NULL,

    BatchID                 INT NULL,

    PipelineName            NVARCHAR(200) NOT NULL,

    PipelineRunID           NVARCHAR(100) NULL,

    SourceTableID           INT NULL,

    ExecutionStatus         NVARCHAR(20) NOT NULL
        CONSTRAINT DF_ExecutionLog_Status
        DEFAULT('RUNNING'),
    -- RUNNING
    -- SUCCESS
    -- FAILED

    RowsExtracted           BIGINT NULL,

    RowsLoaded              BIGINT NULL,

    RowsRejected            BIGINT NULL,

    StartTime               DATETIME2(0) NOT NULL
        CONSTRAINT DF_ExecutionLog_StartTime
        DEFAULT SYSUTCDATETIME(),

    EndTime                 DATETIME2(0) NULL,

    DurationInSeconds       INT NULL,

    ErrorMessage            NVARCHAR(MAX) NULL,

    ExecutedBy              NVARCHAR(100) NULL,

    CreatedDate             DATETIME2(0) NOT NULL
        CONSTRAINT DF_ExecutionLog_CreatedDate
        DEFAULT SYSUTCDATETIME(),

    CONSTRAINT PK_ExecutionLog
        PRIMARY KEY CLUSTERED (ExecutionLogID),

    CONSTRAINT FK_ExecutionLog_SourceTable
        FOREIGN KEY(SourceTableID)
        REFERENCES metadata.SourceTable(SourceTableID)
);

END
GO

CREATE INDEX IX_ExecutionLog_Status
ON logging.ExecutionLog(ExecutionStatus);
GO

CREATE INDEX IX_ExecutionLog_StartTime
ON logging.ExecutionLog(StartTime);
GO

CREATE INDEX IX_ExecutionLog_SourceTable
ON logging.ExecutionLog(SourceTableID);
GO
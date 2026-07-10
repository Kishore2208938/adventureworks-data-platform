/*==============================================================================
 Script Name : 001_Create_Watermark.sql
 Author      : Kishore Kumar
 Purpose     : Stores watermark values for incremental data loads.
==============================================================================*/

USE AW_ETL_Framework;
GO

IF OBJECT_ID('control.Watermark','U') IS NULL
BEGIN

CREATE TABLE control.Watermark
(
    WatermarkID                INT IDENTITY(1,1) NOT NULL,

    SourceTableID              INT NOT NULL,

    WatermarkColumn            NVARCHAR(100) NOT NULL,

    LastSuccessfulValue        NVARCHAR(200) NULL,

    CurrentRunValue            NVARCHAR(200) NULL,

    LastRunStartTime           DATETIME2(0) NULL,

    LastRunEndTime             DATETIME2(0) NULL,

    LastRunStatus              NVARCHAR(20) NULL,
    -- SUCCESS
    -- FAILED
    -- RUNNING

    IsActive                   BIT NOT NULL
        CONSTRAINT DF_Watermark_IsActive
        DEFAULT(1),

    CreatedDate                DATETIME2(0) NOT NULL
        CONSTRAINT DF_Watermark_CreatedDate
        DEFAULT SYSUTCDATETIME(),

    CreatedBy                  NVARCHAR(100) NOT NULL
        CONSTRAINT DF_Watermark_CreatedBy
        DEFAULT SUSER_SNAME(),

    ModifiedDate               DATETIME2(0) NULL,

    ModifiedBy                 NVARCHAR(100) NULL,

    CONSTRAINT PK_Watermark
        PRIMARY KEY CLUSTERED(WatermarkID),

    CONSTRAINT FK_Watermark_SourceTable
        FOREIGN KEY(SourceTableID)
        REFERENCES metadata.SourceTable(SourceTableID),

    CONSTRAINT UQ_Watermark
        UNIQUE(SourceTableID)
);

END
GO

CREATE INDEX IX_Watermark_SourceTable
ON control.Watermark(SourceTableID);
GO
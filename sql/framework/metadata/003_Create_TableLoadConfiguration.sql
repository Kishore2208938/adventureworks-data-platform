/*==============================================================================
 Script Name : 003_Create_TableConfiguration.sql
 Purpose     : Stores runtime configuration for source table processing
==============================================================================*/

USE AW_ETL_Framework;
GO

IF OBJECT_ID('metadata.TableLoadConfiguration','U') IS NULL
BEGIN

CREATE TABLE metadata.TableLoadConfiguration
(
    TableConfigurationID      INT IDENTITY(1,1) NOT NULL,

    SourceTableID             INT NOT NULL,

    TargetContainer           NVARCHAR(100) NOT NULL,

    TargetFolder              NVARCHAR(200) NOT NULL,

    FileFormat                NVARCHAR(20) NOT NULL
        CONSTRAINT DF_TableConfiguration_FileFormat
        DEFAULT('PARQUET'),

    LoadType                  NVARCHAR(20) NOT NULL,
    -- FULL
    -- INCREMENTAL

    WatermarkColumn           NVARCHAR(100) NULL,

    PrimaryKeyColumns         NVARCHAR(500) NULL,
    -- CustomerID
    -- SalesOrderID,SalesOrderDetailID

    PartitionColumn           NVARCHAR(100) NULL,

    MergeStrategy             NVARCHAR(20) NULL,
    -- APPEND
    -- MERGE
    -- OVERWRITE

    CompressionType           NVARCHAR(20) NOT NULL
        CONSTRAINT DF_TableConfiguration_Compression
        DEFAULT('SNAPPY'),

    BronzeTableName           NVARCHAR(200) NULL,

    SilverTableName           NVARCHAR(200) NULL,

    GoldTableName             NVARCHAR(200) NULL,

    NotebookName              NVARCHAR(200) NULL,

    PipelineName              NVARCHAR(200) NULL,

    RetryCount                INT NOT NULL
        CONSTRAINT DF_TableConfiguration_RetryCount
        DEFAULT(3),

    RetryIntervalSeconds      INT NOT NULL
        CONSTRAINT DF_TableConfiguration_RetryInterval
        DEFAULT(60),

    TimeoutMinutes            INT NOT NULL
        CONSTRAINT DF_TableConfiguration_Timeout
        DEFAULT(120),

    LoadSequence              INT NOT NULL
        CONSTRAINT DF_TableConfiguration_LoadSequence
        DEFAULT(1),

    IsActive                  BIT NOT NULL
        CONSTRAINT DF_TableConfiguration_IsActive
        DEFAULT(1),

    CreatedDate               DATETIME2(0) NOT NULL
        CONSTRAINT DF_TableConfiguration_CreatedDate
        DEFAULT SYSUTCDATETIME(),

    CreatedBy                 NVARCHAR(100) NOT NULL
        CONSTRAINT DF_TableConfiguration_CreatedBy
        DEFAULT SUSER_SNAME(),

    ModifiedDate              DATETIME2(0) NULL,

    ModifiedBy                NVARCHAR(100) NULL,

    CONSTRAINT PK_TableConfiguration
        PRIMARY KEY CLUSTERED(TableConfigurationID),

    CONSTRAINT FK_TableConfiguration_SourceTable
        FOREIGN KEY(SourceTableID)
        REFERENCES metadata.SourceTable(SourceTableID)
);

END
GO

CREATE INDEX IX_TableConfiguration_IsActive
ON metadata.TableConfiguration(IsActive);

CREATE INDEX IX_TableConfiguration_LoadSequence
ON metadata.TableConfiguration(LoadSequence);
GO
/*==============================================================================
 Script Name : 002_Create_SourceTable.sql
 Schema      : metadata
 Purpose     : Stores all source tables that participate in the ETL Framework.
===============================================================================*/

USE AW_ETL_Framework;
GO

IF OBJECT_ID('metadata.SourceTable', 'U') IS NULL
BEGIN
    CREATE TABLE metadata.SourceTable
    (
        SourceTableID INT IDENTITY(1,1) NOT NULL,

        SourceSystemID INT NOT NULL,

        SourceSchema NVARCHAR(100) NOT NULL,

        SourceTableName NVARCHAR(200) NOT NULL,

        TargetContainer NVARCHAR(100) NOT NULL,

        TargetFolder NVARCHAR(200) NOT NULL,

        FileFormat NVARCHAR(20) NOT NULL
            CONSTRAINT DF_SourceTable_FileFormat DEFAULT('PARQUET'),

        LoadType NVARCHAR(20) NOT NULL,
        -- FULL | INCREMENTAL

        WatermarkColumn NVARCHAR(100) NULL,

        IsActive BIT NOT NULL
            CONSTRAINT DF_SourceTable_IsActive DEFAULT(1),

        LoadSequence INT NOT NULL
            CONSTRAINT DF_SourceTable_LoadSequence DEFAULT(1),

        Description NVARCHAR(500) NULL,

        CreatedDate DATETIME2(0) NOT NULL
            CONSTRAINT DF_SourceTable_CreatedDate DEFAULT SYSUTCDATETIME(),

        CreatedBy NVARCHAR(100) NOT NULL
            CONSTRAINT DF_SourceTable_CreatedBy DEFAULT SUSER_SNAME(),

        ModifiedDate DATETIME2(0) NULL,

        ModifiedBy NVARCHAR(100) NULL,

        CONSTRAINT PK_SourceTable
            PRIMARY KEY CLUSTERED (SourceTableID),

        CONSTRAINT FK_SourceTable_SourceSystem
            FOREIGN KEY (SourceSystemID)
            REFERENCES metadata.SourceSystem(SourceSystemID),

        CONSTRAINT UQ_SourceTable
            UNIQUE (SourceSystemID, SourceSchema, SourceTableName)
    );
END
GO

CREATE INDEX IX_SourceTable_IsActive
ON metadata.SourceTable(IsActive);
GO

CREATE INDEX IX_SourceTable_LoadSequence
ON metadata.SourceTable(LoadSequence);
GO

CREATE INDEX IX_SourceTable_SourceSystem
ON metadata.SourceTable(SourceSystemID);
GO
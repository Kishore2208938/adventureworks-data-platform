/*==============================================================================
 Script Name : 001_Create_SourceSystem.sql
 Schema      : metadata
 Purpose     : Stores all source systems registered in the ETL Framework.
===============================================================================*/

USE AW_ETL_Framework;
GO

IF OBJECT_ID('metadata.SourceSystem', 'U') IS NULL
BEGIN
    CREATE TABLE metadata.SourceSystem
    (
        SourceSystemID INT IDENTITY(1,1) NOT NULL,

        SourceSystemCode NVARCHAR(20) NOT NULL,

        SourceSystemName NVARCHAR(100) NOT NULL,

        SourceSystemType NVARCHAR(50) NOT NULL,
        -- SQL Server | Oracle | REST API | SAP | Salesforce

        ConnectionReference NVARCHAR(100) NULL,
        -- Example:
        -- ls_aw_sqlserver
        -- ls_sap_prd
        -- ls_rest_customer

        Description NVARCHAR(500) NULL,

        IsActive BIT NOT NULL
            CONSTRAINT DF_SourceSystem_IsActive DEFAULT(1),

        CreatedDate DATETIME2(0) NOT NULL
            CONSTRAINT DF_SourceSystem_CreatedDate DEFAULT SYSUTCDATETIME(),

        CreatedBy NVARCHAR(100) NOT NULL
            CONSTRAINT DF_SourceSystem_CreatedBy DEFAULT SUSER_SNAME(),

        ModifiedDate DATETIME2(0) NULL,

        ModifiedBy NVARCHAR(100) NULL,

        CONSTRAINT PK_SourceSystem
            PRIMARY KEY CLUSTERED (SourceSystemID),

        CONSTRAINT UQ_SourceSystem_Code
            UNIQUE (SourceSystemCode),

        CONSTRAINT UQ_SourceSystem_Name
            UNIQUE (SourceSystemName)
    );
END
GO
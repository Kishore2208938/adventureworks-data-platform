-- ============================================================
-- Script  : 001_Create_DataQualityAudit.sql
-- Schema  : audit
-- Purpose : Records data quality check results per load execution
-- ============================================================

USE AW_ETL_Framework;
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'audit' AND t.name = 'DataQualityAudit'
)
BEGIN
    CREATE TABLE audit.DataQualityAudit
    (
        AuditID             INT             NOT NULL IDENTITY(1,1),
        ExecutionLogID      INT             NOT NULL,
        SourceTableID       INT             NULL,
        CheckName           NVARCHAR(200)   NOT NULL,   -- e.g. 'NullCheck', 'DuplicateCheck'
        CheckStatus         NVARCHAR(50)    NOT NULL,   -- 'Passed' | 'Failed' | 'Warning'
        ExpectedValue       NVARCHAR(255)   NULL,
        ActualValue         NVARCHAR(255)   NULL,
        CheckMessage        NVARCHAR(MAX)   NULL,
        CheckDate           DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_DataQualityAudit PRIMARY KEY (AuditID)
    );
END
GO

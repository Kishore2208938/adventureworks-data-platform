-- ============================================================
-- Script  : 001_Create_ApplicationConfig.sql
-- Schema  : config
-- Purpose : Key-value application configuration store
-- ============================================================

USE AW_ETL_Framework;
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'config' AND t.name = 'ApplicationConfig'
)
BEGIN
    CREATE TABLE config.ApplicationConfig
    (
        ConfigID            INT             NOT NULL IDENTITY(1,1),
        ConfigKey           NVARCHAR(200)   NOT NULL,
        ConfigValue         NVARCHAR(MAX)   NULL,
        ConfigDescription   NVARCHAR(500)   NULL,
        Environment         NVARCHAR(50)    NOT NULL DEFAULT 'All',  -- 'Dev' | 'Test' | 'Prod' | 'All'
        IsActive            BIT             NOT NULL DEFAULT 1,
        CreatedDate         DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),
        ModifiedDate        DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME(),

        CONSTRAINT PK_ApplicationConfig PRIMARY KEY (ConfigID),
        CONSTRAINT UQ_ApplicationConfig_Key UNIQUE (ConfigKey, Environment)
    );
END
GO

-- ============================================================
-- Script  : 002_Create_Schemas.sql
-- Purpose : Create schemas for the AdventureWorks Data Platform
-- Author  :
-- Date    :
-- ============================================================

USE AdventureWorksDW;
GO

-- Framework / Control schemas
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'framework')
    EXEC('CREATE SCHEMA framework');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'metadata')
    EXEC('CREATE SCHEMA metadata');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'control')
    EXEC('CREATE SCHEMA control');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'audit')
    EXEC('CREATE SCHEMA audit');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'logging')
    EXEC('CREATE SCHEMA logging');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'config')
    EXEC('CREATE SCHEMA config');
GO

-- Data Warehouse schemas
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'dim')
    EXEC('CREATE SCHEMA dim');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'fact')
    EXEC('CREATE SCHEMA fact');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'stg')
    EXEC('CREATE SCHEMA stg');
GO

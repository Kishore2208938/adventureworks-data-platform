-- ============================================================
-- Script  : 001_Create_Database.sql
-- Purpose : Create the AdventureWorks Data Platform database
-- Author  :
-- Date    :
-- ============================================================

-- Create Database
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'AW_ETL_Framework')
BEGIN
    CREATE DATABASE AW_ETL_Framework;
END
GO

/*==============================================================================
 Script Name : 001_Seed_SourceSystem.sql
 Purpose     : Seed Source Systems
==============================================================================*/

USE AW_ETL_Framework;
GO

SET IDENTITY_INSERT metadata.SourceSystem ON;
GO

INSERT INTO metadata.SourceSystem
(
    SourceSystemID,
    SourceSystemCode,
    SourceSystemName,
    SourceSystemType,
    ConnectionReference,
    Description,
    IsActive
)
VALUES
(
    1,
    'AW',
    'AdventureWorks',
    'SQL Server',
    'LS_AW_SQLSERVER',
    'AdventureWorks Sample OLTP Database',
    1
),
(
    2,
    'ERP',
    'SAP ERP',
    'SAP',
    'LS_SAP_PRD',
    'SAP ERP Production System',
    1
),
(
    3,
    'CRM',
    'Salesforce',
    'CRM',
    'LS_SFDC_PRD',
    'Salesforce CRM',
    1
),
(
    4,
    'REST',
    'REST API',
    'REST API',
    'LS_REST_API',
    'External REST API Source',
    1
),
(
    5,
    'ORCL',
    'Oracle ERP',
    'Oracle',
    'LS_ORACLE_PRD',
    'Oracle ERP Database',
    0
);

GO

SET IDENTITY_INSERT metadata.SourceSystem OFF;
GO

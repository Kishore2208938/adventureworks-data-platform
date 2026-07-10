USE AW_ETL_Framework;
GO

INSERT INTO metadata.TableLoadConfiguration
(
    SourceTableID,
    TargetContainer,
    TargetFolder,
    LoadType,
    WatermarkColumn,
    PrimaryKeyColumns,
    MergeStrategy,
    BronzeTableName,
    SilverTableName,
    GoldTableName,
    NotebookName,
    PipelineName,
    LoadSequence
)
SELECT
    SourceTableID,
    'raw',
    LOWER(SourceTableName),
    LoadType,
    WatermarkColumn,
    CASE
        WHEN SourceTableName='Customer' THEN 'CustomerID'
        WHEN SourceTableName='Product' THEN 'ProductID'
        WHEN SourceTableName='SalesOrderHeader' THEN 'SalesOrderID'
        WHEN SourceTableName='SalesOrderDetail' THEN 'SalesOrderID,SalesOrderDetailID'
        WHEN SourceTableName='Employee' THEN 'BusinessEntityID'
        WHEN SourceTableName='SalesPerson' THEN 'BusinessEntityID'
        WHEN SourceTableName='Address' THEN 'AddressID'
    END,
    CASE
        WHEN LoadType='FULL' THEN 'OVERWRITE'
        ELSE 'MERGE'
    END,
    'br_' + LOWER(SourceTableName),
    'sl_' + LOWER(SourceTableName),
    'gd_' + LOWER(SourceTableName),
    'nb_generic_loader',
    'pl_generic_ingestion',
    LoadSequence
FROM metadata.SourceTable;
GO
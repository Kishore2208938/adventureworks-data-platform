USE AW_ETL_Framework;
GO

DECLARE @SourceSystemID INT;

SELECT @SourceSystemID = SourceSystemID
FROM metadata.SourceSystem
WHERE SourceSystemCode='AW';

INSERT INTO metadata.SourceTable
(
    SourceSystemID,
    SourceSchema,
    SourceTableName,
    TargetContainer,
    TargetFolder,
    LoadType,
    WatermarkColumn,
    LoadSequence,
    Description
)
VALUES
(@SourceSystemID,'Sales','Customer','raw','customer','FULL',NULL,1,'Customer Master'),

(@SourceSystemID,'Production','Product','raw','product','FULL',NULL,2,'Product Master'),

(@SourceSystemID,'Sales','SalesOrderHeader','raw','salesorderheader','INCREMENTAL','ModifiedDate',3,'Sales Header'),

(@SourceSystemID,'Sales','SalesOrderDetail','raw','salesorderdetail','FULL',NULL,4,'Sales Detail'),

(@SourceSystemID,'Sales','SalesPerson','raw','salesperson','FULL',NULL,5,'Sales Person'),

(@SourceSystemID,'HumanResources','Employee','raw','employee','FULL',NULL,6,'Employee Master'),

(@SourceSystemID,'Person','Address','raw','address','FULL',NULL,7,'Address Master');
GO
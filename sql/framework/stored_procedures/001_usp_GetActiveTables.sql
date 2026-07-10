USE AW_ETL_Framework;
GO

CREATE OR ALTER PROCEDURE metadata.usp_GetTableConfiguration
(
    @SourceTableID INT
)
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        ST.SourceSchema,
        ST.SourceTableName,

        TLC.TargetContainer,
        TLC.TargetFolder,

        TLC.FileFormat,
        TLC.CompressionType,

        TLC.LoadType,
        TLC.WatermarkColumn,

        TLC.PrimaryKeyColumns,
        TLC.MergeStrategy,
        TLC.PartitionColumn,

        TLC.BronzeTableName,
        TLC.SilverTableName,
        TLC.GoldTableName,

        TLC.NotebookName,
        TLC.PipelineName,

        TLC.RetryCount,
        TLC.RetryIntervalSeconds,
        TLC.TimeoutMinutes

    FROM metadata.SourceTable ST
    INNER JOIN metadata.TableLoadConfiguration TLC
        ON ST.SourceTableID = TLC.SourceTableID

    WHERE
        ST.SourceTableID = @SourceTableID;
END
GO
USE AW_ETL_Framework;
GO

CREATE OR ALTER PROCEDURE control.usp_GetWatermark
(
    @SourceTableID INT
)
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        WatermarkID,
        WatermarkColumn,
        LastSuccessfulValue,
        CurrentRunValue,
        LastRunStatus
    FROM control.Watermark
    WHERE
        SourceTableID = @SourceTableID;
END
GO
USE AW_ETL_Framework;
GO

CREATE OR ALTER PROCEDURE control.usp_UpdateWatermark
(
      @SourceTableID INT
    , @WatermarkValue NVARCHAR(200)
    , @RunStatus NVARCHAR(20)
)
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE control.Watermark
    SET
        LastSuccessfulValue = CASE
                                WHEN @RunStatus='SUCCESS'
                                THEN @WatermarkValue
                                ELSE LastSuccessfulValue
                              END,

        CurrentRunValue = @WatermarkValue,

        LastRunStatus = @RunStatus,

        LastRunEndTime = SYSUTCDATETIME(),

        ModifiedDate = SYSUTCDATETIME(),

        ModifiedBy = SUSER_SNAME()

    WHERE SourceTableID = @SourceTableID;
END
GO
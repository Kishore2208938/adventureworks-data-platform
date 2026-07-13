# Databricks notebook source
# =============================================================================
# 02_bronze_ingestion.py
# Purpose : Generic, metadata-driven Raw(CSV, flat files) -> Bronze(Delta).
#           Each table's file is located by exact filename from metadata
#           (landing_file_name), not by subfolder. Mirrors ADF's
#           ACT_Get_Active_Tables -> ACT_ForEach -> PL_Copy_Table.
# =============================================================================

# COMMAND ----------

dbutils.widgets.text("catalog_name", "de_lakehouse")
dbutils.widgets.text("pipeline_name", "PL_Bronze_Load")
catalog = dbutils.widgets.get("catalog_name")
pipeline_name = dbutils.widgets.get("pipeline_name")
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

import datetime, uuid
from pyspark.sql import functions as F

batch_id = str(uuid.uuid4())
current_user = spark.sql("SELECT current_user() AS u").collect()[0]["u"]

# COMMAND ----------

# ---------- logging helpers ---------------------------------------------------
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType

def log_batch_start():
    start = datetime.datetime.utcnow()
    row = [(batch_id, pipeline_name, None, "BRONZE", "RUNNING", None, None, None, start, None, None, None, current_user, start)]
    schema = StructType([
        StructField("batch_id", StringType(), False),
        StructField("pipeline_name", StringType(), False),
        StructField("pipeline_run_id", StringType(), True),
        StructField("layer_name", StringType(), False),
        StructField("execution_status", StringType(), False),
        StructField("rows_extracted", LongType(), True),
        StructField("rows_loaded", LongType(), True),
        StructField("rows_rejected", LongType(), True),
        StructField("start_time", TimestampType(), False),
        StructField("end_time", TimestampType(), True),
        StructField("duration_in_seconds", IntegerType(), True),
        StructField("error_message", StringType(), True),
        StructField("executed_by", StringType(), False),
        StructField("created_date", TimestampType(), False)
    ])
    df = spark.createDataFrame(row, schema)
    df.write.mode("append").saveAsTable("logging.execution_log")
    exec_log_id = spark.sql(f"""
        SELECT execution_log_id FROM logging.execution_log
        WHERE batch_id = '{batch_id}' ORDER BY execution_log_id DESC LIMIT 1
    """).collect()[0][0]
    return exec_log_id, start

def log_batch_end(exec_log_id, status, rows_loaded, error_message=None):
    end = datetime.datetime.utcnow()
    spark.sql(f"""
        UPDATE logging.execution_log
        SET execution_status = '{status}', end_time = TIMESTAMP'{end}',
            rows_loaded = {rows_loaded if rows_loaded is not None else 'NULL'},
            error_message = {("'" + error_message.replace("'", "''") + "'") if error_message else 'NULL'}
        WHERE execution_log_id = {exec_log_id}
    """)

def log_table_detail(exec_log_id, source_table_id, notebook_name, status, rows_read, rows_inserted,
                      rows_updated, start, error_message=None):
    end = datetime.datetime.utcnow()
    duration = int((end - start).total_seconds())
    
    # Define schema explicitly
    schema = StructType([
        StructField("execution_log_id", LongType(), False),
        StructField("source_table_id", LongType(), False),
        StructField("pipeline_name", StringType(), False),
        StructField("notebook_name", StringType(), False),
        StructField("layer_name", StringType(), False),
        StructField("execution_status", StringType(), False),
        StructField("rows_read", LongType(), True),
        StructField("rows_inserted", LongType(), True),
        StructField("rows_updated", LongType(), True),
        StructField("rows_deleted", LongType(), True),
        StructField("rows_rejected", LongType(), True),
        StructField("start_time", TimestampType(), False),
        StructField("end_time", TimestampType(), False),
        StructField("duration_in_seconds", IntegerType(), False),
        StructField("error_message", StringType(), True),
        StructField("created_date", TimestampType(), False)
    ])
    
    row = [(exec_log_id, source_table_id, pipeline_name, notebook_name, "BRONZE", status,
            rows_read, rows_inserted, rows_updated, 0, 0, start, end, duration, error_message, end)]
    df = spark.createDataFrame(row, schema)
    df.write.mode("append").saveAsTable("logging.execution_detail")

# COMMAND ----------

# ---------- watermark helpers --------------------------------------------------
def get_watermark(source_table_id):
    r = spark.sql(f"""
        SELECT last_successful_value FROM control.watermark
        WHERE source_table_id = {source_table_id} AND is_active = true
    """).collect()
    return r[0][0] if r else None

def update_watermark(source_table_id, new_value, status):
    now = datetime.datetime.utcnow()
    spark.sql(f"""
        UPDATE control.watermark
        SET last_successful_value = '{new_value}',
            last_run_end_time = TIMESTAMP'{now}',
            last_run_status = '{status}',
            modified_date = TIMESTAMP'{now}',
            modified_by = '{current_user}'
        WHERE source_table_id = {source_table_id}
    """)

# COMMAND ----------

# ---------- core generic loader (CSV, exact filename) --------------------------
import re

def sanitize_column_name(col_name):
    """Replace invalid Delta column name characters with underscores."""
    sanitized = re.sub(r'[ ,;{}()\n\t=.]+', '_', col_name)
    sanitized = re.sub(r'_+', '_', sanitized).strip('_')
    if not sanitized or sanitized[0].isdigit():
        sanitized = 'col_' + sanitized
    return sanitized

CSV_SCHEMAS = {
    "SalesOrderHeader.csv": "SalesOrderID INT, RevisionNumber INT, OrderDate TIMESTAMP, DueDate TIMESTAMP, ShipDate TIMESTAMP, Status INT, OnlineOrderFlag BOOLEAN, SalesOrderNumber STRING, PurchaseOrderNumber STRING, AccountNumber STRING, CustomerID INT, SalesPersonID INT, TerritoryID INT, BillToAddressID INT, ShipToAddressID INT, ShipMethodID INT, CreditCardApprovalCode STRING, SubTotal DECIMAL(19,4), TaxAmt DECIMAL(19,4), Freight DECIMAL(19,4), TotalDue DECIMAL(19,4), Comment STRING, rowguid STRING, ModifiedDate TIMESTAMP",
    "SalesOrderDetail.csv": "SalesOrderID INT, SalesOrderDetailID INT, CarrierTrackingNumber STRING, OrderQty INT, ProductID INT, SpecialOfferID INT, UnitPrice DECIMAL(19,4), UnitPriceDiscount DECIMAL(19,4), LineTotal DECIMAL(19,4), rowguid STRING, ModifiedDate TIMESTAMP",
    "Customer.csv": "CustomerID INT, PersonID INT, StoreID INT, TerritoryID INT, AccountNumber STRING, rowguid STRING, ModifiedDate TIMESTAMP",
    "Product.csv": "ProductID INT, Name STRING, ProductNumber STRING, MakeFlag BOOLEAN, FinishedGoodsFlag BOOLEAN, Color STRING, SafetyStockLevel INT, ReorderPoint INT, StandardCost DECIMAL(19,4), ListPrice DECIMAL(19,4), Size STRING, SizeUnitMeasureCode STRING, WeightUnitMeasureCode STRING, Weight DECIMAL(8,2), DaysToManufacture INT, ProductLine STRING, Class STRING, Style STRING, ProductSubcategoryID INT, ProductModelID INT, SellStartDate TIMESTAMP, SellEndDate TIMESTAMP, DiscontinuedDate TIMESTAMP, rowguid STRING, ModifiedDate TIMESTAMP",
    "address.csv": "AddressID INT, AddressLine1 STRING, AddressLine2 STRING, City STRING, StateProvinceID INT, PostalCode STRING, SpatialLocation STRING, rowguid STRING, ModifiedDate TIMESTAMP",
    "person.csv": "BusinessEntityID INT, PersonType STRING, NameStyle BOOLEAN, Title STRING, FirstName STRING, MiddleName STRING, LastName STRING, Suffix STRING, EmailPromotion INT, AdditionalContactInfo STRING, Demographics STRING, rowguid STRING, ModifiedDate TIMESTAMP",
    "salesperson.csv": "BusinessEntityID INT, TerritoryID INT, SalesQuota DECIMAL(19,4), Bonus DECIMAL(19,4), CommissionPct DECIMAL(10,4), SalesYTD DECIMAL(19,4), SalesLastYear DECIMAL(19,4), rowguid STRING, ModifiedDate TIMESTAMP",
}

def load_table(cfg):
    start = datetime.datetime.utcnow()
    file_path = f"/Volumes/{catalog}/raw/landing/{cfg.landing_file_name}"
    bronze_table = f"bronze.{cfg.bronze_table_name}"

    try:
        # Use explicit schema if available, otherwise infer
        schema = CSV_SCHEMAS.get(cfg.landing_file_name)
        print(f"Loading {cfg.source_table_name}: file={cfg.landing_file_name}, schema_found={schema is not None}")
        
        reader = spark.read.format("csv").option("header", "true").option("multiLine", "true").option("escape", '"')
        
        if schema:
            print(f"  Using explicit schema")
            df = reader.schema(schema).load(file_path)
        else:
            print(f"  WARNING: No schema found, reading as strings")
            # Fallback: read as strings and sanitize column names
            df = reader.option("inferSchema", "false").load(file_path)
            for col_name in df.columns:
                sanitized_name = sanitize_column_name(col_name)
                if sanitized_name != col_name:
                    df = df.withColumnRenamed(col_name, sanitized_name)

        print(f"  Checking incremental load...")
        if cfg.load_type == "INCREMENTAL" and cfg.watermark_column:
            wm_value = get_watermark(cfg.source_table_id)
            if wm_value:
                df = df.filter(F.col(cfg.watermark_column) > F.lit(wm_value))

        print(f"  Counting rows...")
        rows_read = df.count()
        print(f"  Rows read: {rows_read}")

        print(f"  Adding metadata columns...")
        df = df.withColumn("_ingested_at", F.current_timestamp()) \
               .withColumn("_source_file", F.lit(cfg.landing_file_name)) \
               .withColumn("_batch_id", F.lit(batch_id))

        print(f"  Checking if table exists...")
        table_exists = spark.catalog.tableExists(bronze_table)
        print(f"  Table exists: {table_exists}, merge_strategy: {cfg.merge_strategy}")

        if cfg.merge_strategy == "MERGE" and table_exists:
            print(f"  Executing MERGE operation...")
            pk_cols = [c.strip() for c in cfg.primary_key_columns.split(",")]
            print(f"    Primary keys: {pk_cols}")
            df.createOrReplaceTempView("_src_batch")
            merge_cond = " AND ".join([f"t.{c} = s.{c}" for c in pk_cols])
            print(f"    Merge condition: {merge_cond}")
            spark.sql(f"""
                MERGE INTO {bronze_table} t
                USING _src_batch s
                ON {merge_cond}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
            print(f"  MERGE completed")
        else:
            print(f"  Executing OVERWRITE operation...")
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
                .saveAsTable(bronze_table)
            print(f"  OVERWRITE completed")

        if cfg.load_type == "INCREMENTAL" and cfg.watermark_column and rows_read > 0:
            new_max = df.agg(F.max(cfg.watermark_column)).collect()[0][0]
            if new_max:
                update_watermark(cfg.source_table_id, str(new_max), "SUCCESS")

        log_table_detail(exec_log_id, cfg.source_table_id, "02_bronze_ingestion", "SUCCESS",
                          rows_read, rows_read, 0, start)
        return rows_read, "SUCCESS", None

    except Exception as e:
        log_table_detail(exec_log_id, cfg.source_table_id, "02_bronze_ingestion", "FAILED",
                          0, 0, 0, start, str(e))
        return 0, "FAILED", str(e)

# COMMAND ----------

active_tables = spark.sql("""
    SELECT tlc.*, st.source_table_name, st.source_schema
    FROM metadata.table_load_configuration tlc
    INNER JOIN metadata.source_table st ON tlc.source_table_id = st.source_table_id
    WHERE tlc.is_active = true AND st.is_active = true
    ORDER BY tlc.load_sequence
""").collect()

exec_log_id, batch_start = log_batch_start()

total_rows, failures = 0, []
for cfg in active_tables:
    rows, status, err = load_table(cfg)
    total_rows += rows
    if status == "FAILED":
        failures.append((cfg.source_table_name, err))

batch_status = "FAILED" if failures else "SUCCESS"
log_batch_end(exec_log_id, batch_status, total_rows,
              error_message="; ".join([f"{t}: {e}" for t, e in failures]) if failures else None)

# COMMAND ----------

print(f"Bronze batch {batch_id} finished: {batch_status}, {total_rows} rows across {len(active_tables)} tables.")
if failures:
    for t, e in failures:
        print(f"  FAILED: {t} -> {e}")
    dbutils.notebook.exit(f"BRONZE_FAILED: {failures}")
else:
    dbutils.notebook.exit("BRONZE_SUCCESS")
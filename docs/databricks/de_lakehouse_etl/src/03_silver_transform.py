# Databricks notebook source
# =============================================================================
# 03_silver_transform.py
# Purpose : Generic Bronze -> Silver conformance layer, driven entirely by
#           metadata.transformation_metadata (now includes Person). Dedupes
#           on primary key, drops PK nulls, merges/overwrites per contract.
# =============================================================================

# COMMAND ----------

dbutils.widgets.text("catalog_name", "de_lakehouse")
dbutils.widgets.text("pipeline_name", "PL_Silver_Load")
catalog = dbutils.widgets.get("catalog_name")
pipeline_name = dbutils.widgets.get("pipeline_name")
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

import datetime, uuid
from pyspark.sql import functions as F
from pyspark.sql.window import Window

batch_id = str(uuid.uuid4())
current_user = spark.sql("SELECT current_user() AS u").collect()[0]["u"]

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType

def log_start():
    start = datetime.datetime.utcnow()
    row = [(batch_id, pipeline_name, None, "SILVER", "RUNNING", None, None, None, start, None, None, None, current_user, start)]
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
    return spark.sql(f"""
        SELECT execution_log_id FROM logging.execution_log
        WHERE batch_id = '{batch_id}' ORDER BY execution_log_id DESC LIMIT 1
    """).collect()[0][0], start

def log_end(exec_log_id, status, rows_loaded, error_message=None):
    end = datetime.datetime.utcnow()
    spark.sql(f"""
        UPDATE logging.execution_log
        SET execution_status = '{status}', end_time = TIMESTAMP'{end}',
            rows_loaded = {rows_loaded if rows_loaded is not None else 'NULL'},
            error_message = {("'" + error_message.replace("'", "''") + "'") if error_message else 'NULL'}
        WHERE execution_log_id = {exec_log_id}
    """)

def log_detail(exec_log_id, notebook_name, status, rows_read, rows_written, start, error_message=None):
    end = datetime.datetime.utcnow()
    duration = int((end - start).total_seconds())
    row = [(exec_log_id, None, pipeline_name, notebook_name, "SILVER", status,
            rows_read, rows_written, 0, 0, 0, start, end, duration, error_message, end)]
    schema = StructType([
        StructField("execution_log_id", LongType(), False),
        StructField("source_table_id", LongType(), True),
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
    df = spark.createDataFrame(row, schema)
    df.write.mode("append").saveAsTable("logging.execution_detail")

# COMMAND ----------

def transform_table(tm):
    start = datetime.datetime.utcnow()
    bronze_table = f"bronze.{tm.source_table}"
    silver_table = f"silver.{tm.target_table}"
    pk_cols = [c.strip() for c in tm.primary_key_columns.split(",")]

    try:
        df = spark.table(bronze_table)

        for c in pk_cols:
            df = df.filter(F.col(c).isNotNull())

        w = Window.partitionBy(*pk_cols).orderBy(F.col("_ingested_at").desc())
        df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

        df = df.drop("_source_file", "_batch_id").withColumn("_silver_loaded_at", F.current_timestamp())

        rows_read = df.count()
        table_exists = spark.catalog.tableExists(silver_table)

        if tm.merge_strategy == "MERGE" and table_exists:
            df.createOrReplaceTempView("_src_silver")
            merge_cond = " AND ".join([f"t.{c} = s.{c}" for c in pk_cols])
            spark.sql(f"""
                MERGE INTO {silver_table} t
                USING _src_silver s
                ON {merge_cond}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
        else:
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
                .saveAsTable(silver_table)

        log_detail(exec_log_id, "03_silver_transform", "SUCCESS", rows_read, rows_read, start)
        return rows_read, "SUCCESS", None

    except Exception as e:
        log_detail(exec_log_id, "03_silver_transform", "FAILED", 0, 0, start, str(e))
        return 0, "FAILED", str(e)

# COMMAND ----------

active_transforms = spark.sql("""
    SELECT * FROM metadata.transformation_metadata
    WHERE layer = 'SILVER' AND is_active = true
""").collect()

exec_log_id, batch_start = log_start()

total_rows, failures = 0, []
for tm in active_transforms:
    rows, status, err = transform_table(tm)
    total_rows += rows
    if status == "FAILED":
        failures.append((tm.target_table, err))

batch_status = "FAILED" if failures else "SUCCESS"
log_end(exec_log_id, batch_status, total_rows,
        error_message="; ".join([f"{t}: {e}" for t, e in failures]) if failures else None)

# COMMAND ----------

print(f"Silver batch finished: {batch_status}, {total_rows} rows across {len(active_transforms)} tables.")
if failures:
    dbutils.notebook.exit(f"SILVER_FAILED: {failures}")
else:
    dbutils.notebook.exit("SILVER_SUCCESS")
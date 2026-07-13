# Databricks notebook source
# =============================================================================
# 01_seed_metadata.py
# Purpose : Seed metadata tables for the 7 flat CSV files sitting in
#           /Volumes/de_lakehouse/raw/landing/  (address, Customer, person,
#           Product, SalesOrderDetail, SalesOrderHeader, salesperson).
# Idempotent: re-running overwrites the seed rows.
# =============================================================================

# COMMAND ----------

dbutils.widgets.text("catalog_name", "de_lakehouse")
catalog = dbutils.widgets.get("catalog_name")
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

from pyspark.sql import Row
import datetime

now = datetime.datetime.utcnow()
current_user = spark.sql("SELECT current_user() AS u").collect()[0]["u"]

# COMMAND ----------

source_system_rows = [
    Row(source_system_code="AW_CSV", source_system_name="AdventureWorks CSV Extract",
        source_system_type="File", connection_reference="raw.landing",
        description="AdventureWorks flat-file extract landed as CSV", is_active=True,
        created_date=now, created_by=current_user)
]
spark.createDataFrame(source_system_rows) \
    .write.mode("overwrite").option("mergeSchema", "true") \
    .saveAsTable("metadata.source_system")

source_system_id = spark.table("metadata.source_system") \
    .filter("source_system_code = 'AW_CSV'") \
    .select("source_system_id").collect()[0][0]

# COMMAND ----------

# (schema, table, landing_file_name, short_name, load_type, watermark_col, pk_cols, merge_strategy, load_seq)
table_defs = [
    ("Sales",      "SalesOrderHeader", "SalesOrderHeader.csv", "salesorderheader", "INCREMENTAL", "ModifiedDate", "SalesOrderID",                    "MERGE",     10),
    ("Sales",      "SalesOrderDetail", "SalesOrderDetail.csv", "salesorderdetail", "INCREMENTAL", "ModifiedDate", "SalesOrderID,SalesOrderDetailID", "OVERWRITE", 20),
    ("Sales",      "Customer",         "Customer.csv",         "customer",         "FULL",       None,           "CustomerID",                     "OVERWRITE", 30),
    ("Person",     "Person",           "person.csv",           "person",           "FULL",       None,           "BusinessEntityID",               "OVERWRITE", 35),
    ("Production", "Product",          "Product.csv",          "product",          "FULL",       None,           "ProductID",                      "OVERWRITE", 40),
    ("Person",     "Address",          "address.csv",          "address",          "FULL",       None,           "AddressID",                      "OVERWRITE", 50),
    ("Sales",      "SalesPerson",      "salesperson.csv",      "salesperson",      "FULL",       None,           "BusinessEntityID",               "OVERWRITE", 60),
]

st_rows = []
for schema, table, fname, short, load_type, wm_col, pk, strategy, seq in table_defs:
    st_rows.append(Row(
        source_system_id=source_system_id, source_schema=schema, source_table_name=table,
        landing_file_name=fname, target_folder=short, file_format="CSV",
        load_type=load_type, watermark_column=wm_col, is_active=True, load_sequence=seq,
        description=f"AdventureWorks {schema}.{table}", created_date=now, created_by=current_user
    ))

from pyspark.sql.functions import col

df_st = spark.createDataFrame(st_rows)
df_st = df_st.withColumn("load_sequence", col("load_sequence").cast("int"))
df_st.write.mode("overwrite").option("mergeSchema", "true") \
    .saveAsTable("metadata.source_table")

st_lookup = {r["source_table_name"]: r["source_table_id"]
             for r in spark.table("metadata.source_table").collect()}

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType, BooleanType
from pyspark.sql.functions import col

tlc_rows, tm_rows = [], []
for schema, table, fname, short, load_type, wm_col, pk, strategy, seq in table_defs:
    bronze_tbl = f"br_{short}"
    silver_tbl = f"sl_{short}"
    tlc_rows.append(Row(
        source_table_id=st_lookup[table], landing_file_name=fname, file_format="CSV",
        load_type=load_type, watermark_column=wm_col, primary_key_columns=pk,
        partition_column=None, merge_strategy=strategy,
        bronze_table_name=bronze_tbl, silver_table_name=silver_tbl, gold_table_name=None,
        notebook_name="02_bronze_ingestion", pipeline_name="PL_Master_Load",
        retry_count=3, retry_interval_seconds=60, timeout_minutes=120, load_sequence=seq,
        is_active=True, created_date=now, created_by=current_user
    ))
    tm_rows.append(Row(
        source_table=bronze_tbl, target_table=silver_tbl, notebook_name="03_silver_transform",
        layer="SILVER", primary_key_columns=pk, merge_strategy=strategy,
        is_active=True, created_date=now, created_by=current_user
    ))

tlc_schema = StructType([
    StructField("source_table_id", LongType(), False),
    StructField("landing_file_name", StringType(), False),
    StructField("file_format", StringType(), False),
    StructField("load_type", StringType(), False),
    StructField("watermark_column", StringType(), True),
    StructField("primary_key_columns", StringType(), False),
    StructField("partition_column", StringType(), True),
    StructField("merge_strategy", StringType(), False),
    StructField("bronze_table_name", StringType(), False),
    StructField("silver_table_name", StringType(), False),
    StructField("gold_table_name", StringType(), True),
    StructField("notebook_name", StringType(), False),
    StructField("pipeline_name", StringType(), False),
    StructField("retry_count", IntegerType(), False),
    StructField("retry_interval_seconds", IntegerType(), False),
    StructField("timeout_minutes", IntegerType(), False),
    StructField("load_sequence", IntegerType(), False),
    StructField("is_active", BooleanType(), False),
    StructField("created_date", TimestampType(), False),
    StructField("created_by", StringType(), False)
])

tm_schema = StructType([
    StructField("source_table", StringType(), False),
    StructField("target_table", StringType(), False),
    StructField("notebook_name", StringType(), False),
    StructField("layer", StringType(), False),
    StructField("primary_key_columns", StringType(), False),
    StructField("merge_strategy", StringType(), False),
    StructField("is_active", BooleanType(), False),
    StructField("created_date", TimestampType(), False),
    StructField("created_by", StringType(), False)
])

spark.createDataFrame(tlc_rows, tlc_schema).write.mode("overwrite").option("mergeSchema", "true") \
    .saveAsTable("metadata.table_load_configuration")

spark.createDataFrame(tm_rows, tm_schema).write.mode("overwrite").option("mergeSchema", "true") \
    .saveAsTable("metadata.transformation_metadata")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, BooleanType

watermark_rows = []
for schema, table, fname, short, load_type, wm_col, pk, strategy, seq in table_defs:
    if load_type == "INCREMENTAL":
        watermark_rows.append(Row(
            source_table_id=st_lookup[table], watermark_column=wm_col,
            last_successful_value="1900-01-01T00:00:00", current_run_value=None,
            last_run_start_time=None, last_run_end_time=None, last_run_status=None,
            is_active=True, created_date=now, created_by=current_user,
            modified_date=None, modified_by=None
        ))

if watermark_rows:
    watermark_schema = StructType([
        StructField("source_table_id", LongType(), False),
        StructField("watermark_column", StringType(), False),
        StructField("last_successful_value", StringType(), False),
        StructField("current_run_value", StringType(), True),
        StructField("last_run_start_time", TimestampType(), True),
        StructField("last_run_end_time", TimestampType(), True),
        StructField("last_run_status", StringType(), True),
        StructField("is_active", BooleanType(), False),
        StructField("created_date", TimestampType(), False),
        StructField("created_by", StringType(), False),
        StructField("modified_date", TimestampType(), True),
        StructField("modified_by", StringType(), True)
    ])
    spark.createDataFrame(watermark_rows, watermark_schema).write.mode("overwrite").option("mergeSchema", "true") \
        .saveAsTable("control.watermark")

# COMMAND ----------

display(spark.table("metadata.table_load_configuration"))
print("Metadata seeded for", len(table_defs), "tables (including Person).")
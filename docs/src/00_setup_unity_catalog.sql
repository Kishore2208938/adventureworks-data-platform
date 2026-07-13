-- Databricks notebook source
-- =========================================================================
-- 00_setup_unity_catalog.sql
-- Purpose : Create catalog, schemas, volume and the metadata / control /
--           logging Delta tables that mirror AW_ETL_Framework (SQL Server).
-- Run once. Safe to re-run (IF NOT EXISTS everywhere).
-- =========================================================================

-- COMMAND ----------

-- Parameter widget (change to your workspace catalog name if different)
CREATE WIDGET TEXT catalog_name DEFAULT "de_lakehouse";

-- COMMAND ----------

-- %python
-- catalog = dbutils.widgets.get("catalog_name")
-- spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS de_lakehouse;
USE CATALOG de_lakehouse;

CREATE SCHEMA IF NOT EXISTS raw       COMMENT 'Landing zone for files delivered by ADF / manual upload';
CREATE SCHEMA IF NOT EXISTS bronze    COMMENT 'Raw-to-Delta, 1:1 with source, append/merge only';
CREATE SCHEMA IF NOT EXISTS silver    COMMENT 'Cleansed, deduped, conformed';
CREATE SCHEMA IF NOT EXISTS gold      COMMENT 'Star schema: Dim*/Fact*';
CREATE SCHEMA IF NOT EXISTS metadata  COMMENT 'Source/target registration and load configuration';
CREATE SCHEMA IF NOT EXISTS control   COMMENT 'Watermarks and run-state';
CREATE SCHEMA IF NOT EXISTS logging   COMMENT 'Pipeline / step execution logs';

-- Managed volume that ADF (or manual upload) lands parquet files into.
-- Path pattern matches TargetContainer/TargetFolder from your ADF metadata:
--   /Volumes/de_lakehouse/raw/landing/<targetfolder>/*.parquet
CREATE VOLUME IF NOT EXISTS raw.landing COMMENT 'Raw file landing area, one subfolder per source table';

-- COMMAND ----------

-- ============================ metadata.SourceSystem =====================
CREATE TABLE IF NOT EXISTS metadata.source_system (
  source_system_id     BIGINT GENERATED ALWAYS AS IDENTITY,
  source_system_code    STRING NOT NULL,
  source_system_name    STRING NOT NULL,
  source_system_type    STRING NOT NULL,
  connection_reference  STRING,
  description           STRING,
  is_active             BOOLEAN NOT NULL DEFAULT TRUE,
  created_date           TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  created_by             STRING NOT NULL,
  modified_date          TIMESTAMP,
  modified_by            STRING
) USING DELTA
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');

-- COMMAND ----------

-- ============================ metadata.SourceTable =======================
CREATE TABLE IF NOT EXISTS metadata.source_table (
  source_table_id     BIGINT GENERATED ALWAYS AS IDENTITY,
  source_system_id    BIGINT NOT NULL,
  source_schema       STRING NOT NULL,
  source_table_name   STRING NOT NULL,
  landing_file_name   STRING NOT NULL,       -- exact filename in raw.landing volume, e.g. 'Customer.csv'
  target_folder       STRING NOT NULL,       -- short name used for bronze/silver table suffix
  file_format         STRING NOT NULL DEFAULT 'CSV',
  load_type           STRING NOT NULL,       -- FULL | INCREMENTAL
  watermark_column    STRING,
  is_active           BOOLEAN NOT NULL DEFAULT TRUE,
  load_sequence       INT NOT NULL DEFAULT 1,
  description         STRING,
  created_date        TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  created_by          STRING NOT NULL
) USING DELTA
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');

-- COMMAND ----------

-- ===================== metadata.TableLoadConfiguration ===================
CREATE TABLE IF NOT EXISTS metadata.table_load_configuration (
  table_configuration_id  BIGINT GENERATED ALWAYS AS IDENTITY,
  source_table_id         BIGINT NOT NULL,
										  
  landing_file_name       STRING NOT NULL,
  file_format             STRING NOT NULL DEFAULT 'CSV',
  load_type                STRING NOT NULL,     -- FULL | INCREMENTAL
  watermark_column          STRING,
  primary_key_columns       STRING,              -- comma separated
  partition_column           STRING,
  merge_strategy               STRING,              -- MERGE | OVERWRITE
															  
  bronze_table_name             STRING,
  silver_table_name              STRING,
  gold_table_name                  STRING,
  notebook_name                     STRING,
  pipeline_name                      STRING,
  retry_count                         INT NOT NULL DEFAULT 3,
  retry_interval_seconds                INT NOT NULL DEFAULT 60,
  timeout_minutes                        INT NOT NULL DEFAULT 120,
  load_sequence                           INT NOT NULL DEFAULT 1,
  is_active                                BOOLEAN NOT NULL DEFAULT TRUE,
  created_date                              TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  created_by                                 STRING NOT NULL
) USING DELTA
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');

-- COMMAND ----------

-- ===================== metadata.TransformationMetadata ===================
-- New table (Bronze->Silver->Gold lineage), same idea Anthropic-chat-suggested
-- for the ADF phase, now driving the Databricks phase.
CREATE TABLE IF NOT EXISTS metadata.transformation_metadata (
  transformation_id   BIGINT GENERATED ALWAYS AS IDENTITY,
  source_table         STRING NOT NULL,   -- bronze table (short name)
  target_table          STRING NOT NULL,   -- silver/gold table (short name)
  notebook_name           STRING NOT NULL,
  layer                     STRING NOT NULL,   -- BRONZE | SILVER | GOLD
  primary_key_columns       STRING,
  merge_strategy              STRING,
  is_active                    BOOLEAN NOT NULL DEFAULT TRUE,
  created_date                 TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  created_by                    STRING NOT NULL
) USING DELTA
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');

-- COMMAND ----------

-- ============================ control.Watermark ===========================
CREATE TABLE IF NOT EXISTS control.watermark (
  watermark_id            BIGINT GENERATED ALWAYS AS IDENTITY,
  source_table_id          BIGINT NOT NULL,
  watermark_column          STRING NOT NULL,
  last_successful_value      STRING,
  current_run_value            STRING,
  last_run_start_time            TIMESTAMP,
  last_run_end_time                TIMESTAMP,
  last_run_status                    STRING,
  is_active                            BOOLEAN NOT NULL DEFAULT TRUE,
  created_date                          TIMESTAMP NOT NULL DEFAULT current_timestamp(),
  created_by                             STRING NOT NULL,
  modified_date                           TIMESTAMP,
  modified_by                              STRING
) USING DELTA
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');

-- COMMAND ----------

-- ============================ logging.ExecutionLog =========================
CREATE TABLE IF NOT EXISTS logging.execution_log (
  execution_log_id   BIGINT GENERATED ALWAYS AS IDENTITY,
  batch_id             STRING,
  pipeline_name          STRING NOT NULL,
  pipeline_run_id          STRING,
  layer_name                 STRING NOT NULL,   -- BRONZE | SILVER | GOLD
  execution_status             STRING NOT NULL,   -- RUNNING | SUCCESS | FAILED
  rows_extracted                 BIGINT,
  rows_loaded                      BIGINT,
  rows_rejected                      BIGINT,
  start_time                          TIMESTAMP NOT NULL,
  end_time                             TIMESTAMP,
  duration_in_seconds                    INT,
  error_message                          STRING,
  executed_by                              STRING,
  created_date                              TIMESTAMP NOT NULL DEFAULT current_timestamp()
) USING DELTA
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');

-- COMMAND ----------

-- ============================ logging.ExecutionDetail =======================
CREATE TABLE IF NOT EXISTS logging.execution_detail (
  execution_detail_id   BIGINT GENERATED ALWAYS AS IDENTITY,
  execution_log_id        BIGINT NOT NULL,
  source_table_id           BIGINT,
  pipeline_name               STRING NOT NULL,
  notebook_name                  STRING,
  layer_name                       STRING NOT NULL,
  execution_status                   STRING NOT NULL,
  rows_read                            BIGINT,
  rows_inserted                          BIGINT,
  rows_updated                             BIGINT,
  rows_deleted                               BIGINT,
  rows_rejected                                BIGINT,
  start_time                                    TIMESTAMP NOT NULL,
  end_time                                        TIMESTAMP,
  duration_in_seconds                               INT,
  error_message                                       STRING,
  created_date                                          TIMESTAMP NOT NULL DEFAULT current_timestamp()
) USING DELTA
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');


-- COMMAND ----------

SELECT 'Unity Catalog framework created.' AS status;

-- COMMAND ----------

SELECT tlc.*, st.source_table_name, st.source_schema
    FROM metadata.table_load_configuration tlc
    INNER JOIN metadata.source_table st ON tlc.source_table_id = st.source_table_id
    WHERE tlc.is_active = true AND st.is_active = true
    ORDER BY tlc.load_sequence
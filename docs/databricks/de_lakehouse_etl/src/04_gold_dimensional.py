# Databricks notebook source
# =============================================================================
# 04_gold_dimensional.py
# Purpose : Build the Gold star schema from Silver (de_lakehouse).
#           Dimensional modeling is intentionally explicit (business logic),
#           logging/lineage still follows the framework contract.
#
#   DimCustomer      <- silver.sl_customer + silver.sl_person (names)
#   DimProduct       <- silver.sl_product
#   DimAddress       <- silver.sl_address
#   DimSalesPerson   <- silver.sl_salesperson
#   FactSalesOrder   <- silver.sl_salesorderheader + silver.sl_salesorderdetail
# =============================================================================

# COMMAND ----------

dbutils.widgets.text("catalog_name", "de_lakehouse")
catalog = dbutils.widgets.get("catalog_name")
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

import datetime, uuid
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType

batch_id = str(uuid.uuid4())
current_user = spark.sql("SELECT current_user() AS u").collect()[0]["u"]
pipeline_name = "PL_Gold_Load"

def log_start():
    start = datetime.datetime.utcnow()
    row = [(batch_id, pipeline_name, None, "GOLD", "RUNNING", None, None, None, start, None, None, None, current_user, start)]
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
    return spark.sql(f"""SELECT execution_log_id FROM logging.execution_log
        WHERE batch_id = '{batch_id}' ORDER BY execution_log_id DESC LIMIT 1""").collect()[0][0], start

def log_end(exec_log_id, status, rows_loaded, error_message=None):
    end = datetime.datetime.utcnow()
    spark.sql(f"""UPDATE logging.execution_log SET execution_status = '{status}',
        end_time = TIMESTAMP'{end}', rows_loaded = {rows_loaded},
        error_message = {("'" + error_message.replace("'", "''") + "'") if error_message else 'NULL'}
        WHERE execution_log_id = {exec_log_id}""")

exec_log_id, batch_start = log_start()
total_rows = 0

# COMMAND ----------

# ---------------------------- DimCustomer (+ Person names) ---------------------
sl_customer = spark.table("silver.sl_customer")
sl_person = spark.table("silver.sl_person")

dim_customer = (
    sl_customer.alias("c")
    .join(sl_person.alias("p"), F.col("c.PersonID") == F.col("p.BusinessEntityID"), "left")
    .select(
        F.col("c.CustomerID").alias("customer_id"),
        F.col("c.PersonID").alias("person_id"),
        F.col("c.StoreID").alias("store_id"),
        F.col("c.TerritoryID").alias("territory_id"),
        F.col("p.PersonType").alias("person_type"),
        F.col("p.FirstName").alias("first_name"),
        F.col("p.MiddleName").alias("middle_name"),
        F.col("p.LastName").alias("last_name"),
        F.current_timestamp().alias("_gold_loaded_at"),
    )
)
dim_customer.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("gold.dim_customer")
total_rows += dim_customer.count()

# COMMAND ----------

# ---------------------------- DimProduct ---------------------------------------
dim_product = spark.table("silver.sl_product").select(
    F.col("ProductID").alias("product_id"),
    F.col("Name").alias("product_name"),
    F.col("ProductNumber").alias("product_number"),
    F.col("Color").alias("color"),
    F.col("ListPrice").alias("list_price"),
    F.col("StandardCost").alias("standard_cost"),
    F.current_timestamp().alias("_gold_loaded_at"),
)
dim_product.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("gold.dim_product")
total_rows += dim_product.count()

# COMMAND ----------

# ---------------------------- DimAddress ----------------------------------------
dim_address = spark.table("silver.sl_address").select(
    F.col("AddressID").alias("address_id"),
    F.col("AddressLine1").alias("address_line1"),
    F.col("City").alias("city"),
    F.col("StateProvinceID").alias("state_province_id"),
    F.col("PostalCode").alias("postal_code"),
    F.current_timestamp().alias("_gold_loaded_at"),
)
dim_address.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("gold.dim_address")
total_rows += dim_address.count()

# COMMAND ----------

# ---------------------------- DimSalesPerson --------------------------------------
dim_salesperson = spark.table("silver.sl_salesperson").select(
    F.col("BusinessEntityID").alias("sales_person_id"),
    F.col("TerritoryID").alias("territory_id"),
    F.col("SalesQuota").alias("sales_quota"),
    F.col("Bonus").alias("bonus"),
    F.col("CommissionPct").alias("commission_pct"),
    F.current_timestamp().alias("_gold_loaded_at"),
)
dim_salesperson.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("gold.dim_salesperson")
total_rows += dim_salesperson.count()

# COMMAND ----------

# ---------------------------- FactSalesOrder ---------------------------------------
header = spark.table("silver.sl_salesorderheader").select(
    "SalesOrderID", "OrderDate", "DueDate", "ShipDate", "Status",
    "CustomerID", "SalesPersonID", "TerritoryID", "BillToAddressID", "ShipToAddressID",
    "SubTotal", "TaxAmt", "Freight", "TotalDue",
)
detail = spark.table("silver.sl_salesorderdetail").select(
    "SalesOrderID", "SalesOrderDetailID", "ProductID", "OrderQty",
    "UnitPrice", "UnitPriceDiscount", "LineTotal",
)

fact_sales = (
    detail.join(header, on="SalesOrderID", how="inner")
    .select(
        F.col("SalesOrderID").alias("sales_order_id"),
        F.col("SalesOrderDetailID").alias("sales_order_detail_id"),
        F.col("OrderDate").cast("date").alias("order_date"),
        F.col("DueDate").cast("date").alias("due_date"),
        F.col("ShipDate").cast("date").alias("ship_date"),
        F.col("CustomerID").alias("customer_id"),
        F.col("SalesPersonID").alias("sales_person_id"),
        F.col("TerritoryID").alias("territory_id"),
        F.col("ProductID").alias("product_id"),
        F.col("BillToAddressID").alias("bill_to_address_id"),
        F.col("ShipToAddressID").alias("ship_to_address_id"),
        F.col("OrderQty").alias("order_qty"),
        F.col("UnitPrice").alias("unit_price"),
        F.col("UnitPriceDiscount").alias("unit_price_discount"),
        F.col("LineTotal").alias("line_total"),
        F.col("TaxAmt").alias("tax_amt"),
        F.col("Freight").alias("freight"),
        F.col("TotalDue").alias("total_due"),
        F.current_timestamp().alias("_gold_loaded_at"),
    )
)

fact_table = "gold.fact_sales_order"
if spark.catalog.tableExists(fact_table):
    fact_sales.createOrReplaceTempView("_src_fact_sales")
    spark.sql(f"""
        MERGE INTO {fact_table} t
        USING _src_fact_sales s
        ON t.sales_order_id = s.sales_order_id AND t.sales_order_detail_id = s.sales_order_detail_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
else:
    fact_sales.write.format("delta").mode("overwrite") \
        .partitionBy("order_date") \
        .saveAsTable(fact_table)

total_rows += fact_sales.count()

# COMMAND ----------

log_end(exec_log_id, "SUCCESS", total_rows)
print(f"Gold batch finished: SUCCESS, {total_rows} rows written across 5 gold tables.")
dbutils.notebook.exit("GOLD_SUCCESS")
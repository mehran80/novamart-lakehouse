# Databricks notebook source
import os
import sys
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, BooleanType, DateType, TimestampType
from pyspark.testing.utils import assertDataFrameEqual
from decimal import Decimal
import datetime as dt

# ----------------------------------------------------------------------
# Make src/ importable when running locally / in CI (outside Databricks).
# In Databricks, these same functions are also reachable via `%run`, so
# this block is what makes the file dual-purpose: works as a Databricks
# notebook AND as a plain pytest module.
# ----------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

from utils.silver_transformations import (
    bronze_sql_customers_silver,
    bronze_crm_customers_silver,
    bronze_sql_products_silver,
    bronze_sql_sale_transactions,
    bronze_clickstream,
)
from utils.gold_transformations import (
    transfer_silver_to_gold_dim_customer,
    transfer_silver_to_gold_dim_products,
    transfer_silver_to_gold_fact_inventory,
    transfer_silver_to_gold_fact_sales,
    transfer_silver_to_gold_fact_clicks,
    daily_sales,
    low_stock_alert,
)

# ----------------------------------------------------------------------
# In Databricks, `spark` is auto-injected into every notebook. Outside
# Databricks (local pytest, GitHub Actions), we have to create it ourselves.
# ----------------------------------------------------------------------
# Newer JDKs (17+) block the low-level memory access that Apache Arrow (used
# by @pandas_udf) needs, unless explicitly reopened via --add-opens. Without
# this, pandas_udf calls fail with "sun.misc.Unsafe ... not available".
_arrow_jvm_opts = "--add-opens=java.base/java.nio=ALL-UNNAMED"
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("pytest-novamart")
    .config("spark.driver.extraJavaOptions", _arrow_jvm_opts)
    .config("spark.executor.extraJavaOptions", _arrow_jvm_opts)
    .getOrCreate()
)

# COMMAND ----------

def test_bronze_sql_customers_silver():
    raw_data = [
        ("C10139  ", "  bryan  ", "  wiggins  ", "  MARYstone@example.com  ", "(495)657-9887x74924", 
         "5402 Mitchell NW Apt. 3B", "lake amanda", "  latam  ", "2023-09-10", "smb", "null",
         "2026-07-17T20:08:21.026Z", "/Volumes/novamart/bronze/customers/file.csv")
    ]
    raw_schema = [
        "customer_id", "first_name", "last_name", "email", "phone", 
        "address", "city", "region", "signup_date", "customer_segment", 
        "_rescued_data", "ingesttime", "file_name"
    ]

    df_raw_mock = spark.createDataFrame(raw_data, raw_schema)
    df_actual = bronze_sql_customers_silver(df_raw_mock)

    expected_schema = [
        "customer_id", "first_name", "last_name", "email", "address", 
        "city", "region", "customer_segment", "full_name", "verified_email", 
        "cleaned_phone", "verified_phone"
    ]
    expected_data = [
        (
            "C10139",
            "Bryan",
            "Wiggins",
            "marystone@example.com",
            "5402 Mitchell NW Apt. 3B",
            "Lake Amanda",
            "LATAM",
            "Smb",
            "Bryan Wiggins",
            True,
            "+14956579887",
            True 
        )
    ]

    df_expected = spark.createDataFrame(expected_data, expected_schema)

    assertDataFrameEqual(
        df_actual.select(*expected_schema), df_expected
    )

# COMMAND ----------

def test_bronze_crm_customers_silver():
    raw_data = [
        ("2023-09-10", "C10139", "spring_2025", "10000", "0.2001", "  platinium  ", " TRUE", " email", "null", "2026-07-17T21:09:54.743+00:00", 
         "/Volumes/novamart/bronze/landing/landing_zone/crm_api/customers/bronze_crm_customers_2026-07-05.json"
         )
    ]
    raw_schema = [
        "crm_last_updated", "customer_id", "last_campaign_engaged", "lifetime_value_estimate", "churn_risk_score",
        "loyalty_tier", "marketing_opt_in", "preferred_channel", "_rescued_data", "ingesttime", "file_name"
    ]

    df_raw = spark.createDataFrame(raw_data, raw_schema)
    df_actual_crm_cust = bronze_crm_customers_silver(df_raw)

    expected_data = [
        (
            "C10139", "Spring_2025", Decimal("10000.00"), Decimal("0.200"), "Platinium", True, "Email"
         )
    ]

    expected_schema = StructType([
        StructField('customer_id', StringType(), True), 
        StructField('last_campaign_engaged', StringType(), True), 
        StructField('lifetime_value_estimate', DecimalType(10, 2), True),
        StructField('churn_risk_score', DecimalType(5, 3), True),
        StructField('loyalty_tier', StringType(), True), 
        StructField('marketing_opt_in', BooleanType(), False),
        StructField('preferred_channel', StringType(), True)
    ])

    df_expected = spark.createDataFrame(expected_data, expected_schema)

    assert_cols_silver_crm_customers = [
        "customer_id", "last_campaign_engaged", "lifetime_value_estimate", 
        "churn_risk_score", "loyalty_tier", "marketing_opt_in", "preferred_channel"
    ]
    
    assertDataFrameEqual(
        df_actual_crm_cust.select(*assert_cols_silver_crm_customers), df_expected
    )

# COMMAND ----------

def test_bronze_sql_products_silver():
    
    raw_data = [
        (
            "P1000", "Purpose Headphone Lite", "Electronics", "Headphones", "Johnson LLC", "71.76", "47.2", "71", "10", "SUP35", "2024-02-20", "null", "2026-07-17T20:50:56.358+00:00", "/Volumes/novamart/bronze/landing/landing_zone/sales_db/products/bronze_sql_server_products.csv"
        )
    ]

    raw_schema = [
        "product_id", "product_name", "category", "subcategory", "brand","unit_price", "cost_price", "stock_quantity", "reorder_threshold", "supplier_id", "created_date", "_rescued_data", "ingesttime", "file_name"
    ]

    def_raw = spark.createDataFrame(raw_data, raw_schema)
    df_actual_sql_products = bronze_sql_products_silver(def_raw)

    expected_schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("subcategory", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("unit_price", DecimalType(10, 2), False),
        StructField("cost_price", DecimalType(10, 2), False),
        StructField("stock_quantity", IntegerType(), False),
        StructField("reorder_threshold", IntegerType(), False),
        StructField("supplier_id", StringType(), True),
        StructField("is_margin_positive", BooleanType(), True),
        StructField("is_reorder_needed", BooleanType(), True)
    ])

    expected_data = [
        (
            "P1000", "Purpose Headphone Lite", "Electronics", "Headphones", "Johnson LLC", Decimal("71.76"), Decimal("47.2"), 71, 10, "SUP35", True, False
        )
    ]

    df_expected = spark.createDataFrame(expected_data, expected_schema)

    assert_cols_silver_products = [
        "product_id", "product_name", "category", "subcategory", "brand", 
        "unit_price", "cost_price", "stock_quantity", "reorder_threshold", 
        "supplier_id", "is_margin_positive", "is_reorder_needed"
    ]

    assertDataFrameEqual(
        df_actual_sql_products.select(*assert_cols_silver_products), df_expected
        )

# COMMAND ----------

def test_bronze_sql_sale_transactions():
    raw_data = [
        (
            "T122221", "C11746", "P1390", "1", "201.92", "0.1", "181.73", "STORE03", "latam", 
            "2026-07-04T21:03:00", "2026-07-04T21:03:00", "credit_card", "null", "2026-07-17T20:51:00.842+00:00", 
            "/Volumes/novamart/bronze/landing/landing_zone/sales_db/sales_transactions/bronze_sql_server_sales_transactions.csv"
        )
    ]
    raw_schema = [
        "transaction_id", "customer_id", "product_id", "quantity", "unit_price", 
        "discount_pct", "total_amount", "store_id", "region", "transaction_ts", 
        "last_modified_ts", "payment_method", "_rescued_data", "ingesttime", "file_name"
    ]

    df_raw = spark.createDataFrame(raw_data, raw_schema)
    df_actual_sale_tran = bronze_sql_sale_transactions(df_raw)
    
    expected_data =[
        (
            "T122221", "C11746", "P1390", "STORE03", "CREDIT_CARD", 
            1, Decimal("201.92"), Decimal("0.10"), Decimal("181.73"), "LATAM", True
        )
    ]

    expected_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DecimalType(10, 2), False),
        StructField("discount_pct", DecimalType(10, 2), False),
        StructField("total_amount", DecimalType(10, 2), False),
        StructField("region", StringType(), True),
        StructField("is_pricing_correct", BooleanType(), True)
    ])

    df_expected = spark.createDataFrame(expected_data, expected_schema)

    assert_cols_silver_sales = [
        "transaction_id", "customer_id", "product_id", "store_id", "payment_method", 
        "quantity", "unit_price", "discount_pct", "total_amount", "region", "is_pricing_correct"
    ]
    assertDataFrameEqual(
        df_actual_sale_tran.select(*assert_cols_silver_sales), df_expected
    )

# COMMAND ----------

def test_bronze_clickstream():
    raw_schema = [
        "customer_id",
        "device_type",
        "event_id",
        "event_type",
        "page_url",
        "product_id",
        "session_id",
        "timestamp",
        "_rescued_data",
        "ingesttime",
        "file_name",
    ]

    raw_data = [
        (
            "C10176", "desktop", "E1", "purchase", "/product/P1174", "P1174", "0898bab0-b31b-4c07-a788-59d7a3dbfcac", "2026-07-12T09:00:02", "null", "2026-07-19T12:16:58.787+00:00", "/Volumes/novamart/bronze/landing/landing_zone/kafka/bronze_kafka_website_clickstream.jsonl"
        )
    ]

    df_raw = spark.createDataFrame(raw_data, raw_schema)
    df_actual = bronze_clickstream(df_raw)

    expected_data = [
        (
            "C10176", "DESKTOP", "E1", "PURCHASE", "/product/P1174", "P1174", "0898bab0-b31b-4c07-a788-59d7a3dbfcac", True, True, None
        )
    ]

    expected_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("page_url", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("is_event_valid", BooleanType(), True),
        StructField("is_device_valid", BooleanType(), True),
        StructField("search_query", StringType(), True) # Explicitly defined StringType
    ])

    df_expected = spark.createDataFrame(expected_data, expected_schema)

    assert_cols_silver_clicks = [
        "customer_id", "device_type", "event_id", "event_type", "page_url", 
        "product_id", "session_id", "is_event_valid", "is_device_valid", "search_query"
    ]

    assertDataFrameEqual(
        df_actual.select(*assert_cols_silver_clicks), df_expected
    )

# COMMAND ----------

def test_transfer_silver_to_gold_dim_customer():
    sql_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("cleaned_phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("region", StringType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("verified_email", BooleanType(), True),
        StructField("verified_phone", BooleanType(), True),
        StructField("join_timestamp", TimestampType(), True)
    ])
    sql_data = [
        ("C10001", "Jeremy", "Williams", "Jeremy Williams", "ojackson@example.net", "+14628309130", "8970 April Points", "Whitneyfort", "LATAM", "SMB", True, True, dt.datetime(2024, 11, 3, 0, 0))
    ]
    df_sql = spark.createDataFrame(sql_data, sql_schema)
    
    crm_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("last_campaign_engaged", StringType(), True),
        StructField("lifetime_value_estimate", DecimalType(10, 2), False), 
        StructField("loyalty_tier", StringType(), True),
        StructField("preferred_channel", StringType(), True),
        StructField("marketing_opt_in", BooleanType(), False),           
        StructField("churn_risk_score", DecimalType(5, 3), False),        
        StructField("last_updated_crm", TimestampType(), True)
    ])
    crm_data = [
        ("C10001", "debate_2026", Decimal("100.00"), "Platinum", "Email", True, Decimal("0.214"), dt.datetime(2026, 7, 5, 0, 0))
    ]
    df_crm = spark.createDataFrame(crm_data, crm_schema)
    
    df_actual = transfer_silver_to_gold_dim_customer(df_sql, df_crm)
    
    expected_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("cleaned_phone", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("region", StringType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("verified_email", BooleanType(), True),
        StructField("verified_phone", BooleanType(), True),
        StructField("last_campaign_engaged", StringType(), True),
        StructField("lifetime_value_estimate", DecimalType(10, 2), False), 
        StructField("loyalty_tier", StringType(), False),
        StructField("preffered_channel", StringType(), False),
        StructField("marketing_opt_in", BooleanType(), True),
        StructField("churn_risk_score", DecimalType(5, 3), True)
    ])
    
    expected_data = [
        (
            "C10001", "Jeremy", "Williams", "Jeremy Williams", "ojackson@example.net", 
            "+14628309130", "8970 April Points", "Whitneyfort", "LATAM", "SMB",
            True, True, "debate_2026", Decimal("100.00"), "Platinum", "Email", True, Decimal("0.214")
        )
    ]
    df_expected = spark.createDataFrame(expected_data, expected_schema)
    
    assert_cols_gold_customers = [
        "customer_id", "first_name", "last_name", "full_name", "email", 
        "cleaned_phone", "address", "city", "region", "customer_segment", 
        "verified_email", "verified_phone", "last_campaign_engaged", 
        "lifetime_value_estimate", "loyalty_tier", "preffered_channel", 
        "marketing_opt_in", "churn_risk_score"
    ]
    
    assertDataFrameEqual(df_actual.select(*assert_cols_gold_customers), df_expected)

# COMMAND ----------

def test_transfer_silver_to_gold_dim_products():
    raw_data = [
        ("P1000", "Purpose Headphone Lite", "Electronics", "Headphones", "Johnson LLC", Decimal("47.20"), Decimal("71.76"), "SUP35", 10, True, dt.datetime(2024,2,20))
    ]
    raw_schema = ["product_id", "product_name", "category", "subcategory", "brand", "cost_price", "unit_price", "supplier_id", "reorder_threshold", "is_margin_positive", "created_date"]
    df_raw = spark.createDataFrame(raw_data, raw_schema)
    
    df_actual = transfer_silver_to_gold_dim_products(df_raw)
    
    assert_cols_gold_products = [
        "product_id", "product_name", "category", "subcategory", "brand", 
        "cost_price", "unit_price", "supplier_id", "reorder_threshold", 
        "is_margin_positive", "gross_margin_percentage"
    ]
    
    expected_data = [
        ("P1000", "Purpose Headphone Lite", "Electronics", "Headphones", "Johnson LLC", Decimal("47.20"), Decimal("71.76"), "SUP35", 10, True, Decimal("34.23"))
    ]

    df_expected = (spark.createDataFrame(expected_data, assert_cols_gold_products)
                   .withColumns({
                       "gross_margin_percentage": col("gross_margin_percentage").cast("decimal(5,2)"),
                       "cost_price": col("cost_price").cast("decimal(10,2)"),
                       "unit_price": col("unit_price").cast("decimal(10,2)")
                       })
                   )
    
    assertDataFrameEqual(df_actual.select(*assert_cols_gold_products), df_expected, ignoreNullable=True)

# COMMAND ----------

def test_transfer_silver_to_gold_fact_inventory():
    raw_data = [
        ("P1000", 71, False)
    ]
    raw_schema = ["product_id", "stock_quantity", "is_reorder_needed"]
    df_raw = spark.createDataFrame(raw_data, raw_schema)
    
    df_actual = transfer_silver_to_gold_fact_inventory(df_raw)
    
    expected_schema = ["product_id", "stock_quantity", "is_reorder_needed"]
    expected_data = [("P1000", 71, False)]
    df_expected = spark.createDataFrame(expected_data, expected_schema)
    
    assertDataFrameEqual(df_actual.select(*expected_schema), df_expected)

# COMMAND ----------

def test_transfer_silver_to_gold_fact_sales():
    raw_data = [
        ("C11746", "P1390", "T122221", "STORE03", 1, Decimal("201.92"), Decimal("0.10"), "CREDIT_CARD", Decimal("181.73"), dt.datetime(2026, 7, 4, 21, 3, 0), dt.datetime(2026, 7, 4, 21, 3, 0), True, True)
    ]
    raw_schema = ["customer_id", "product_id", "transaction_id", "store_id", "quantity", "unit_price", "discount_pct", "payment_method", "total_amount", "transaction_timestamp", "last_modified_timestamp", "is_pricing_correct", "is_time_valid"]
    df_raw = spark.createDataFrame(raw_data, raw_schema)
    
    df_actual = transfer_silver_to_gold_fact_sales(df_raw)
    
    assert_cols_gold_sales = [
        "customer_id", "product_id", "transaction_id", "store_id", 
        "quantity", "unit_price", "discount_pct", "payment_method", "total_amount"
    ]
    expected_data = [
        ("C11746", "P1390", "T122221", "STORE03", 1, Decimal("201.92"), Decimal("0.10"), "CREDIT_CARD", Decimal("181.73"))
    ]
    df_expected = (spark.createDataFrame(expected_data, assert_cols_gold_sales)
                   .withColumns({
                       "unit_price": col("unit_price").cast("decimal(10,2)"),
                       "discount_pct": col("discount_pct").cast("decimal(5,2)"),
                       "total_amount": col("total_amount").cast("decimal(10,2)")
                       })
                   )
    
    assertDataFrameEqual(df_actual.select(*assert_cols_gold_sales), df_expected, ignoreNullable=True)

# COMMAND ----------

def test_transfer_silver_to_gold_fact_clicks():
    raw_data = [
        ("C10176", "P1174", "0898bab0-b31b-4c07-a788-59d7a3dbfcac", "E1", "PURCHASE", "DESKTOP", "/product/P1174", None, dt.datetime(2026, 7, 12, 9, 0, 2), True, True)
    ]
    raw_schema ="""
            customer_id string, product_id string, session_id string, event_id string, event_type string, 
            device_type string, page_url string, search_query string, event_timestamp timestamp, 
            is_event_valid boolean, is_device_valid boolean
        """
    df_raw = spark.createDataFrame(raw_data, raw_schema)
    
    df_actual = transfer_silver_to_gold_fact_clicks(df_raw)
    
    expected_schema ="""
            customer_id string, product_id string, session_id string, event_id string, event_type string, 
            device_type string, page_url string, search_query string
            """ 

    expected_data = [
        ("C10176", "P1174", "0898bab0-b31b-4c07-a788-59d7a3dbfcac", "E1", "PURCHASE", "DESKTOP", "/product/P1174", None)
    ]

    df_expected = spark.createDataFrame(expected_data, expected_schema)

    assert_col_fact_clicks = [
        "customer_id", "product_id", "session_id", "event_id", 
        "event_type", "device_type", "page_url", "search_query"
    ]

    assertDataFrameEqual(df_actual.select(*assert_col_fact_clicks), df_expected, ignoreNullable=True)

# COMMAND ----------

def test_daily_sales():
    sales_data = [
        ("C10001", "P1000", Decimal("181.73"), 1, dt.datetime(2026,7,28))
    ]
    sales_schema = ["customer_id", "product_id", "total_amount", "quantity", "transaction_timestamp"]
    df_sales = spark.createDataFrame(sales_data, sales_schema)
    
    product_data = [
        ("P1000", "Electronics", Decimal("34.23"))
    ]
    product_schema = ["product_id", "category", "gross_margin_percentage"]
    df_product = spark.createDataFrame(product_data, product_schema)
    
    customer_data = [
        ("C10001", "LATAM")
    ]
    customer_schema = ["customer_id", "region"]
    df_customer = spark.createDataFrame(customer_data, customer_schema)
    
    df_actual = daily_sales(df_customer, df_product, df_sales)
    
    expected_schema =expected_schema_ddl = """
        sales_date date, 
        product_category string, 
        customer_region string, 
        daily_sale decimal(12,2), 
        daily_units_sold bigint, 
        average_margin_pct decimal(16,6)
    """
    
    expected_data = [
        (dt.date(2026, 7, 28), "Electronics", "LATAM", Decimal("181.73"), 1, Decimal("34.23"))
    ]

    df_expected = spark.createDataFrame(expected_data, expected_schema)
    
    assert_cols_gold_daily_sales = [
        "sales_date", "product_category", "customer_region", 
        "daily_sale", "daily_units_sold", "average_margin_pct"
    ]
    assertDataFrameEqual(df_actual.select(*assert_cols_gold_daily_sales), df_expected, ignoreNullable=True)

# COMMAND ----------

def test_low_stock_alert():
    clicks_data = [
        ("E1", "0898bab0-b31b-4c07-a788-59d7a3dbfcac", "P1000", "ADD_TO_CART", dt.datetime(2026,7,28,12,0,0))
    ]
    clicks_schema = ["event_id", "session_id", "product_id", "event_type", "event_at"]
    df_clicks = spark.createDataFrame(clicks_data, clicks_schema)
    
    product_data = [
        ("P1000", 10)
    ]
    product_schema = ["product_id", "reorder_threshold"]
    df_product = spark.createDataFrame(product_data, product_schema)
    
    inventory_data = [
        ("P1000", 5)
    ]
    inventory_schema = ["product_id", "stock_quantity"]
    df_inventory = spark.createDataFrame(inventory_data, inventory_schema)
    
    df_actual = low_stock_alert(df_product, df_clicks, df_inventory)
    
    expected_schema = ["event_id", "session_id", "product_id", "available_stock"]

    expected_data = [
        ("E1", "0898bab0-b31b-4c07-a788-59d7a3dbfcac", "P1000", 5)
    ]
    
    df_expected = spark.createDataFrame(expected_data, expected_schema)
    
    assertDataFrameEqual(df_actual.select(*expected_schema), df_expected)
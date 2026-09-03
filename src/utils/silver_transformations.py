from pyspark.sql import DataFrame
from pyspark.sql.types import BooleanType
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, upper, trim, lit, concat, current_timestamp, coalesce, when,
    lower, regexp_extract, desc, row_number
)

from .cleaning_helpers import (
    standardize_date, clean_text_standard, clean_email, format_phone_e164,
    is_verified_email, is_verified_phone_fn, cast_to_decimal, cast_to_integer
)


def bronze_sql_customers_silver(df_bronze: DataFrame) -> DataFrame:
    window_spec = Window.partitionBy("customer_id").orderBy(desc("ingesttime"))
    df_dup = (
        df_bronze
        .select("*", row_number().over(window_spec).alias("row_num"))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )
    df_stage_1 = df_dup.withColumns({
        "_ingest_timestamp": standardize_date("ingesttime"),
        "_source_file": col("file_name"),
        "customer_id": upper(trim(col("customer_id"))),
        "first_name": clean_text_standard("first_name"),
        "last_name": clean_text_standard("last_name"),
        "city": clean_text_standard("city"),
        "customer_segment": clean_text_standard("customer_segment"),
        "address": clean_text_standard("address", enforce_title_case=False),
        "region": clean_text_standard("region", is_acronym=True),
        "join_timestamp": standardize_date("signup_date"),
        "email": clean_email("email"),
        "cleaned_phone": format_phone_e164("phone")
    })

    df_finnal = (df_stage_1.withColumns({
        "full_name": concat(col("first_name"), lit(" "), col("last_name")),
        "verified_email": is_verified_email("email"),
        "verified_phone": is_verified_phone_fn("cleaned_phone"),
        "_record_source": lit("SQL_SERVER_CUSTOMERS"),
        "_silver_processed_at": current_timestamp()
    }))

    df_silver = (df_finnal.drop("phone", "signup_date", "file_name", "ingesttime", "_rescued_data"))
    return df_silver


def bronze_crm_customers_silver(df_bronze: DataFrame) -> DataFrame:
    window_spec = Window.partitionBy("customer_id").orderBy(desc("ingesttime"))
    df_dedup = (
        df_bronze.select("*", row_number().over(window_spec).alias("row_num"))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    df_stage_1 = (df_dedup
                  .withColumns({
                      "last_updated_crm": standardize_date("crm_last_updated"),
                      "customer_id": upper(trim(col("customer_id"))),
                      "last_campaign_engaged": clean_text_standard("last_campaign_engaged", enforce_title_case=True),
                      "lifetime_value_estimate": cast_to_decimal("lifetime_value_estimate", precision=10, scale=2, default_val=0.0),
                      "churn_risk_score": cast_to_decimal("churn_risk_score", 5, 3, 0.0),
                      "loyalty_tier": clean_text_standard("loyalty_tier"),
                      "marketing_opt_in": coalesce(col("marketing_opt_in").cast(BooleanType()), lit(False)),
                      "preferred_channel": clean_text_standard("preferred_channel"),
                      "_ingestion_timestamp": standardize_date("ingesttime"),
                      "_source_file": col("file_name")
                  })
                  )
    df_silver = (df_stage_1.withColumns({
        "_record_source": lit("CRM_SYSTEM_CUSTOMERS"),
        "_silver_processed_at": current_timestamp()
    })
    )
    df_finnal = (df_silver.drop("ingesttime", "file_name", "_rescued_data", "crm_last_updated"))
    return df_finnal


def bronze_sql_products_silver(df_bronze: DataFrame) -> DataFrame:
    window_spec = Window.partitionBy("product_id").orderBy(desc("ingesttime"))
    df_dedup = (
        df_bronze.select("*", row_number().over(window_spec).alias("row_num"))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    df_stage_1 = (df_dedup
                  .withColumns({
                      "_ingestion_timestamp": standardize_date("ingesttime"),
                      "_source_file": col("file_name"),
                      "product_id": upper(trim(col("product_id"))),
                      "product_name": clean_text_standard("product_name", enforce_title_case=True),
                      "category": clean_text_standard("category", enforce_title_case=True),
                      "subcategory": clean_text_standard("subcategory", enforce_title_case=True),
                      "brand": clean_text_standard("brand", enforce_title_case=False),
                      "unit_price": cast_to_decimal("unit_price", precision=10, scale=2, default_val=0.0),
                      "cost_price": cast_to_decimal("cost_price", precision=10, scale=2, default_val=0.0),
                      "stock_quantity": cast_to_integer("stock_quantity", default_val=0),
                      "reorder_threshold": cast_to_integer("reorder_threshold", default_val=0),
                      "supplier_id": upper(trim(col("supplier_id"))),
                      "created_date": standardize_date("created_date")
                  }))
    df_silver = (df_stage_1.withColumns({
        "is_margin_positive": col("unit_price") >= col("cost_price"),
        "is_reorder_needed": col("stock_quantity") <= col("reorder_threshold"),
        "_record_source": lit("SQL_SERVER_PRODUCTS"),
        "_silver_processed_at": current_timestamp()
    })
    )

    df_finnal = df_silver.drop("ingesttime", "file_name", "_rescued_data")
    return df_finnal


def bronze_sql_sale_transactions(df_bronze: DataFrame) -> DataFrame:
    window_spec = Window.partitionBy("transaction_id").orderBy(desc("last_modified_ts"), desc("ingesttime"))
    df_dedup = (
        df_bronze.select("*", row_number().over(window_spec).alias("row_num"))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    df_stage_1 = (df_dedup.withColumns({
        "_ingestion_timestamp": standardize_date("ingesttime"),
        "_source_file": col("file_name"),
        "transaction_id": upper(trim(col("transaction_id"))),
        "customer_id": upper(trim(col("customer_id"))),
        "product_id": upper(trim(col("product_id"))),
        "store_id": upper(trim(col("store_id"))),
        "payment_method": upper(trim(col("payment_method"))),
        "quantity": cast_to_integer("quantity"),
        "unit_price": cast_to_decimal("unit_price", precision=10, scale=2, default_val=0.0),
        "discount_pct": cast_to_decimal("discount_pct", precision=10, scale=2, default_val=0.0),
        "total_amount": cast_to_decimal("total_amount", precision=10, scale=2, default_val=0.0),
        "region": clean_text_standard("region", is_acronym=True),
        "transaction_timestamp": standardize_date("transaction_ts"),
        "last_modified_timestamp": standardize_date("last_modified_ts"),
    }))

    calculated_amount = (col("quantity") * col("unit_price") * (lit(1.0) - col("discount_pct")))

    df_stage_2 = (df_stage_1.withColumns({
        "is_pricing_correct": col("total_amount") == calculated_amount.cast("decimal(10,2)"),
        "is_time_valid": col("last_modified_timestamp") >= col("transaction_timestamp"),
        "_record_source": lit("SQL_SERVER_SALES"),
        "_silver_processed_at": current_timestamp()
    }))

    df_finnal = df_stage_2.drop("ingesttime", "file_name", "_rescued_data", "transaction_ts", "last_modified_ts")
    return df_finnal


def bronze_clickstream(df_bronze: DataFrame) -> DataFrame:
    window_spec = Window.partitionBy("event_id").orderBy(desc("ingesttime"))
    df_dedup = (
        df_bronze.select("*", row_number().over(window_spec).alias("row_num"))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    allowed_events = ["PAGE_VIEW", "SEARCH", "ADD_TO_CART", "REMOVE_FROM_CART", "PURCHASE"]
    allowed_devices = ["DESKTOP", "MOBILE", "TABLET"]

    raw_customer_id = trim(col("customer_id"))

    df_stage_1 = (df_dedup.withColumns({
        "_ingestion_timestamp": standardize_date("ingesttime"),
        "_source_file": col("file_name"),
        "customer_id": when(lower(raw_customer_id) == "null", lit(None).cast("string")).otherwise(upper(raw_customer_id)),
        "event_id": upper(trim(col("event_id"))),
        "session_id": lower(trim(col("session_id"))),
        "product_id": upper(trim(col("product_id"))),
        "event_type": upper(trim(col("event_type"))),
        "device_type": upper(trim(col("device_type"))),
        "page_url": trim(col("page_url")),
        "event_timestamp": standardize_date("timestamp")
    })
    )

    url_query_extractor = r"[?&]q=([^&]+)"
    extracted_query = regexp_extract(col("page_url"), url_query_extractor, 1)

    df_silver = (df_stage_1.withColumns({
        "is_event_valid": col("event_type").isin(allowed_events),
        "is_device_valid": col("device_type").isin(allowed_devices),
        "search_query": when(col("event_type") == "SEARCH",
                              when(extracted_query != "", extracted_query).otherwise(None)
                              ).otherwise(None),
        "_record_source": lit("WEB_CLICKSTREAM"),
        "_silver_processed_at": current_timestamp()
    }))
    df_finnal = df_silver.drop("ingesttime", "file_name", "_rescued_data", "timestamp")
    return df_finnal

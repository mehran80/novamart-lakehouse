from pyspark.sql.functions import col, coalesce, lit, current_timestamp, when, sum, to_date, avg
from pyspark.sql import DataFrame


def transfer_silver_to_gold_dim_customer(df_sql: DataFrame, df_crm: DataFrame) -> DataFrame:
    df_joined = (df_sql.join(df_crm, "customer_id", "left")
                 .select(
                     col("customer_id"),
                     col("first_name"),
                     col("last_name"),
                     col("full_name"),
                     col("email"),
                     col("cleaned_phone"),
                     col("address"),
                     col("city"),
                     col("region"),
                     col("customer_segment"),
                     col("verified_email"),
                     col("verified_phone"),
                     col("join_timestamp").alias("joined_at"),
                     col("last_campaign_engaged"),
                     coalesce(col("lifetime_value_estimate"), lit(0.0).cast("decimal(10,2)")).alias("lifetime_value_estimate"),
                     coalesce(col("loyalty_tier"), lit("Standard")).alias("loyalty_tier"),
                     coalesce(col("preferred_channel"), lit("Email")).alias("preffered_channel"),
                     col("marketing_opt_in"),
                     col("churn_risk_score"),
                     col("last_updated_crm")
                 ))
    df_finnal = df_joined.withColumns({"_gold_updated_at": current_timestamp()})
    return df_finnal


def transfer_silver_to_gold_dim_products(df: DataFrame) -> DataFrame:
    df_dim_products = (df
                        .select(
                            col("product_id"),
                            col("product_name"),
                            col("category"),
                            col("subcategory"),
                            col("brand"),
                            col("cost_price"),
                            col("unit_price"),
                            col("supplier_id"),
                            col("reorder_threshold"),
                            col("is_margin_positive"),
                            when((col("is_margin_positive") == True) & (col("unit_price") > 0),
                                 ((col("unit_price") - col("cost_price")) / col("unit_price")) * 100)
                            .otherwise(0.00).cast("decimal(5,2)").alias("gross_margin_percentage"),
                            col("created_date").alias("product_created_at")
                        ))
    df_finnal = df_dim_products.withColumns({"_gold_updated_at": current_timestamp()})
    return df_finnal


def transfer_silver_to_gold_fact_inventory(df: DataFrame) -> DataFrame:
    df_fact_inventory = (df
                          .select(
                              col("product_id"),
                              col("stock_quantity"),
                              col("is_reorder_needed"))
                          )
    df_finnal = df_fact_inventory.withColumns({"_gold_updated_at": current_timestamp()})
    return df_finnal


def transfer_silver_to_gold_fact_sales(df: DataFrame) -> DataFrame:
    df_fact_sales = (df
                      .filter((col("is_pricing_correct") == True) & (col("is_time_valid") == True))
                      .select(
                          col("customer_id"),
                          col("product_id"),
                          col("transaction_id"),
                          col("store_id"),
                          col("quantity"),
                          col("unit_price"),
                          col("discount_pct"),
                          col("payment_method"),
                          col("total_amount"),
                          col("transaction_timestamp"),
                          col("last_modified_timestamp")
                      ))

    df_finnal = df_fact_sales.withColumns({"_gold_updated_at": current_timestamp()})
    return df_finnal


def transfer_silver_to_gold_fact_clicks(df: DataFrame) -> DataFrame:
    df_fact_clicks = (df
                       .filter((col("is_event_valid") == True) & (col("is_device_valid") == True))
                       .select(
                           col("customer_id"),
                           col("product_id"),
                           col("session_id"),
                           col("event_id"),
                           col("event_type"),
                           col("device_type"),
                           col("page_url"),
                           col("search_query"),
                           col("event_timestamp").alias("event_at")
                       ))

    df_finnal = df_fact_clicks.withColumns({"_gold_updated_at": current_timestamp()})
    return df_finnal


def daily_sales(df_dim_cust: DataFrame, df_dim_prod: DataFrame, df_fct_sales: DataFrame) -> DataFrame:
    df_joined = (
        df_fct_sales
        .join(df_dim_cust, on="customer_id", how="left")
        .join(df_dim_prod, on="product_id", how="left")
    )

    df_agg_daily_sales = (df_joined
                           .groupBy(
                               to_date(col("transaction_timestamp")).alias("sales_date"),
                               col("category").alias("product_category"),
                               col("region").alias("customer_region")
                           )
                           .agg(
                               sum(col("total_amount")).cast("decimal(12,2)").alias("daily_sale"),
                               sum(col("quantity").cast("integer")).alias("daily_units_sold"),
                               avg(col("gross_margin_percentage").cast("decimal(5,2)")).alias("average_margin_pct")
                           )
                           )
    df_finnal = df_agg_daily_sales.withColumns({"_gold_updated_at": current_timestamp()})
    return df_finnal


def low_stock_alert(df_dim_prod: DataFrame, df_fct_clicks: DataFrame, df_fct_inven: DataFrame) -> DataFrame:
    df_add_to_cart = (df_fct_clicks.filter(col("event_type") == "ADD_TO_CART"))

    df_low_stock_alert = (df_add_to_cart
                           .join(df_fct_inven, on="product_id", how="inner")
                           .join(df_dim_prod, on="product_id", how="inner")
                           .filter(col("stock_quantity") <= col("reorder_threshold"))
                           .select(
                               col("event_id"),
                               col("session_id"),
                               col("product_id"),
                               col("stock_quantity").alias("available_stock"),
                               col("event_at").alias("alert_timestamp")
                           ))
    df_finnal = df_low_stock_alert.withColumns({"_gold_updated_at": current_timestamp()})
    return df_finnal

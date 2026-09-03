from pyspark.sql import Column, DataFrame
from pyspark.sql.types import StringType, BooleanType, IntegerType, DecimalType
from pyspark.sql.window import Window
import phonenumbers
import pandas as pd
from pyspark.sql.functions import (
    col, trim, regexp_replace, lower, when, coalesce, lit,
    to_timestamp, length, locate, initcap, upper, udf, pandas_udf, expr, row_number, desc,
    concat, current_timestamp, regexp_extract, try_to_timestamp
)


def clean_spaces(col_name: str) -> Column:
    return trim(regexp_replace(col(col_name), r'\s+', ' '))


def clean_email(col_name: str) -> Column:
    return lower(trim(col(col_name)))


def is_verified_email(col_name: str) -> Column:
    email_expression = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return when(col(col_name).isNotNull(), col(col_name).rlike(email_expression)).otherwise(None)


def clean_phone(col_name: str) -> Column:
    return regexp_replace(col(col_name), '[^0-9+]', '')


# phone verify helping function
# NOTE: this must be @pandas_udf (vectorized, receives a pandas Series), not
# plain @udf (scalar, receives one value at a time) -- the body operates on
# a pandas Series via .apply(), so the decorator has to match that contract.
@pandas_udf(returnType=BooleanType())
def is_verified_phone_fn(phone_series: pd.Series) -> pd.Series:
    def check_valid(num):
        if pd.isna(num) or not str(num).strip():
            return False

        num_str = str(num).strip()
        if num_str.startswith("00"):
            num_str = "+" + num_str[2:]

        try:
            parsed = phonenumbers.parse(num_str, "US")
            return phonenumbers.is_possible_number(parsed)
        except Exception:
            return False

    return pd.Series(phone_series.apply(check_valid))


# phone format helping function
# Same reason as above -- must be @pandas_udf, not @udf.
@pandas_udf(returnType=StringType())
def format_phone_e164(phone_series: pd.Series) -> pd.Series:
    def format_e164(num):
        if pd.isna(num) or not str(num).strip():
            return None

        num_str = str(num).strip()
        if num_str.startswith("00"):
            num_str = "+" + num_str[2:]

        try:
            parsed = phonenumbers.parse(num_str, "US")
            if phonenumbers.is_possible_number(parsed):
                return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
        except Exception:
            pass

        return None

    return pd.Series(phone_series.apply(format_e164))


# Integer Casting Helping Function
def cast_to_integer(col_name: str, default_val: int = 0) -> Column:
    return coalesce(col(col_name).cast(IntegerType()), lit(default_val).cast(IntegerType()))


# Decimal casting Helping Function
def cast_to_decimal(col_name: str, precision: int = 10, scale: int = 2, default_val: float = 0.0) -> Column:
    target_type = DecimalType(precision, scale)
    return coalesce(col(col_name).cast(target_type), lit(default_val).cast(target_type))


# Date standardization Function
def standardize_date(col_name: str) -> Column:
    date_formats = [
        "yyyy-MM-dd'T'HH:mm:ss",
        "yyyy-MM-dd"
    ]
    return coalesce(*[expr(f'try_to_timestamp({col_name},"{f}")') for f in date_formats])


# text_column clean Helping Function
def clean_text_standard(col_name: str, enforce_title_case: bool = True, is_acronym: bool = False) -> Column:
    cleaned_text = trim(regexp_replace(col(col_name), r'\s+', ' '))
    if is_acronym:
        return when((length(cleaned_text) <= 5) & (locate(" ", cleaned_text) == 0), upper(cleaned_text)).otherwise(initcap(cleaned_text))
    if enforce_title_case:
        return initcap(cleaned_text)
    return cleaned_text

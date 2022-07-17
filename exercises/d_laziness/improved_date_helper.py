"""
The issues with the date_helper.py module are these:

1. Spark's execution model is a lazy one: data won't be processed until an
action on the DataFrame triggers the execution of the query plan. That means
that data related errors won't be thrown until Runtime, at which point any
error handling using standard Python functionality is already out of scope.

2. Most column functions in pyspark.sql.functions are designed to transform
data the way you instruct the computer to, and if the instruction fails for a
specific value inside the column, Spark will automatically replace the value
with a null. This is by design, and it's actually quite wise: in the long term
you're better served with a process that continues until the end and simply
masks erroneous values. At a later stage, you could do some error-checking on
the data ("data quality tests") and gradually improve the handling process.
"""

import datetime as dt

import pyspark.sql.functions as psf
from pyspark.sql import DataFrame
from pyspark.sql.types import DateType


def parse_string_to_date(s: str) -> dt.date:
    if isinstance(s, dt.date):
        return s
    parse_date = dt.datetime.strptime
    try:
        return parse_date(s, "%Y%m").date()
    except ValueError:
        try:
            return parse_date(s, "%Y%m%d").date()
        except ValueError as err:
            raise ValueError("Bad date")


parse_string_to_date_spark = psf.udf(parse_string_to_date, DateType())


def parse_as_year_month(s):
    return psf.last_day(psf.to_date(s, "yyyyMM"))


def parse_as_year_month_day(s):
    return psf.to_date(s, "yyyyMMdd")


def cast_as_date(s):
    return psf.col(s).cast(DateType())


def parse_month_in_three_ways(df: DataFrame, column: str) -> DataFrame:
    df2 = (
        df.withColumn("yearmonth", parse_as_year_month(column))
        .withColumn("yearmonthday", parse_as_year_month_day(column))
        .withColumn("asdate", cast_as_date(column))
    )

    df2.show()

    new_cols = ("yearmonth", "yearmonthday", "asdate")
    return df2.withColumn("parsed", psf.coalesce(*new_cols)).drop(*new_cols)
    # The above is only as verbose to demonstrate the intermediate DataFrame:
    # one could simply write the following, which does not require explicitly
    # dropping the newly added columns. Note that Spark will likely optimize
    # the query plan to the plan below anyway.
    # parsers_to_try = (
    #     parse_as_year_month,
    #     parse_as_year_month_day,
    #     cast_as_date,
    # )
    # return df.withColumn(
    #     "parsed",
    #     psf.coalesce(*(f(column) for f in parsers_to_try)),
    # )


def data_quality_check(df: DataFrame, colname: str = "parsed") -> None:

    entries_with_nulls = df.filter(df[colname].isNull()).take(5)

    if entries_with_nulls:  # or (more verbose): len(entries_with_nulls) > 0
        print(entries_with_nulls)
        raise ValueError(
            "at least one record did not have a date format that we're expecting"
        )

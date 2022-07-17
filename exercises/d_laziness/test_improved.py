import pytest
from pyspark.sql import SparkSession

from .improved_date_helper import (
    data_quality_check,
    parse_string_to_date_spark,
    parse_month_in_three_ways,
)

spark = SparkSession.builder.getOrCreate()

# We're taking shortcuts here by not writing proper tests, merely printing the
# resulting DataFrames to stdout.


@pytest.mark.parametrize("column_to_transform", list("abc"))
def test_improved_date_parser_as_udf(column_to_transform: str):
    df = spark.createDataFrame(
        [
            # The first row will always cause errors to appear as none of these formats are expected.
            # Do you believe these are edge cases?
            ("2020-01-19", "2020-01", "2020"),
            ("20200119", "202001", "202"),
            #("20200119", "202001", "zever"),
        ],
        schema=("a", "b", "c"),
    )

    df = df.withColumn(
        f"{column_to_transform} converted",
        parse_string_to_date_spark(column_to_transform),
    )

    df.show()


@pytest.mark.parametrize("column_to_transform", list("abc"))
def test_improved_date_parser_without_udf(column_to_transform: str) -> None:
    df = spark.createDataFrame(
        [
            ("2020-01-19", "2020-01", "1970"),
            ("20200119", "202001", "1969"),
            ("20200119", "202001", "zever"),
        ],
        schema=("a", "b", "c"),
    )

    z = parse_month_in_three_ways(df, column_to_transform)
    z.show()
    assert False
    data_quality_check(z)

import pytest
from pyspark.sql import SparkSession

from src.data_quality.checks import (
    check_row_count,
    check_nulls,
    check_unique_key,
    check_null_keys,
    check_positive_values,
    check_foreign_key,
    check_grain,
)


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder
        .appName("AdventureWorksDQTests")
        .master("local[2]")
        .getOrCreate()
    )

    yield spark

    spark.stop()


def test_row_count(spark):
    df = spark.createDataFrame(
        [(1,), (2,), (3,)],
        ["id"]
    )

    result = check_row_count(df, 3)

    assert result["status"] == "PASSED"
    assert result["actual"] == 3


def test_null_check(spark):
    df = spark.createDataFrame(
        [(1,), (2,)],
        ["id"]
    )

    result = check_nulls(df, ["id"])

    assert result["status"] == "PASSED"


def test_unique_key(spark):
    df = spark.createDataFrame(
        [(1,), (2,), (3,)],
        ["id"]
    )

    result = check_unique_key(df, ["id"])

    assert result["status"] == "PASSED"
    assert result["duplicate_count"] == 0


def test_null_key(spark):
    df = spark.createDataFrame(
        [(1,), (2,)],
        ["id"]
    )

    result = check_null_keys(df, ["id"])

    assert result["status"] == "PASSED"


def test_positive_values(spark):
    df = spark.createDataFrame(
        [(10,), (20,), (30,)],
        ["quantity"]
    )

    result = check_positive_values(df, "quantity")

    assert result["status"] == "PASSED"
    assert result["invalid_count"] == 0


def test_foreign_key(spark):
    fact_df = spark.createDataFrame(
        [(1,), (2,), (3,)],
        ["customer_key"]
    )

    dimension_df = spark.createDataFrame(
        [(1,), (2,), (3,)],
        ["customer_key"]
    )

    result = check_foreign_key(
        fact_df,
        "customer_key",
        dimension_df,
        "customer_key"
    )

    assert result["status"] == "PASSED"
    assert result["orphan_count"] == 0


def test_grain(spark):
    df = spark.createDataFrame(
        [
            ("SO1", 1),
            ("SO1", 2),
            ("SO2", 1),
        ],
        ["order_number", "order_line_item"]
    )

    result = check_grain(
        df,
        ["order_number", "order_line_item"]
    )

    assert result["status"] == "PASSED"
    assert result["duplicate_grain"] == 0


def test_row_count_failure(spark):
    df = spark.createDataFrame(
        [(1,), (2,)],
        ["id"]
    )

    result = check_row_count(df, 3)

    assert result["status"] == "FAILED"
    assert result["actual"] == 2


def test_unique_key_failure(spark):
    df = spark.createDataFrame(
        [(1,), (1,), (2,)],
        ["id"]
    )

    result = check_unique_key(df, ["id"])

    assert result["status"] == "FAILED"
    assert result["duplicate_count"] == 1


def test_null_key_failure(spark):
    df = spark.createDataFrame(
        [(1,), (None,)],
        ["id"]
    )

    result = check_null_keys(df, ["id"])

    assert result["status"] == "FAILED"
    assert result["null_counts"]["id"] == 1


def test_positive_value_failure(spark):
    df = spark.createDataFrame(
        [(10,), (-5,), (20,)],
        ["quantity"]
    )

    result = check_positive_values(df, "quantity")

    assert result["status"] == "FAILED"
    assert result["invalid_count"] == 1


def test_foreign_key_failure(spark):
    fact_df = spark.createDataFrame(
        [(1,), (2,), (99,)],
        ["customer_key"]
    )

    dimension_df = spark.createDataFrame(
        [(1,), (2,), (3,)],
        ["customer_key"]
    )

    result = check_foreign_key(
        fact_df,
        "customer_key",
        dimension_df,
        "customer_key"
    )

    assert result["status"] == "FAILED"
    assert result["orphan_count"] == 1


def test_grain_failure(spark):
    df = spark.createDataFrame(
        [
            ("SO1", 1),
            ("SO1", 1),
            ("SO2", 1),
        ],
        ["order_number", "order_line_item"]
    )

    result = check_grain(
        df,
        ["order_number", "order_line_item"]
    )

    assert result["status"] == "FAILED"
    assert result["duplicate_grain"] == 1

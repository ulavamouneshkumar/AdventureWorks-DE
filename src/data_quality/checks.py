from pyspark.sql import DataFrame
from pyspark.sql.functions import col


# ============================================================
# ROW COUNT CHECK
# ============================================================

def check_row_count(
    df: DataFrame,
    expected_count: int
) -> dict:

    actual_count = df.count()

    passed = actual_count == expected_count

    return {
        "check": "ROW_COUNT",
        "expected": expected_count,
        "actual": actual_count,
        "status": "PASSED" if passed else "FAILED"
    }


# ============================================================
# NULL CHECK
# ============================================================

def check_nulls(
    df: DataFrame,
    columns: list[str]
) -> dict:

    null_counts = {}

    for column in columns:

        count = (
            df
            .filter(col(column).isNull())
            .count()
        )

        null_counts[column] = count

    failed_columns = {
        column: count
        for column, count in null_counts.items()
        if count > 0
    }

    return {
        "check": "NULL_CHECK",
        "columns_checked": columns,
        "null_counts": null_counts,
        "status": (
            "FAILED"
            if failed_columns
            else "PASSED"
        )
    }


# ============================================================
# UNIQUE KEY CHECK
# ============================================================

def check_unique_key(
    df: DataFrame,
    key_columns: list[str]
) -> dict:

    total_rows = df.count()

    distinct_keys = (
        df
        .select(*key_columns)
        .distinct()
        .count()
    )

    duplicate_count = (
        total_rows - distinct_keys
    )

    passed = duplicate_count == 0

    return {
        "check": "UNIQUE_KEY",
        "key_columns": key_columns,
        "total_rows": total_rows,
        "distinct_keys": distinct_keys,
        "duplicate_count": duplicate_count,
        "status": "PASSED" if passed else "FAILED"
    }


# ============================================================
# NULL KEY CHECK
# ============================================================

def check_null_keys(
    df: DataFrame,
    key_columns: list[str]
) -> dict:

    null_counts = {}

    for column in key_columns:

        count = (
            df
            .filter(col(column).isNull())
            .count()
        )

        null_counts[column] = count

    failed_columns = {
        column: count
        for column, count in null_counts.items()
        if count > 0
    }

    return {
        "check": "NULL_KEY",
        "key_columns": key_columns,
        "null_counts": null_counts,
        "status": (
            "FAILED"
            if failed_columns
            else "PASSED"
        )
    }


# ============================================================
# POSITIVE VALUE CHECK
# ============================================================

def check_positive_values(
    df: DataFrame,
    column: str
) -> dict:

    invalid_count = (
        df
        .filter(
            col(column) <= 0
        )
        .count()
    )

    passed = invalid_count == 0

    return {
        "check": "POSITIVE_VALUE",
        "column": column,
        "invalid_count": invalid_count,
        "status": "PASSED" if passed else "FAILED"
    }


# ============================================================
# FOREIGN KEY CHECK
# ============================================================

def check_foreign_key(
    fact_df: DataFrame,
    fact_key: str,
    dimension_df: DataFrame,
    dimension_key: str
) -> dict:

    orphan_count = (
        fact_df
        .select(fact_key)
        .distinct()
        .join(
            dimension_df
            .select(dimension_key)
            .distinct(),

            fact_df[fact_key]
            == dimension_df[dimension_key],

            "left_anti"
        )
        .count()
    )

    passed = orphan_count == 0

    return {
        "check": "FOREIGN_KEY",
        "fact_key": fact_key,
        "dimension_key": dimension_key,
        "orphan_count": orphan_count,
        "status": "PASSED" if passed else "FAILED"
    }


# ============================================================
# GRAIN CHECK
# ============================================================

def check_grain(
    df: DataFrame,
    grain_columns: list[str]
) -> dict:

    total_rows = df.count()

    distinct_grain = (
        df
        .select(*grain_columns)
        .distinct()
        .count()
    )

    duplicate_grain = (
        total_rows - distinct_grain
    )

    passed = duplicate_grain == 0

    return {
        "check": "GRAIN",
        "grain_columns": grain_columns,
        "total_rows": total_rows,
        "distinct_grain": distinct_grain,
        "duplicate_grain": duplicate_grain,
        "status": "PASSED" if passed else "FAILED"
    }
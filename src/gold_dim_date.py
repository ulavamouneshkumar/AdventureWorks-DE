# ============================================================
# GOLD DIM DATE - OPTIMIZED
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    year,
    quarter,
    month,
    monthname,
    dayofmonth,
    dayofweek,
    when,
    concat,
    lit,
    lpad,
)
from delta import configure_spark_with_delta_pip

from config import SILVER_TABLES, GOLD_TABLES


# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

builder = (
    SparkSession.builder
    .appName("AdventureWorksGoldDimDate")
    .master("local[*]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()


# ============================================================
# 2. READ SILVER CALENDAR
# ============================================================

silver_path = str(SILVER_TABLES["calendar"])

calendar = (
    spark.read
    .format("delta")
    .load(silver_path)
)


# ============================================================
# 3. CREATE GOLD DATE DIMENSION
# ============================================================

dim_date = (
    calendar
    .withColumn(
        "date_key",
        date_format(col("date"), "yyyyMMdd").cast("integer")
    )
    .withColumn("calendar_year", year(col("date")))
    .withColumn("calendar_quarter", quarter(col("date")))
    .withColumn("month", month(col("date")))
    .withColumn("month_name", monthname(col("date")))
    .withColumn("day", dayofmonth(col("date")))
    .withColumn("day_of_week", dayofweek(col("date")))
    .withColumn(
        "day_name",
        date_format(col("date"), "EEEE")
    )
    .withColumn(
        "financial_quarter",
        when(
            month(col("date")).isin(4, 5, 6),
            "Q1"
        )
        .when(
            month(col("date")).isin(7, 8, 9),
            "Q2"
        )
        .when(
            month(col("date")).isin(10, 11, 12),
            "Q3"
        )
        .otherwise("Q4")
    )
    .withColumn(
        "financial_year",
        when(
            month(col("date")) >= 4,
            concat(
                lit("FY"),
                year(col("date")),
                lit("-"),
                lpad(
                    ((year(col("date")) + 1) % 100).cast("string"),
                    2,
                    "0"
                )
            )
        )
        .otherwise(
            concat(
                lit("FY"),
                year(col("date")) - 1,
                lit("-"),
                lpad(
                    (year(col("date")) % 100).cast("string"),
                    2,
                    "0"
                )
            )
        )
    )
    .select(
        "date_key",
        "date",
        "calendar_year",
        "calendar_quarter",
        "month",
        "month_name",
        "day",
        "day_of_week",
        "day_name",
        "financial_year",
        "financial_quarter"
    )
)


# ============================================================
# 4. WRITE GOLD DELTA TABLE
# ============================================================

target_path = str(GOLD_TABLES["dim_date"])

(
    dim_date
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(target_path)
)


# ============================================================
# 5. READ BACK FOR VALIDATION
# ============================================================

# Cache because the same validation DataFrame is used by
# multiple actions below.
result = (
    spark.read
    .format("delta")
    .load(target_path)
    .cache()
)


# ============================================================
# 6. BASIC VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("GOLD DIM DATE")
print("=" * 70)

# One count is reused throughout validation.
total_rows = result.count()

print(f"Rows: {total_rows}")

print("\nSchema:")
result.printSchema()

print("\nSample:")
result.show(10, truncate=False)


# ============================================================
# 7. DATE RANGE
# ============================================================

print("\nDate range:")

(
    result
    .selectExpr(
        "min(date) as min_date",
        "max(date) as max_date"
    )
    .show()
)


# ============================================================
# 8. DATE KEY VALIDATION
# ============================================================

distinct_date_keys = (
    result
    .select("date_key")
    .distinct()
    .count()
)

print(
    f"Duplicate date keys : "
    f"{total_rows - distinct_date_keys}"
)


# ============================================================
# 9. FINANCIAL QUARTER VALIDATION
# ============================================================

print("\nFinancial quarter distribution:")

(
    result
    .groupBy(
        "financial_year",
        "financial_quarter"
    )
    .count()
    .orderBy(
        "financial_year",
        "financial_quarter"
    )
    .show(20, truncate=False)
)


# ============================================================
# 10. CLEANUP
# ============================================================

result.unpersist()
spark.stop()

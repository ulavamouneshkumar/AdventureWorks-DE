from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    year,
    quarter,
    month,
    monthname,
    dayofmonth,
    dayofweek,
    date_format,
    when,
    concat,
    lit,
    lpad
)

from delta import configure_spark_with_delta_pip


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

calendar = (
    spark.read
    .format("delta")
    .load("data/silver/calendar")
)


# ============================================================
# 3. CREATE GOLD DATE DIMENSION
# ============================================================

dim_date = (
    calendar

    # --------------------------------------------------------
    # DATE KEY
    # Example: 2015-01-10 → 20150110
    # --------------------------------------------------------

    .withColumn(
        "date_key",
        date_format(
            col("date"),
            "yyyyMMdd"
        ).cast("integer")
    )

    # --------------------------------------------------------
    # CALENDAR ATTRIBUTES
    # --------------------------------------------------------

    .withColumn(
        "calendar_year",
        year(col("date"))
    )

    .withColumn(
        "calendar_quarter",
        quarter(col("date"))
    )

    .withColumn(
        "month",
        month(col("date"))
    )

    .withColumn(
        "month_name",
        monthname(col("date"))
    )

    .withColumn(
        "day",
        dayofmonth(col("date"))
    )

    .withColumn(
        "day_of_week",
        dayofweek(col("date"))
    )

    .withColumn(
        "day_name",
        date_format(
            col("date"),
            "EEEE"
        )
    )

    # --------------------------------------------------------
    # FINANCIAL QUARTER
    #
    # Apr-Jun  → Q1
    # Jul-Sep  → Q2
    # Oct-Dec  → Q3
    # Jan-Mar  → Q4
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # FINANCIAL YEAR
    #
    # Apr 2015 - Mar 2016 → FY2015-16
    # Jan 2016            → FY2015-16
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # SELECT FINAL GOLD COLUMNS
    # --------------------------------------------------------

    .select(
        "date_key",
        "date",

        # Calendar
        "calendar_year",
        "calendar_quarter",

        # Month / Day
        "month",
        "month_name",
        "day",
        "day_of_week",
        "day_name",

        # Financial calendar
        "financial_year",
        "financial_quarter"
    )
)


# ============================================================
# 4. WRITE GOLD DELTA TABLE
# ============================================================

target_path = "data/gold/dim_date"

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

result = (
    spark.read
    .format("delta")
    .load(target_path)
)


# ============================================================
# 6. BASIC VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("GOLD DIM DATE")
print("=" * 70)

print(f"Rows: {result.count()}")

print("\nSchema:")
result.printSchema()

print("\nSample:")
result.show(10, truncate=False)


# ============================================================
# 7. DATE RANGE
# ============================================================

print("\nDate range:")

result.selectExpr(
    "min(date) as min_date",
    "max(date) as max_date"
).show()


# ============================================================
# 8. DATE KEY VALIDATION
# ============================================================

total_rows = result.count()

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
# 10. STOP SPARK
# ============================================================

spark.stop()
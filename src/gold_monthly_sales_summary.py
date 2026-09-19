from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum,
    round,
    when,
    broadcast
)


from delta import configure_spark_with_delta_pip
from config import GOLD_TABLES


# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

builder = (
    SparkSession.builder
    .appName("AdventureWorksGoldMonthlySalesSummary")
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
# 2. READ GOLD TABLES
# ============================================================

print("\n" + "=" * 70)
print("READING GOLD TABLES")
print("=" * 70)

sales_summary = (
    spark.read
    .format("delta")
    .load(str(GOLD_TABLES["sales_summary"]))
)

dim_date = (
    spark.read
    .format("delta")
    .load(str(GOLD_TABLES["dim_date"]))
)

print("Sales Summary and Date Dimension loaded successfully.")


# ============================================================
# 3. JOIN SALES SUMMARY WITH DATE DIMENSION
# ============================================================

print("\n" + "=" * 70)
print("JOINING SALES WITH DATE DIMENSION")
print("=" * 70)

sales_with_date = (
    sales_summary.alias("s")
    .join(
        broadcast(dim_date).alias("d"),
        col("s.date_key") == col("d.date_key"),
        "inner"
    )
    .select(
        col("s.date_key"),
        col("s.product_key"),
        col("s.territory_key"),

        col("d.date"),
        col("d.calendar_year"),
        col("d.calendar_quarter"),
        col("d.month"),
        col("d.month_name"),
        col("d.financial_year"),
        col("d.financial_quarter"),

        col("s.total_quantity"),
        col("s.total_sales"),
        col("s.total_cost"),
        col("s.total_profit"),
        col("s.return_quantity"),
        col("s.net_quantity")
    )
)


# ============================================================
# 4. AGGREGATE BY MONTH
# ============================================================

print("\n" + "=" * 70)
print("CREATING MONTHLY SALES SUMMARY")
print("=" * 70)

monthly_summary = (
    sales_with_date
    .groupBy(
        "calendar_year",
        "calendar_quarter",
        "month",
        "month_name",
        "financial_year",
        "financial_quarter"
    )
    .agg(
        sum("total_quantity").alias("total_quantity"),

        round(
            sum("total_sales"),
            2
        ).alias("total_sales"),

        round(
            sum("total_cost"),
            2
        ).alias("total_cost"),

        round(
            sum("total_profit"),
            2
        ).alias("total_profit"),

        sum("return_quantity").alias("return_quantity"),

        sum("net_quantity").alias("net_quantity")
    )
)


# ============================================================
# 5. CALCULATE PROFIT MARGIN
# ============================================================

monthly_summary = (
    monthly_summary
    .withColumn(
        "profit_margin",
        when(
            col("total_sales") > 0,
            round(
                (
                    col("total_profit")
                    / col("total_sales")
                ) * 100,
                2
            )
        ).otherwise(None)
    )
)


# ============================================================
# 6. CALCULATE RETURN RATE
# ============================================================

monthly_summary = (
    monthly_summary
    .withColumn(
        "return_rate",
        when(
            col("total_quantity") > 0,
            round(
                (
                    col("return_quantity")
                    / col("total_quantity")
                ) * 100,
                2
            )
        ).otherwise(None)
    )
)


# ============================================================
# 7. WRITE GOLD TABLE
# ============================================================

target_path = str(GOLD_TABLES["monthly_sales_summary"])

print("\n" + "=" * 70)
print("WRITING GOLD MONTHLY SUMMARY")
print("=" * 70)

(
    monthly_summary
    .write
    .format("delta")
    .mode("overwrite")
    .save(target_path)
)

print(f"Target : {target_path}")


# ============================================================
# 8. READ BACK
# ============================================================

result = (
    spark.read
    .format("delta")
    .load(target_path)
    .cache()
)


# ============================================================
# 9. DISPLAY RESULT
# ============================================================

print("\n" + "=" * 70)
print("GOLD MONTHLY SALES SUMMARY")
print("=" * 70)

total_rows = result.count()
print(f"Rows: {total_rows}")

print("\nSchema:")
result.printSchema()

print("\nData:")

(
    result
    .orderBy(
        "calendar_year",
        "month"
    )
    .show(
        50,
        truncate=False
    )
)


# ============================================================
# 10. KEY VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("MONTHLY SUMMARY VALIDATION")
print("=" * 70)

null_check = (
    result
    .selectExpr(
        """
        sum(
            case when calendar_year is null
            then 1 else 0 end
        ) as null_calendar_year
        """,

        """
        sum(
            case when month is null
            then 1 else 0 end
        ) as null_month
        """,

        """
        sum(
            case when financial_year is null
            then 1 else 0 end
        ) as null_financial_year
        """,

        """
        sum(
            case when financial_quarter is null
            then 1 else 0 end
        ) as null_financial_quarter
        """
    )
)

null_check.show()


# ============================================================
# 11. MONTH GRAIN VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("MONTH GRAIN VALIDATION")
print("=" * 70)

distinct_months = (
    result
    .select(
        "calendar_year",
        "month"
    )
    .distinct()
    .count()
)

print(f"Total rows              : {total_rows}")
print(f"Distinct Year + Month   : {distinct_months}")

print(
    f"Duplicate month records : "
    f"{total_rows - distinct_months}"
)


# ============================================================
# 12. BUSINESS TOTAL VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("BUSINESS TOTAL VALIDATION")
print("=" * 70)

result.selectExpr(
    "sum(total_quantity) as total_quantity",
    "round(sum(total_sales), 2) as total_sales",
    "round(sum(total_cost), 2) as total_cost",
    "round(sum(total_profit), 2) as total_profit",
    "sum(return_quantity) as total_returns",
    "sum(net_quantity) as total_net_quantity"
).show()


# ============================================================
# 13. FINANCIAL YEAR DISTRIBUTION
# ============================================================

print("\n" + "=" * 70)
print("FINANCIAL YEAR DISTRIBUTION")
print("=" * 70)

(
    result
    .groupBy(
        "financial_year"
    )
    .agg(
        sum("total_quantity").alias("total_quantity"),
        round(
            sum("total_sales"),
            2
        ).alias("total_sales"),
        round(
            sum("total_profit"),
            2
        ).alias("total_profit"),
        sum("return_quantity").alias("return_quantity")
    )
    .orderBy("financial_year")
    .show(
        truncate=False
    )
)


# ============================================================
# 14. FINANCIAL QUARTER DISTRIBUTION
# ============================================================

print("\n" + "=" * 70)
print("FINANCIAL QUARTER DISTRIBUTION")
print("=" * 70)

(
    result
    .groupBy(
        "financial_year",
        "financial_quarter"
    )
    .agg(
        sum("total_quantity").alias("total_quantity"),
        round(
            sum("total_sales"),
            2
        ).alias("total_sales"),
        round(
            sum("total_profit"),
            2
        ).alias("total_profit"),
        sum("return_quantity").alias("return_quantity")
    )
    .orderBy(
        "financial_year",
        "financial_quarter"
    )
    .show(
        truncate=False
    )
)


# ============================================================
# 15. FINAL VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("FINAL MONTHLY SUMMARY VALIDATION")
print("=" * 70)

expected_quantity = 84174
expected_sales = 24914583.66
expected_cost = 14456847.13
expected_profit = 10457736.53
expected_returns = 1828
expected_net_quantity = 82346


actual = (
    result
    .selectExpr(
        "sum(total_quantity) as quantity",
        "round(sum(total_sales), 2) as sales",
        "round(sum(total_cost), 2) as cost",
        "round(sum(total_profit), 2) as profit",
        "sum(return_quantity) as returns",
        "sum(net_quantity) as net_quantity"
    )
    .collect()[0]
)


quantity_passed = (
    actual["quantity"] == expected_quantity
)

sales_passed = (
    float(actual["sales"]) == expected_sales
)

cost_passed = (
    float(actual["cost"]) == expected_cost
)

profit_passed = (
    float(actual["profit"]) == expected_profit
)

returns_passed = (
    actual["returns"] == expected_returns
)

net_quantity_passed = (
    actual["net_quantity"] == expected_net_quantity
)


print(
    f"Quantity validation     : "
    f"{'PASSED' if quantity_passed else 'FAILED'}"
)

print(
    f"Sales validation        : "
    f"{'PASSED' if sales_passed else 'FAILED'}"
)

print(
    f"Cost validation         : "
    f"{'PASSED' if cost_passed else 'FAILED'}"
)

print(
    f"Profit validation       : "
    f"{'PASSED' if profit_passed else 'FAILED'}"
)

print(
    f"Returns validation      : "
    f"{'PASSED' if returns_passed else 'FAILED'}"
)

print(
    f"Net quantity validation : "
    f"{'PASSED' if net_quantity_passed else 'FAILED'}"
)


all_passed = (
    quantity_passed
    and sales_passed
    and cost_passed
    and profit_passed
    and returns_passed
    and net_quantity_passed
)


print(
    f"\nOverall Status : "
    f"{'PASSED' if all_passed else 'FAILED'}"
)


# ============================================================
# 16. STOP SPARK
# ============================================================

spark.stop()
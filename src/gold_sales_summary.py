from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum,
    coalesce,
    lit,
    round,
    when
)

from delta import configure_spark_with_delta_pip
from config import GOLD_TABLES


# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

builder = (
    SparkSession.builder
    .appName("AdventureWorksGoldSalesSummary")
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
# 2. READ GOLD FACT TABLES
# ============================================================

print("\n" + "=" * 70)
print("READING GOLD FACT TABLES")
print("=" * 70)

fact_sales = (
    spark.read
    .format("delta")
    .load(str(GOLD_TABLES["fact_sales"]))
)

fact_returns = (
    spark.read
    .format("delta")
    .load(str(GOLD_TABLES["fact_returns"]))
)



# ============================================================
# 3. AGGREGATE SALES
# ============================================================

print("\n" + "=" * 70)
print("AGGREGATING SALES")
print("=" * 70)

sales_summary = (
    fact_sales
    .groupBy(
        "date_key",
        "product_key",
        "territory_key"
    )
    .agg(
        sum("order_quantity").alias("total_quantity"),

        round(
            sum("sales_amount"),
            2
        ).alias("total_sales"),

        round(
            sum("cost_amount"),
            2
        ).alias("total_cost"),

        round(
            sum("profit_amount"),
            2
        ).alias("total_profit")
    )
)



# ============================================================
# 4. AGGREGATE RETURNS
# ============================================================

print("\n" + "=" * 70)
print("AGGREGATING RETURNS")
print("=" * 70)

returns_summary = (
    fact_returns
    .groupBy(
        "date_key",
        "product_key",
        "territory_key"
    )
    .agg(
        sum("return_quantity").alias("return_quantity")
    )
)



# ============================================================
# 5. FULL OUTER JOIN SALES + RETURNS
# ============================================================
#
# IMPORTANT:
#
# We use FULL OUTER JOIN because some return records may exist
# for a date/product/territory combination that does not exist
# in the sales aggregation.
#
# A LEFT JOIN would lose those returns.
#
# ============================================================

print("\n" + "=" * 70)
print("COMBINING SALES + RETURNS")
print("=" * 70)

business_summary = (
    sales_summary.alias("s")
    .join(
        returns_summary.alias("r"),
        (
            (col("s.date_key") == col("r.date_key"))
            &
            (col("s.product_key") == col("r.product_key"))
            &
            (col("s.territory_key") == col("r.territory_key"))
        ),
        "full_outer"
    )
    .select(

        # ----------------------------------------------------
        # Keys
        # ----------------------------------------------------

        coalesce(
            col("s.date_key"),
            col("r.date_key")
        ).alias("date_key"),

        coalesce(
            col("s.product_key"),
            col("r.product_key")
        ).alias("product_key"),

        coalesce(
            col("s.territory_key"),
            col("r.territory_key")
        ).alias("territory_key"),

        # ----------------------------------------------------
        # Sales measures
        # ----------------------------------------------------

        coalesce(
            col("s.total_quantity"),
            lit(0)
        ).alias("total_quantity"),

        coalesce(
            col("s.total_sales"),
            lit(0)
        ).alias("total_sales"),

        coalesce(
            col("s.total_cost"),
            lit(0)
        ).alias("total_cost"),

        coalesce(
            col("s.total_profit"),
            lit(0)
        ).alias("total_profit"),

        # ----------------------------------------------------
        # Return measure
        # ----------------------------------------------------

        coalesce(
            col("r.return_quantity"),
            lit(0)
        ).alias("return_quantity")
    )
)


# ============================================================
# 6. CALCULATE NET QUANTITY
# ============================================================

business_summary = (
    business_summary
    .withColumn(
        "net_quantity",
        col("total_quantity") - col("return_quantity")
    )
)


# ============================================================
# 7. CALCULATE RETURN RATE
# ============================================================
#
# Return rate:
#
#     Return Quantity
#     ---------------
#     Sales Quantity
#
# multiplied by 100.
#
# For rows where sales quantity is 0, return rate is NULL
# because division by zero is not meaningful.
#
# ============================================================

business_summary = (
    business_summary
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
# 8. WRITE GOLD SALES SUMMARY
# ============================================================

target_path = str(GOLD_TABLES["sales_summary"])

print("\n" + "=" * 70)
print("WRITING GOLD SALES SUMMARY")
print("=" * 70)

(
    business_summary
    .write
    .format("delta")
    .mode("overwrite")
    .save(target_path)
)

print(f"Target : {target_path}")


# ============================================================
# 9. READ BACK FROM DELTA
# ============================================================

result = (
    spark.read
    .format("delta")
    .load(target_path)
    .cache()
)


# ============================================================
# 10. BASIC INFORMATION
# ============================================================

print("\n" + "=" * 70)
print("GOLD SALES SUMMARY")
print("=" * 70)

total_rows = result.count()

print(f"Rows: {total_rows}")

print("\nSchema:")
result.printSchema()

print("\nSample:")
result.show(10, truncate=False)


# ============================================================
# 11. SUMMARY KEY VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("SUMMARY KEY VALIDATION")
print("=" * 70)

result.selectExpr(

    """
    sum(
        case when date_key is null
        then 1 else 0 end
    ) as null_date_keys
    """,

    """
    sum(
        case when product_key is null
        then 1 else 0 end
    ) as null_product_keys
    """,

    """
    sum(
        case when territory_key is null
        then 1 else 0 end
    ) as null_territory_keys
    """

).show()


# ============================================================
# 12. SUMMARY GRAIN VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("SUMMARY GRAIN VALIDATION")
print("=" * 70)

distinct_summary_grain = (
    result
    .select(
        "date_key",
        "product_key",
        "territory_key"
    )
    .distinct()
    .count()
)

print(f"Total rows                  : {total_rows}")
print(f"Distinct summary grain     : {distinct_summary_grain}")
print(
    f"Duplicate summary grains   : "
    f"{total_rows - distinct_summary_grain}"
)


# ============================================================
# 13. BUSINESS SUMMARY VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("BUSINESS SUMMARY")
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
# 14. SOURCE FACT RECONCILIATION
# ============================================================
#
# This is especially important.
#
# We compare the Gold summary against the original Gold facts.
#
# ============================================================

print("\n" + "=" * 70)
print("SOURCE FACT RECONCILIATION")
print("=" * 70)

sales_totals = (
    fact_sales
    .selectExpr(
        "sum(order_quantity) as quantity",
        "round(sum(sales_amount), 2) as sales",
        "round(sum(cost_amount), 2) as cost",
        "round(sum(profit_amount), 2) as profit"
    )
    .collect()[0]
)

return_totals = (
    fact_returns
    .selectExpr(
        "sum(return_quantity) as returns"
    )
    .collect()[0]
)

summary_totals = (
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


print("\nSales reconciliation:")

print(
    f"Quantity - Source : {sales_totals['quantity']}"
)

print(
    f"Quantity - Summary: {summary_totals['quantity']}"
)

print(
    f"Sales - Source    : {sales_totals['sales']}"
)

print(
    f"Sales - Summary   : {summary_totals['sales']}"
)

print(
    f"Cost - Source     : {sales_totals['cost']}"
)

print(
    f"Cost - Summary    : {summary_totals['cost']}"
)

print(
    f"Profit - Source   : {sales_totals['profit']}"
)

print(
    f"Profit - Summary  : {summary_totals['profit']}"
)


print("\nReturns reconciliation:")

print(
    f"Returns - Source  : {return_totals['returns']}"
)

print(
    f"Returns - Summary : {summary_totals['returns']}"
)


# ============================================================
# 15. RECONCILIATION STATUS
# ============================================================

sales_reconciliation = (
    sales_totals["quantity"] == summary_totals["quantity"]
    and
    float(sales_totals["sales"])
    == float(summary_totals["sales"])
    and
    float(sales_totals["cost"])
    == float(summary_totals["cost"])
    and
    float(sales_totals["profit"])
    == float(summary_totals["profit"])
)

return_reconciliation = (
    return_totals["returns"]
    == summary_totals["returns"]
)


print("\n" + "=" * 70)
print("RECONCILIATION STATUS")
print("=" * 70)

print(
    f"Sales reconciliation   : "
    f"{'PASSED' if sales_reconciliation else 'FAILED'}"
)

print(
    f"Returns reconciliation : "
    f"{'PASSED' if return_reconciliation else 'FAILED'}"
)


# ============================================================
# 16. EXPECTED NET QUANTITY
# ============================================================

expected_net_quantity = (
    sales_totals["quantity"]
    - return_totals["returns"]
)

actual_net_quantity = summary_totals["net_quantity"]

print(
    f"Expected net quantity  : "
    f"{expected_net_quantity}"
)

print(
    f"Actual net quantity    : "
    f"{actual_net_quantity}"
)

print(
    f"Net quantity validation: "
    f"{'PASSED' if expected_net_quantity == actual_net_quantity else 'FAILED'}"
)


# ============================================================
# 17. FINAL RESULT
# ============================================================

all_validations_passed = (
    total_rows == distinct_summary_grain
    and sales_reconciliation
    and return_reconciliation
    and expected_net_quantity == actual_net_quantity
)

print("\n" + "=" * 70)
print("FINAL SALES SUMMARY VALIDATION")
print("=" * 70)

print(
    f"Overall Status : "
    f"{'PASSED' if all_validations_passed else 'FAILED'}"
)


# ============================================================
# 18. STOP SPARK
# ============================================================

spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
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
    .appName("AdventureWorksGoldCustomerPerformance")
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

dim_customer = (
    spark.read
    .format("delta")
    .load(str(GOLD_TABLES["dim_customer"]))
)

print(f"Fact Sales rows     : {fact_sales.count()}")
print(f"Fact Returns rows   : {fact_returns.count()}")
print(f"Dim Customer rows   : {dim_customer.count()}")


# ============================================================
# 3. AGGREGATE SALES BY CUSTOMER
# ============================================================

print("\n" + "=" * 70)
print("AGGREGATING SALES BY CUSTOMER")
print("=" * 70)

sales_by_customer = (
    fact_sales
    .groupBy("customer_key")
    .agg(
        countDistinct("order_number").alias(
            "total_orders"
        ),

        sum("order_quantity").alias(
            "total_quantity"
        ),

        round(
            sum("sales_amount"),
            2
        ).alias(
            "total_sales"
        ),

        round(
            sum("cost_amount"),
            2
        ).alias(
            "total_cost"
        ),

        round(
            sum("profit_amount"),
            2
        ).alias(
            "total_profit"
        )
    )
)

print(
    f"Customers with sales : "
    f"{sales_by_customer.count()}"
)


# ============================================================
# 4. AGGREGATE RETURNS BY CUSTOMER
# ============================================================
#
# IMPORTANT:
# fact_returns does not contain customer_key.
#
# Therefore, returns cannot be directly attributed to customers
# using the current source data.
#
# We will NOT incorrectly assign returns to customers.
#
# ============================================================

print("\n" + "=" * 70)
print("CUSTOMER RETURN ANALYSIS")
print("=" * 70)

print(
    "Customer-level returns cannot be calculated from the "
    "current fact_returns table because it has no customer_key."
)

print(
    "Return metrics will therefore be NULL for customer-level "
    "analysis rather than being incorrectly attributed."
)


# ============================================================
# 5. START FROM COMPLETE CUSTOMER DIMENSION
# ============================================================

print("\n" + "=" * 70)
print("COMBINING CUSTOMER + SALES")
print("=" * 70)

customer_metrics = (
    dim_customer
    .select("customer_key")
    .distinct()
    .alias("c")
    .join(
        sales_by_customer.alias("s"),
        col("c.customer_key")
        == col("s.customer_key"),
        "left"
    )
    .select(
        col("c.customer_key"),

        coalesce(
            col("s.total_orders"),
            lit(0)
        ).alias("total_orders"),

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
        ).alias("total_profit")
    )
)


# ============================================================
# 6. CALCULATE PROFIT MARGIN
# ============================================================

customer_metrics = (
    customer_metrics
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
# 7. CUSTOMER SEGMENT
# ============================================================
#
# Segment based on total sales:
#
# >= 100,000 -> High Value
# >= 25,000  -> Medium Value
# < 25,000   -> Low Value
#
# These thresholds are business rules for this project and
# can be changed later.
#
# ============================================================

customer_metrics = (
    customer_metrics
    .withColumn(
        "customer_segment",
        when(
            col("total_sales") >= 100000,
            "High Value"
        )
        .when(
            col("total_sales") >= 25000,
            "Medium Value"
        )
        .otherwise(
            "Low Value"
        )
    )
)


# ============================================================
# 8. JOIN CUSTOMER ATTRIBUTES
# ============================================================

print("\n" + "=" * 70)
print("ADDING CUSTOMER ATTRIBUTES")
print("=" * 70)

customer_performance = (
    customer_metrics.alias("m")
    .join(
        dim_customer.alias("c"),
        col("m.customer_key")
        == col("c.customer_key"),
        "left"
    )
    .select(
        col("m.customer_key"),

        col("c.prefix"),
        col("c.first_name"),
        col("c.last_name"),
        col("c.full_name"),

        col("c.birth_date"),
        col("c.marital_status"),
        col("c.gender"),

        col("c.email_address"),
        col("c.annual_income"),
        col("c.total_children"),

        col("c.education_level"),
        col("c.occupation"),
        col("c.home_owner"),

        col("m.total_orders"),
        col("m.total_quantity"),

        col("m.total_sales"),
        col("m.total_cost"),
        col("m.total_profit"),

        col("m.profit_margin"),

        lit(None).cast("long").alias(
            "return_quantity"
        ),

        lit(None).cast("long").alias(
            "net_quantity"
        ),

        lit(None).cast("double").alias(
            "return_rate"
        ),

        col("m.customer_segment")
    )
)


# ============================================================
# 9. WRITE GOLD TABLE
# ============================================================

target_path = str(GOLD_TABLES["customer_performance"])

print("\n" + "=" * 70)
print("WRITING GOLD CUSTOMER PERFORMANCE")
print("=" * 70)

(
    customer_performance
    .write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .save(target_path)
)

print(f"Target : {target_path}")


# ============================================================
# 10. READ BACK
# ============================================================

result = (
    spark.read
    .format("delta")
    .load(target_path)
)


# ============================================================
# 11. DISPLAY RESULT
# ============================================================

print("\n" + "=" * 70)
print("GOLD CUSTOMER PERFORMANCE")
print("=" * 70)

total_rows = result.count()

print(f"Rows: {total_rows}")

print("\nSchema:")

result.printSchema()

print("\nTop Customers by Sales:")

(
    result
    .orderBy(
        col("total_sales").desc()
    )
    .show(
        10,
        truncate=False
    )
)


# ============================================================
# 12. CUSTOMER KEY VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("CUSTOMER KEY VALIDATION")
print("=" * 70)

distinct_customer_keys = (
    result
    .select("customer_key")
    .distinct()
    .count()
)

null_customer_keys = (
    result
    .filter(
        col("customer_key").isNull()
    )
    .count()
)

print(
    f"Total customers         : "
    f"{total_rows}"
)

print(
    f"Distinct customer keys  : "
    f"{distinct_customer_keys}"
)

print(
    f"Null customer keys      : "
    f"{null_customer_keys}"
)

print(
    f"Duplicate customer keys : "
    f"{total_rows - distinct_customer_keys}"
)


# ============================================================
# 13. CUSTOMER ATTRIBUTE VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("CUSTOMER ATTRIBUTE VALIDATION")
print("=" * 70)

null_full_names = (
    result
    .filter(
        col("full_name").isNull()
    )
    .count()
)

null_annual_income = (
    result
    .filter(
        col("annual_income").isNull()
    )
    .count()
)

print(
    f"Null full names     : "
    f"{null_full_names}"
)

print(
    f"Null annual income  : "
    f"{null_annual_income}"
)


# ============================================================
# 14. CUSTOMERS WITHOUT SALES
# ============================================================

print("\n" + "=" * 70)
print("CUSTOMERS WITHOUT SALES")
print("=" * 70)

customers_without_sales = (
    result
    .filter(
        col("total_orders") == 0
    )
    .count()
)

print(
    f"Customers without sales : "
    f"{customers_without_sales}"
)


# ============================================================
# 15. CUSTOMER BUSINESS METRICS
# ============================================================

print("\n" + "=" * 70)
print("CUSTOMER BUSINESS METRICS")
print("=" * 70)

result.selectExpr(
    "sum(total_orders) as total_orders",

    "sum(total_quantity) as total_quantity",

    "round(sum(total_sales), 2) as total_sales",

    "round(sum(total_cost), 2) as total_cost",

    "round(sum(total_profit), 2) as total_profit"
).show()


# ============================================================
# 16. CUSTOMER SEGMENT DISTRIBUTION
# ============================================================

print("\n" + "=" * 70)
print("CUSTOMER SEGMENT DISTRIBUTION")
print("=" * 70)

(
    result
    .groupBy(
        "customer_segment"
    )
    .agg(
        countDistinct(
            "customer_key"
        ).alias(
            "customer_count"
        ),

        sum(
            "total_quantity"
        ).alias(
            "total_quantity"
        ),

        round(
            sum("total_sales"),
            2
        ).alias(
            "total_sales"
        ),

        round(
            sum("total_profit"),
            2
        ).alias(
            "total_profit"
        )
    )
    .orderBy(
        col("total_sales").desc()
    )
    .show(
        truncate=False
    )
)


# ============================================================
# 17. TOP CUSTOMERS BY PROFIT
# ============================================================

print("\n" + "=" * 70)
print("TOP CUSTOMERS BY PROFIT")
print("=" * 70)

(
    result
    .orderBy(
        col("total_profit").desc()
    )
    .select(
        "customer_key",
        "full_name",
        "total_orders",
        "total_quantity",
        "total_sales",
        "total_profit",
        "profit_margin",
        "customer_segment"
    )
    .show(
        10,
        truncate=False
    )
)


# ============================================================
# 18. TOP CUSTOMERS BY ORDER COUNT
# ============================================================

print("\n" + "=" * 70)
print("TOP CUSTOMERS BY ORDER COUNT")
print("=" * 70)

(
    result
    .orderBy(
        col("total_orders").desc()
    )
    .select(
        "customer_key",
        "full_name",
        "total_orders",
        "total_quantity",
        "total_sales",
        "total_profit",
        "customer_segment"
    )
    .show(
        10,
        truncate=False
    )
)


# ============================================================
# 19. SOURCE FACT RECONCILIATION
# ============================================================

print("\n" + "=" * 70)
print("SOURCE FACT RECONCILIATION")
print("=" * 70)

source = (
    fact_sales
    .selectExpr(
        "sum(order_quantity) as quantity",

        "round(sum(sales_amount), 2) as sales",

        "round(sum(cost_amount), 2) as cost",

        "round(sum(profit_amount), 2) as profit"
    )
    .collect()[0]
)

summary = (
    result
    .selectExpr(
        "sum(total_quantity) as quantity",

        "round(sum(total_sales), 2) as sales",

        "round(sum(total_cost), 2) as cost",

        "round(sum(total_profit), 2) as profit"
    )
    .collect()[0]
)


print("\nSales reconciliation:")

print(
    f"Quantity - Source : "
    f"{source['quantity']}"
)

print(
    f"Quantity - Summary: "
    f"{summary['quantity']}"
)

print(
    f"Sales - Source    : "
    f"{source['sales']}"
)

print(
    f"Sales - Summary   : "
    f"{summary['sales']}"
)

print(
    f"Cost - Source     : "
    f"{source['cost']}"
)

print(
    f"Cost - Summary    : "
    f"{summary['cost']}"
)

print(
    f"Profit - Source   : "
    f"{source['profit']}"
)

print(
    f"Profit - Summary  : "
    f"{summary['profit']}"
)


# ============================================================
# 20. RECONCILIATION STATUS
# ============================================================

sales_passed = (
    source["quantity"]
    == summary["quantity"]
    and
    float(source["sales"])
    == float(summary["sales"])
    and
    float(source["cost"])
    == float(summary["cost"])
    and
    float(source["profit"])
    == float(summary["profit"])
)


print("\n" + "=" * 70)
print("RECONCILIATION STATUS")
print("=" * 70)

print(
    f"Sales reconciliation : "
    f"{'PASSED' if sales_passed else 'FAILED'}"
)


# ============================================================
# 21. FINAL VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("FINAL CUSTOMER PERFORMANCE VALIDATION")
print("=" * 70)

expected_customers = 18148

customer_count_passed = (
    total_rows == expected_customers
)

key_validation_passed = (
    total_rows == distinct_customer_keys
    and
    null_customer_keys == 0
)

attribute_validation_passed = (
    null_full_names == 0
)

all_passed = (
    customer_count_passed
    and key_validation_passed
    and attribute_validation_passed
    and sales_passed
)


print(
    f"Customer count validation : "
    f"{'PASSED' if customer_count_passed else 'FAILED'}"
)

print(
    f"Key validation            : "
    f"{'PASSED' if key_validation_passed else 'FAILED'}"
)

print(
    f"Attribute validation      : "
    f"{'PASSED' if attribute_validation_passed else 'FAILED'}"
)

print(
    f"Sales reconciliation      : "
    f"{'PASSED' if sales_passed else 'FAILED'}"
)

print(
    f"\nOverall Status : "
    f"{'PASSED' if all_passed else 'FAILED'}"
)


# ============================================================
# 22. STOP SPARK
# ============================================================

spark.stop()
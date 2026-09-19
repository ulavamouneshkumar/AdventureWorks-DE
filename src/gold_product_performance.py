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
    .appName("AdventureWorksGoldProductPerformance")
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

dim_product = (
    spark.read
    .format("delta")
    .load(str(GOLD_TABLES["dim_product"]))
)



# ============================================================
# 3. AGGREGATE SALES BY PRODUCT
# ============================================================

print("\n" + "=" * 70)
print("AGGREGATING SALES BY PRODUCT")
print("=" * 70)

sales_by_product = (
    fact_sales
    .groupBy("product_key")
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
# 4. AGGREGATE RETURNS BY PRODUCT
# ============================================================

print("\n" + "=" * 70)
print("AGGREGATING RETURNS BY PRODUCT")
print("=" * 70)

returns_by_product = (
    fact_returns
    .groupBy("product_key")
    .agg(
        sum("return_quantity").alias("return_quantity")
    )
)



# ============================================================
# 5. START FROM COMPLETE PRODUCT DIMENSION
# ============================================================
#
# IMPORTANT:
#
# dim_product contains all 293 products.
#
# Therefore, it must be the LEFT side of the joins.
#
# This ensures products with no sales still appear.
#
# ============================================================

print("\n" + "=" * 70)
print("COMBINING PRODUCT + SALES + RETURNS")
print("=" * 70)

product_metrics = (
    dim_product
    .select("product_key")
    .distinct()
    .alias("p")
    .join(
        sales_by_product.alias("s"),
        col("p.product_key") == col("s.product_key"),
        "left"
    )
    .join(
        returns_by_product.alias("r"),
        col("p.product_key") == col("r.product_key"),
        "left"
    )
    .select(
        col("p.product_key"),

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

        coalesce(
            col("r.return_quantity"),
            lit(0)
        ).alias("return_quantity")
    )
)


# ============================================================
# 6. CALCULATE NET QUANTITY
# ============================================================

product_metrics = (
    product_metrics
    .withColumn(
        "net_quantity",
        col("total_quantity")
        - col("return_quantity")
    )
)


# ============================================================
# 7. CALCULATE PROFIT MARGIN
# ============================================================

product_metrics = (
    product_metrics
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
# 8. CALCULATE RETURN RATE
# ============================================================

product_metrics = (
    product_metrics
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
# 9. JOIN PRODUCT ATTRIBUTES
# ============================================================

print("\n" + "=" * 70)
print("ADDING PRODUCT ATTRIBUTES")
print("=" * 70)

product_performance = (
    product_metrics.alias("m")
    .join(
        dim_product.alias("p"),
        col("m.product_key") == col("p.product_key"),
        "left"
    )
    .select(
        col("m.product_key"),

        col("p.product_sku"),
        col("p.product_name"),
        col("p.model_name"),

        col("p.product_subcategory_key"),
        col("p.subcategory_name"),

        col("p.product_category_key"),
        col("p.category_name"),

        col("p.product_color"),
        col("p.product_size"),
        col("p.product_style"),

        col("p.product_cost"),
        col("p.product_price"),

        col("m.total_quantity"),
        col("m.total_sales"),
        col("m.total_cost"),
        col("m.total_profit"),

        col("m.return_quantity"),
        col("m.net_quantity"),

        col("m.profit_margin"),
        col("m.return_rate")
    )
)


# ============================================================
# 10. WRITE GOLD TABLE
# ============================================================

target_path = str(GOLD_TABLES["product_performance"])

print("\n" + "=" * 70)
print("WRITING GOLD PRODUCT PERFORMANCE")
print("=" * 70)

(
    product_performance
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(target_path)
)

print(f"Target : {target_path}")


# ============================================================
# 11. READ BACK
# ============================================================

result = (
    spark.read
    .format("delta")
    .load(target_path)
    .cache()
)


# ============================================================
# 12. DISPLAY RESULT
# ============================================================

print("\n" + "=" * 70)
print("GOLD PRODUCT PERFORMANCE")
print("=" * 70)

total_rows = result.count()

print(f"Rows: {total_rows}")

print("\nSchema:")
result.printSchema()

print("\nTop Products by Sales:")

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
# 13. PRODUCT KEY VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("PRODUCT KEY VALIDATION")
print("=" * 70)

distinct_product_keys = (
    result
    .select("product_key")
    .distinct()
    .count()
)

null_product_keys = (
    result
    .filter(
        col("product_key").isNull()
    )
    .count()
)

print(f"Total products         : {total_rows}")
print(f"Distinct product keys  : {distinct_product_keys}")
print(f"Null product keys      : {null_product_keys}")

print(
    f"Duplicate product keys : "
    f"{total_rows - distinct_product_keys}"
)


# ============================================================
# 14. PRODUCT DIMENSION VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("PRODUCT DIMENSION VALIDATION")
print("=" * 70)

null_product_names = (
    result
    .filter(
        col("product_name").isNull()
    )
    .count()
)

null_categories = (
    result
    .filter(
        col("category_name").isNull()
    )
    .count()
)

null_subcategories = (
    result
    .filter(
        col("subcategory_name").isNull()
    )
    .count()
)

print(
    f"Null product names : "
    f"{null_product_names}"
)

print(
    f"Null categories    : "
    f"{null_categories}"
)

print(
    f"Null subcategories : "
    f"{null_subcategories}"
)


# ============================================================
# 15. PRODUCTS WITHOUT SALES
# ============================================================

print("\n" + "=" * 70)
print("PRODUCTS WITHOUT SALES")
print("=" * 70)

products_without_sales = (
    result
    .filter(
        col("total_quantity") == 0
    )
    .count()
)

print(
    f"Products without sales : "
    f"{products_without_sales}"
)


# ============================================================
# 16. PRODUCTS WITHOUT RETURNS
# ============================================================

print("\n" + "=" * 70)
print("PRODUCTS WITHOUT RETURNS")
print("=" * 70)

products_without_returns = (
    result
    .filter(
        col("return_quantity") == 0
    )
    .count()
)

print(
    f"Products without returns : "
    f"{products_without_returns}"
)


# ============================================================
# 17. BUSINESS METRICS
# ============================================================

print("\n" + "=" * 70)
print("PRODUCT BUSINESS METRICS")
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
# 18. CATEGORY PERFORMANCE
# ============================================================

print("\n" + "=" * 70)
print("CATEGORY PERFORMANCE")
print("=" * 70)

(
    result
    .groupBy(
        "product_category_key",
        "category_name"
    )
    .agg(
        sum("total_quantity").alias(
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
        ),

        sum("return_quantity").alias(
            "return_quantity"
        ),

        sum("net_quantity").alias(
            "net_quantity"
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
# 19. SUBCATEGORY PERFORMANCE
# ============================================================

print("\n" + "=" * 70)
print("SUBCATEGORY PERFORMANCE")
print("=" * 70)

(
    result
    .groupBy(
        "product_subcategory_key",
        "subcategory_name"
    )
    .agg(
        sum("total_quantity").alias(
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
        ),

        sum("return_quantity").alias(
            "return_quantity"
        )
    )
    .orderBy(
        col("total_sales").desc()
    )
    .show(
        20,
        truncate=False
    )
)


# ============================================================
# 20. HIGH RETURN RATE PRODUCTS
# ============================================================

print("\n" + "=" * 70)
print("HIGH RETURN RATE PRODUCTS")
print("=" * 70)

(
    result
    .filter(
        col("total_quantity") > 0
    )
    .orderBy(
        col("return_rate").desc()
    )
    .select(
        "product_key",
        "product_name",
        "category_name",
        "total_quantity",
        "return_quantity",
        "return_rate"
    )
    .show(
        10,
        truncate=False
    )
)


# ============================================================
# 21. TOP PRODUCTS BY PROFIT
# ============================================================

print("\n" + "=" * 70)
print("TOP PRODUCTS BY PROFIT")
print("=" * 70)

(
    result
    .orderBy(
        col("total_profit").desc()
    )
    .select(
        "product_key",
        "product_name",
        "category_name",
        "total_sales",
        "total_profit",
        "profit_margin"
    )
    .show(
        10,
        truncate=False
    )
)


# ============================================================
# 22. SOURCE FACT RECONCILIATION
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

returns_source = (
    fact_returns
    .selectExpr(
        "sum(return_quantity) as returns"
    )
    .collect()[0]
)

summary = (
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


print("\nReturns reconciliation:")

print(
    f"Returns - Source  : "
    f"{returns_source['returns']}"
)

print(
    f"Returns - Summary : "
    f"{summary['returns']}"
)


# ============================================================
# 23. RECONCILIATION STATUS
# ============================================================

sales_passed = (
    source["quantity"] == summary["quantity"]
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

returns_passed = (
    returns_source["returns"]
    == summary["returns"]
)


print("\n" + "=" * 70)
print("RECONCILIATION STATUS")
print("=" * 70)

print(
    f"Sales reconciliation   : "
    f"{'PASSED' if sales_passed else 'FAILED'}"
)

print(
    f"Returns reconciliation : "
    f"{'PASSED' if returns_passed else 'FAILED'}"
)


# ============================================================
# 24. FINAL VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("FINAL PRODUCT PERFORMANCE VALIDATION")
print("=" * 70)

expected_products = 293

product_count_passed = (
    total_rows == expected_products
)

key_validation_passed = (
    total_rows == distinct_product_keys
    and
    null_product_keys == 0
)

dimension_validation_passed = (
    null_product_names == 0
    and
    null_categories == 0
    and
    null_subcategories == 0
)


all_passed = (
    product_count_passed
    and key_validation_passed
    and dimension_validation_passed
    and sales_passed
    and returns_passed
)


print(
    f"Product count validation : "
    f"{'PASSED' if product_count_passed else 'FAILED'}"
)

print(
    f"Key validation           : "
    f"{'PASSED' if key_validation_passed else 'FAILED'}"
)

print(
    f"Dimension validation     : "
    f"{'PASSED' if dimension_validation_passed else 'FAILED'}"
)

print(
    f"Sales reconciliation     : "
    f"{'PASSED' if sales_passed else 'FAILED'}"
)

print(
    f"Returns reconciliation   : "
    f"{'PASSED' if returns_passed else 'FAILED'}"
)

print(
    f"\nOverall Status : "
    f"{'PASSED' if all_passed else 'FAILED'}"
)


# ============================================================
# 25. STOP SPARK
# ============================================================

spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, broadcast
from delta import configure_spark_with_delta_pip
from config import SILVER_TABLES, GOLD_TABLES


# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

builder = (
    SparkSession.builder
    .appName("AdventureWorksGoldFactSales")
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
# 2. READ SILVER SALES
# ============================================================

sales_path = str(SILVER_TABLES["sales"])

sales = (
    spark.read
    .format("delta")
    .load(sales_path)
)


# ============================================================
# 3. READ GOLD DIMENSIONS
# ============================================================

dim_product_path = str(GOLD_TABLES["dim_product"])
dim_date_path = str(GOLD_TABLES["dim_date"])
dim_customer_path = str(GOLD_TABLES["dim_customer"])
dim_territory_path = str(GOLD_TABLES["dim_territory"])


dim_product = (
    spark.read
    .format("delta")
    .load(dim_product_path)
)

dim_date = (
    spark.read
    .format("delta")
    .load(dim_date_path)
)

dim_customer = (
    spark.read
    .format("delta")
    .load(dim_customer_path)
)

dim_territory = (
    spark.read
    .format("delta")
    .load(dim_territory_path)
)


# ============================================================
# 4. JOIN SALES → PRODUCT
# ============================================================

sales_with_product = (
    sales.alias("s")
    .join(
        broadcast(
            dim_product.select(
                "product_key",
                "product_price",
                "product_cost"
            )
        ).alias("p"),
        col("s.product_key") == col("p.product_key"),
        "left"
    )
)


# ============================================================
# 5. JOIN SALES → DATE
# ============================================================

sales_with_date = (
    sales_with_product
    .join(
        broadcast(
            dim_date.select(
                "date",
                "date_key"
            )
        ).alias("d"),
        col("s.order_date") == col("d.date"),
        "left"
    )
)


# ============================================================
# 6. JOIN SALES → CUSTOMER
# ============================================================

sales_with_customer = (
    sales_with_date
    .join(
        broadcast(
            dim_customer.select(
                "customer_key"
            )
        ).alias("c"),
        col("s.customer_key") == col("c.customer_key"),
        "left"
    )
)


# ============================================================
# 7. JOIN SALES → TERRITORY
# ============================================================

sales_enriched = (
    sales_with_customer
    .join(
        broadcast(
            dim_territory.select(
                "territory_key"
            )
        ).alias("t"),
        col("s.territory_key") == col("t.territory_key"),
        "left"
    )
)


# ============================================================
# 8. CREATE FACT SALES
# ============================================================

fact_sales = (
    sales_enriched

    .select(
        # Dimension keys
        col("d.date_key").alias("date_key"),
        col("s.product_key").alias("product_key"),
        col("s.customer_key").alias("customer_key"),
        col("s.territory_key").alias("territory_key"),

        # Transaction identifiers
        col("s.order_number").alias("order_number"),
        col("s.order_line_item").alias("order_line_item"),

        # Dates
        col("s.order_date").alias("order_date"),
        col("s.stock_date").alias("stock_date"),

        # Quantity
        col("s.order_quantity").alias("order_quantity"),

        # Product pricing
        col("p.product_price").alias("unit_price"),
        col("p.product_cost").alias("unit_cost")
    )

    # --------------------------------------------------------
    # Revenue
    # --------------------------------------------------------

    .withColumn(
        "sales_amount",
        round(
            col("order_quantity") * col("unit_price"),
            2
        )
    )

    # --------------------------------------------------------
    # Cost
    # --------------------------------------------------------

    .withColumn(
        "cost_amount",
        round(
            col("order_quantity") * col("unit_cost"),
            2
        )
    )

    # --------------------------------------------------------
    # Profit
    # --------------------------------------------------------

    .withColumn(
        "profit_amount",
        round(
            col("sales_amount") - col("cost_amount"),
            2
        )
    )
)


# ============================================================
# 9. WRITE GOLD FACT
# ============================================================

target_path = str(GOLD_TABLES["fact_sales"])

(
    fact_sales
    .write
    .format("delta")
    .mode("overwrite")
    .save(target_path)
)


# ============================================================
# 10. READ BACK FOR VALIDATION
# ============================================================

result = (
    spark.read
    .format("delta")
    .load(target_path)
    .cache()
)


# ============================================================
# 11. BASIC INFORMATION
# ============================================================

print("\n" + "=" * 70)
print("GOLD FACT SALES")
print("=" * 70)

total_rows = result.count()

print(f"Rows: {total_rows}")

print("\nSchema:")
result.printSchema()

print("\nSample:")
result.show(10, truncate=False)


# ============================================================
# 12. GRAIN VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("FACT SALES GRAIN VALIDATION")
print("=" * 70)

distinct_grain = (
    result
    .select(
        "order_number",
        "order_line_item"
    )
    .distinct()
    .count()
)

print(f"Total rows                  : {total_rows}")
print(f"Distinct Order + Line Item  : {distinct_grain}")
print(
    f"Duplicate grain combinations: "
    f"{total_rows - distinct_grain}"
)


# ============================================================
# 13. NULL KEY VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("FACT SALES KEY VALIDATION")
print("=" * 70)

result.selectExpr(
    "sum(case when date_key is null then 1 else 0 end) as null_date_keys",
    "sum(case when product_key is null then 1 else 0 end) as null_product_keys",
    "sum(case when customer_key is null then 1 else 0 end) as null_customer_keys",
    "sum(case when territory_key is null then 1 else 0 end) as null_territory_keys",
    "sum(case when order_number is null then 1 else 0 end) as null_order_numbers"
).show()


# ============================================================
# 14. BUSINESS MEASURE VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("BUSINESS MEASURE VALIDATION")
print("=" * 70)

result.selectExpr(
    "sum(order_quantity) as total_quantity",
    "round(sum(sales_amount), 2) as total_sales",
    "round(sum(cost_amount), 2) as total_cost",
    "round(sum(profit_amount), 2) as total_profit"
).show()


# ============================================================
# 15. NEGATIVE / NULL MEASURE VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("MEASURE QUALITY VALIDATION")
print("=" * 70)

result.selectExpr(
    "sum(case when order_quantity <= 0 then 1 else 0 end) as non_positive_quantity",
    "sum(case when unit_price is null then 1 else 0 end) as null_unit_price",
    "sum(case when unit_cost is null then 1 else 0 end) as null_unit_cost",
    "sum(case when sales_amount is null then 1 else 0 end) as null_sales_amount",
    "sum(case when cost_amount is null then 1 else 0 end) as null_cost_amount",
    "sum(case when profit_amount is null then 1 else 0 end) as null_profit_amount"
).show()


# ============================================================
# 16. DATE RANGE
# ============================================================

print("\n" + "=" * 70)
print("FACT SALES DATE RANGE")
print("=" * 70)

result.selectExpr(
    "min(order_date) as min_order_date",
    "max(order_date) as max_order_date",
    "min(stock_date) as min_stock_date",
    "max(stock_date) as max_stock_date"
).show()


# ============================================================
# 17. STOP SPARK
# ============================================================

spark.stop()

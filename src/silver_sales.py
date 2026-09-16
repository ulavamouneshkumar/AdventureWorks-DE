from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, trim
from delta import configure_spark_with_delta_pip
from config import BRONZE_TABLES, SILVER_TABLES

# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

builder = (
    SparkSession.builder
    .appName("AdventureWorksSilverSales")
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
# 2. READ BRONZE SALES
# ============================================================
bronze_path = str(BRONZE_TABLES["sales"])
sales = (
    spark.read
    .format("delta")
    .load(bronze_path)
)


# ============================================================
# 3. TRANSFORM SALES
# ============================================================

silver_sales = (
    sales

    # --------------------------------------------------------
    # Rename columns
    # --------------------------------------------------------

    .withColumnRenamed(
        "OrderDate",
        "order_date"
    )
    .withColumnRenamed(
        "StockDate",
        "stock_date"
    )
    .withColumnRenamed(
        "OrderNumber",
        "order_number"
    )
    .withColumnRenamed(
        "ProductKey",
        "product_key"
    )
    .withColumnRenamed(
        "CustomerKey",
        "customer_key"
    )
    .withColumnRenamed(
        "TerritoryKey",
        "territory_key"
    )
    .withColumnRenamed(
        "OrderLineItem",
        "order_line_item"
    )
    .withColumnRenamed(
        "OrderQuantity",
        "order_quantity"
    )

    # --------------------------------------------------------
    # Convert dates
    # --------------------------------------------------------

    .withColumn(
        "order_date",
        to_date(
            col("order_date"),
            "M/d/yyyy"
        )
    )
    .withColumn(
        "stock_date",
        to_date(
            col("stock_date"),
            "M/d/yyyy"
        )
    )

    # --------------------------------------------------------
    # Clean string columns
    # --------------------------------------------------------

    .withColumn(
        "order_number",
        trim(col("order_number"))
    )
)


# ============================================================
# 4. WRITE SILVER DELTA
# ============================================================

target_path = str(SILVER_TABLES["sales"])

(
    silver_sales
    .write
    .format("delta")
    .mode("overwrite")
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
print("SILVER SALES")
print("=" * 70)

total_rows = result.count()

print(f"Rows: {total_rows}")


# ============================================================
# 7. SCHEMA
# ============================================================

print("\nSchema:")
result.printSchema()


# ============================================================
# 8. SAMPLE
# ============================================================

print("\nSample:")
result.show(10, truncate=False)


# ============================================================
# 9. SALES GRAIN VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("SALES GRAIN VALIDATION")
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

print(f"Total rows                    : {total_rows}")
print(f"Distinct Order + Line Item   : {distinct_grain}")
print(
    f"Duplicate grain combinations : "
    f"{total_rows - distinct_grain}"
)


# ============================================================
# 10. DATE QUALITY VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("DATE QUALITY VALIDATION")
print("=" * 70)

stock_after_order = (
    result
    .filter(
        col("stock_date") > col("order_date")
    )
    .count()
)

stock_before_order = (
    result
    .filter(
        col("stock_date") < col("order_date")
    )
    .count()
)

same_date = (
    result
    .filter(
        col("stock_date") == col("order_date")
    )
    .count()
)

print(
    f"StockDate > OrderDate : "
    f"{stock_after_order}"
)

print(
    f"StockDate < OrderDate : "
    f"{stock_before_order}"
)

print(
    f"StockDate = OrderDate : "
    f"{same_date}"
)


# ============================================================
# 11. FOREIGN KEY NULL VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("KEY NULL VALIDATION")
print("=" * 70)

result.selectExpr(
    "sum(case when product_key is null then 1 else 0 end) as null_product_keys",
    "sum(case when customer_key is null then 1 else 0 end) as null_customer_keys",
    "sum(case when territory_key is null then 1 else 0 end) as null_territory_keys",
    "sum(case when order_number is null then 1 else 0 end) as null_order_numbers"
).show()


# ============================================================
# 12. SOURCE YEAR VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("SOURCE YEAR VALIDATION")
print("=" * 70)

(
    result
    .groupBy("_source_year")
    .count()
    .orderBy("_source_year")
    .show()
)


# ============================================================
# 13. FINAL VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("FINAL SILVER SALES VALIDATION")
print("=" * 70)

if total_rows == 56046:
    print("Row count validation : PASSED")
else:
    print("Row count validation : FAILED")


if total_rows == distinct_grain:
    print("Sales grain validation : PASSED")
else:
    print("Sales grain validation : FAILED")


# ============================================================
# 14. STOP SPARK
# ============================================================

spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip


# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

builder = (
    SparkSession.builder
    .appName("AdventureWorksGoldFactReturns")
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
# 2. READ SILVER RETURNS
# ============================================================

returns = (
    spark.read
    .format("delta")
    .load("data/silver/returns")
)


# ============================================================
# 3. READ GOLD DIMENSIONS
# ============================================================

dim_date = (
    spark.read
    .format("delta")
    .load("data/gold/dim_date")
)

dim_product = (
    spark.read
    .format("delta")
    .load("data/gold/dim_product")
)

dim_territory = (
    spark.read
    .format("delta")
    .load("data/gold/dim_territory")
)


# ============================================================
# 4. JOIN RETURNS → DATE
# ============================================================

returns_with_date = (
    returns.alias("r")
    .join(
        dim_date.select(
            "date",
            "date_key"
        ).alias("d"),
        col("r.return_date") == col("d.date"),
        "left"
    )
)


# ============================================================
# 5. JOIN RETURNS → PRODUCT
# ============================================================

returns_with_product = (
    returns_with_date
    .join(
        dim_product.select(
            "product_key"
        ).alias("p"),
        col("r.product_key") == col("p.product_key"),
        "left"
    )
)


# ============================================================
# 6. JOIN RETURNS → TERRITORY
# ============================================================

returns_enriched = (
    returns_with_product
    .join(
        dim_territory.select(
            "territory_key"
        ).alias("t"),
        col("r.territory_key") == col("t.territory_key"),
        "left"
    )
)


# ============================================================
# 7. CREATE FACT RETURNS
# ============================================================

fact_returns = (
    returns_enriched
    .select(
        col("d.date_key").alias("date_key"),
        col("r.product_key").alias("product_key"),
        col("r.territory_key").alias("territory_key"),
        col("r.return_date").alias("return_date"),
        col("r.return_quantity").alias("return_quantity")
    )
)


# ============================================================
# 8. WRITE GOLD FACT
# ============================================================

target_path = "data/gold/fact_returns"

(
    fact_returns
    .write
    .format("delta")
    .mode("overwrite")
    .save(target_path)
)


# ============================================================
# 9. READ BACK FOR VALIDATION
# ============================================================

result = (
    spark.read
    .format("delta")
    .load(target_path)
)


# ============================================================
# 10. BASIC INFORMATION
# ============================================================

print("\n" + "=" * 70)
print("GOLD FACT RETURNS")
print("=" * 70)

total_rows = result.count()

print(f"Rows: {total_rows}")

print("\nSchema:")
result.printSchema()

print("\nSample:")
result.show(10, truncate=False)


# ============================================================
# 11. GRAIN VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("RETURNS GRAIN VALIDATION")
print("=" * 70)

distinct_grain = (
    result
    .select(
        "return_date",
        "territory_key",
        "product_key"
    )
    .distinct()
    .count()
)

print(f"Total rows              : {total_rows}")
print(f"Distinct return grain  : {distinct_grain}")
print(
    f"Duplicate combinations : "
    f"{total_rows - distinct_grain}"
)


# ============================================================
# 12. KEY NULL VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("RETURNS KEY VALIDATION")
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
    """,

    """
    sum(
        case when return_date is null
        then 1 else 0 end
    ) as null_return_dates
    """
).show()


# ============================================================
# 13. RETURN QUANTITY VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("RETURN QUANTITY VALIDATION")
print("=" * 70)

result.selectExpr(
    "sum(return_quantity) as total_return_quantity",

    """
    sum(
        case when return_quantity <= 0
        then 1 else 0 end
    ) as non_positive_returns
    """,

    """
    sum(
        case when return_quantity is null
        then 1 else 0 end
    ) as null_return_quantities
    """
).show()


# ============================================================
# 14. RETURN DATE RANGE
# ============================================================

print("\n" + "=" * 70)
print("RETURN DATE RANGE")
print("=" * 70)

result.selectExpr(
    "min(return_date) as min_return_date",
    "max(return_date) as max_return_date"
).show()


# ============================================================
# 15. TOTAL RETURNS BY YEAR
# ============================================================

print("\n" + "=" * 70)
print("RETURNS BY YEAR")
print("=" * 70)

(
    result
    .join(
        dim_date.select(
            "date_key",
            "calendar_year"
        ),
        on="date_key",
        how="left"
    )
    .groupBy("calendar_year")
    .sum("return_quantity")
    .withColumnRenamed(
        "sum(return_quantity)",
        "return_quantity"
    )
    .orderBy("calendar_year")
    .show()
)


# ============================================================
# 16. STOP SPARK
# ============================================================

spark.stop()
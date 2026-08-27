from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from delta import configure_spark_with_delta_pip


# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

builder = (
    SparkSession.builder
    .appName("AdventureWorksSilverReturns")
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
# 2. READ BRONZE RETURNS
# ============================================================

returns = (
    spark.read
    .format("delta")
    .load("data/bronze/returns")
)


# ============================================================
# 3. TRANSFORM RETURNS
# ============================================================

silver_returns = (
    returns

    # Rename columns
    .withColumnRenamed(
        "ReturnDate",
        "return_date"
    )
    .withColumnRenamed(
        "TerritoryKey",
        "territory_key"
    )
    .withColumnRenamed(
        "ProductKey",
        "product_key"
    )
    .withColumnRenamed(
        "ReturnQuantity",
        "return_quantity"
    )

    # Convert ReturnDate string → DATE
    .withColumn(
        "return_date",
        to_date(
            col("return_date"),
            "M/d/yyyy"
        )
    )
)


# ============================================================
# 4. WRITE SILVER DELTA
# ============================================================

target_path = "data/silver/returns"

(
    silver_returns
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
# 6. VALIDATE
# ============================================================

print("\n" + "=" * 70)
print("SILVER RETURNS")
print("=" * 70)

print(f"Rows: {result.count()}")

print("\nSchema:")
result.printSchema()

print("\nSample:")
result.show(10, truncate=False)


# ============================================================
# 7. DATE RANGE
# ============================================================

print("\nReturn date range:")

result.selectExpr(
    "min(return_date) as min_return_date",
    "max(return_date) as max_return_date"
).show()


# ============================================================
# 8. STOP SPARK
# ============================================================

spark.stop()
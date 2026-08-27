from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from delta import configure_spark_with_delta_pip


# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

builder = (
    SparkSession.builder
    .appName("AdventureWorksSilverProductCategories")
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
# 2. READ BRONZE
# ============================================================

categories = (
    spark.read
    .format("delta")
    .load("data/bronze/product_categories")
)


# ============================================================
# 3. TRANSFORM
# ============================================================

silver_categories = (
    categories

    .withColumnRenamed(
        "ProductCategoryKey",
        "product_category_key"
    )
    .withColumnRenamed(
        "CategoryName",
        "category_name"
    )

    .withColumn(
        "category_name",
        trim(col("category_name"))
    )
)


# ============================================================
# 4. WRITE SILVER
# ============================================================

(
    silver_categories
    .write
    .format("delta")
    .mode("overwrite")
    .save("data/silver/product_categories")
)


# ============================================================
# 5. VALIDATE
# ============================================================

result = (
    spark.read
    .format("delta")
    .load("data/silver/product_categories")
)

print("\n" + "=" * 70)
print("SILVER PRODUCT CATEGORIES")
print("=" * 70)

print(f"Rows: {result.count()}")

result.printSchema()

result.show(truncate=False)


# ============================================================
# 6. STOP
# ============================================================

spark.stop()
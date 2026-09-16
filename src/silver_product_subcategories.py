from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from delta import configure_spark_with_delta_pip
from config import BRONZE_TABLES, SILVER_TABLES

# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

builder = (
    SparkSession.builder
    .appName("AdventureWorksSilverProductSubcategories")
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
bronze_path = str(BRONZE_TABLES["product_subcategories"])
subcategories = (
    spark.read
    .format("delta")
    .load(bronze_path)
)


# ============================================================
# 3. TRANSFORM
# ============================================================

silver_subcategories = (
    subcategories

    .withColumnRenamed(
        "ProductSubcategoryKey",
        "product_subcategory_key"
    )
    .withColumnRenamed(
        "ProductCategoryKey",
        "product_category_key"
    )
    .withColumnRenamed(
        "SubcategoryName",
        "subcategory_name"
    )

    .withColumn(
        "subcategory_name",
        trim(col("subcategory_name"))
    )
)


# ============================================================
# 4. WRITE SILVER
# ============================================================
target_path = str(SILVER_TABLES["product_subcategories"])
(
    silver_subcategories
    .write
    .format("delta")
    .mode("overwrite")
    .save(target_path)
)


# ============================================================
# 5. VALIDATE
# ============================================================

result = (
    spark.read
    .format("delta")
    .load(target_path)
)

print("\n" + "=" * 70)
print("SILVER PRODUCT SUBCATEGORIES")
print("=" * 70)

print(f"Rows: {result.count()}")

result.printSchema()

result.show(truncate=False)


# ============================================================
# 6. STOP
# ============================================================

spark.stop()
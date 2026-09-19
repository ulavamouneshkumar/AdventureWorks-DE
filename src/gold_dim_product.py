from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, broadcast
from delta import configure_spark_with_delta_pip
from config import SILVER_TABLES, GOLD_TABLES


# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

builder = (
    SparkSession.builder
    .appName("AdventureWorksGoldDimProduct")
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
# 2. READ SILVER TABLES
# ============================================================

products_path = str(SILVER_TABLES["products"])
subcategories_path = str(SILVER_TABLES["product_subcategories"])
categories_path = str(SILVER_TABLES["product_categories"])


products = (
    spark.read
    .format("delta")
    .load(products_path)
)

subcategories = (
    spark.read
    .format("delta")
    .load(subcategories_path)
)

categories = (
    spark.read
    .format("delta")
    .load(categories_path)
)


# ============================================================
# 3. JOIN PRODUCT → SUBCATEGORY
# ============================================================

product_with_subcategory = (
    products.alias("p")
    .join(
        broadcast(subcategories).alias("s"),
        col("p.product_subcategory_key")
        == col("s.product_subcategory_key"),
        "left"
    )
)


# ============================================================
# 4. JOIN → CATEGORY
# ============================================================

product_with_category = (
    product_with_subcategory
    .join(
        broadcast(categories).alias("c"),
        col("s.product_category_key")
        == col("c.product_category_key"),
        "left"
    )
)


# ============================================================
# 5. CREATE GOLD DIMENSION
# ============================================================

dim_product = (
    product_with_category

    .select(
        col("p.product_key"),
        col("p.product_sku"),
        col("p.product_name"),
        col("p.model_name"),
        col("p.product_description"),
        col("p.product_color"),
        col("p.product_size"),
        col("p.product_style"),
        col("p.product_cost"),
        col("p.product_price"),

        col("s.product_subcategory_key"),
        col("s.subcategory_name"),

        col("c.product_category_key"),
        col("c.category_name")
    )

    # Trim business-facing strings
    .withColumn(
        "product_sku",
        trim(col("product_sku"))
    )
    .withColumn(
        "product_name",
        trim(col("product_name"))
    )
    .withColumn(
        "model_name",
        trim(col("model_name"))
    )
    .withColumn(
        "subcategory_name",
        trim(col("subcategory_name"))
    )
    .withColumn(
        "category_name",
        trim(col("category_name"))
    )
)


# ============================================================
# 6. WRITE GOLD DELTA
# ============================================================

target_path = str(GOLD_TABLES["dim_product"])


(
    dim_product
    .write
    .format("delta")
    .mode("overwrite")
    .save(target_path)
)


# ============================================================
# 7. READ BACK FOR VALIDATION
# ============================================================

result = (
    spark.read
    .format("delta")
    .load(target_path)
    .cache()
)


# ============================================================
# 8. BASIC VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("GOLD DIM PRODUCT")
print("=" * 70)

total_products = result.count()

print(f"Rows: {total_products}")


print("\nSchema:")
result.printSchema()


print("\nSample:")
result.show(10, truncate=False)


# ============================================================
# 9. PRODUCT KEY VALIDATION
# ============================================================

distinct_products = (
    result
    .select("product_key")
    .distinct()
    .count()
)


print("\n" + "=" * 70)
print("PRODUCT KEY VALIDATION")
print("=" * 70)

print(f"Total products         : {total_products}")
print(f"Distinct product keys  : {distinct_products}")
print(
    f"Duplicate product keys : "
    f"{total_products - distinct_products}"
)


# ============================================================
# 10. HIERARCHY VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("PRODUCT HIERARCHY VALIDATION")
print("=" * 70)


orphan_subcategories = (
    result
    .filter(col("subcategory_name").isNull())
    .count()
)


orphan_categories = (
    result
    .filter(col("category_name").isNull())
    .count()
)


print(
    f"Products without subcategory : "
    f"{orphan_subcategories}"
)

print(
    f"Products without category    : "
    f"{orphan_categories}"
)


# ============================================================
# 11. CATEGORY DISTRIBUTION
# ============================================================

print("\n" + "=" * 70)
print("PRODUCTS BY CATEGORY")
print("=" * 70)


(
    result
    .groupBy("category_name")
    .count()
    .orderBy("category_name")
    .show(truncate=False)
)


# ============================================================
# 12. STOP SPARK
# ============================================================

spark.stop()

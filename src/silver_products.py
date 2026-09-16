from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from pyspark.sql.types import DecimalType
from delta import configure_spark_with_delta_pip
from config import BRONZE_TABLES, SILVER_TABLES

# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

builder = (
    SparkSession.builder
    .appName("AdventureWorksSilverProducts")
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
# 2. READ BRONZE PRODUCTS
# ============================================================

bronze_path = str(BRONZE_TABLES["products"])

products = (
    spark.read
    .format("delta")
    .load(bronze_path)
)


# ============================================================
# 3. TRANSFORM PRODUCTS
# ============================================================

silver_products = (
    products

    # --------------------------------------------------------
    # Rename columns
    # --------------------------------------------------------

    .withColumnRenamed(
        "ProductKey",
        "product_key"
    )
    .withColumnRenamed(
        "ProductSubcategoryKey",
        "product_subcategory_key"
    )
    .withColumnRenamed(
        "ProductSKU",
        "product_sku"
    )
    .withColumnRenamed(
        "ProductName",
        "product_name"
    )
    .withColumnRenamed(
        "ModelName",
        "model_name"
    )
    .withColumnRenamed(
        "ProductDescription",
        "product_description"
    )
    .withColumnRenamed(
        "ProductColor",
        "product_color"
    )
    .withColumnRenamed(
        "ProductSize",
        "product_size"
    )
    .withColumnRenamed(
        "ProductStyle",
        "product_style"
    )
    .withColumnRenamed(
        "ProductCost",
        "product_cost"
    )
    .withColumnRenamed(
        "ProductPrice",
        "product_price"
    )

    # --------------------------------------------------------
    # Convert monetary values
    # --------------------------------------------------------

    .withColumn(
        "product_cost",
        col("product_cost").cast(
            DecimalType(12, 4)
        )
    )
    .withColumn(
        "product_price",
        col("product_price").cast(
            DecimalType(12, 4)
        )
    )

    # --------------------------------------------------------
    # Trim text columns
    # --------------------------------------------------------

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
        "product_description",
        trim(col("product_description"))
    )
    .withColumn(
        "product_color",
        trim(col("product_color"))
    )
    .withColumn(
        "product_size",
        trim(col("product_size"))
    )
    .withColumn(
        "product_style",
        trim(col("product_style"))
    )
)


# ============================================================
# 4. WRITE SILVER DELTA TABLE
# ============================================================

target_path = str(SILVER_TABLES["products"])

(
    silver_products
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
print("SILVER PRODUCTS")
print("=" * 70)

print(f"Rows: {result.count()}")

print("\nSchema:")
result.printSchema()

print("\nSample:")
result.show(10, truncate=False)


# ============================================================
# 7. STOP SPARK
# ============================================================

spark.stop()
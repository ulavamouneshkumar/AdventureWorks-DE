from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from delta import configure_spark_with_delta_pip


# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

builder = (
    SparkSession.builder
    .appName("AdventureWorksSilverTerritories")
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

territories = (
    spark.read
    .format("delta")
    .load("data/bronze/territories")
)


# ============================================================
# 3. TRANSFORM
# ============================================================

silver_territories = (
    territories

    .withColumnRenamed(
        "SalesTerritoryKey",
        "sales_territory_key"
    )
    .withColumnRenamed(
        "Region",
        "region"
    )
    .withColumnRenamed(
        "Country",
        "country"
    )
    .withColumnRenamed(
        "Continent",
        "continent"
    )

    .withColumn(
        "region",
        trim(col("region"))
    )
    .withColumn(
        "country",
        trim(col("country"))
    )
    .withColumn(
        "continent",
        trim(col("continent"))
    )
)


# ============================================================
# 4. WRITE SILVER
# ============================================================

(
    silver_territories
    .write
    .format("delta")
    .mode("overwrite")
    .save("data/silver/territories")
)


# ============================================================
# 5. VALIDATE
# ============================================================

result = (
    spark.read
    .format("delta")
    .load("data/silver/territories")
)

print("\n" + "=" * 70)
print("SILVER TERRITORIES")
print("=" * 70)

print(f"Rows: {result.count()}")

print("\nSchema:")
result.printSchema()

print("\nData:")
result.show(truncate=False)


# ============================================================
# 6. STOP
# ============================================================

spark.stop()
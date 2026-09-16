from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip
from config import SILVER_TABLES, GOLD_TABLES
# ============================================================
# Spark Session
# ============================================================

builder = (
    SparkSession.builder
    .appName("AdventureWorksGoldDimTerritory")
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
# Read Silver Territory
# ============================================================
silver_path = str(SILVER_TABLES["territories"])
territories = (
    spark.read
    .format("delta")
    .load(silver_path)
)

# ============================================================
# Create Gold Dimension
# ============================================================

dim_territory = (
    territories
    .select(
        col("sales_territory_key").alias("territory_key"),
        "region",
        "country",
        "continent"
    )
)

# ============================================================
# Write Gold
# ============================================================

target_path = str(GOLD_TABLES["dim_territory"])

(
    dim_territory
    .write
    .format("delta")
    .mode("overwrite")
    .save(target_path)
)

# ============================================================
# Validation
# ============================================================

result = (
    spark.read
    .format("delta")
    .load(target_path)
)

print("\n" + "=" * 70)
print("GOLD DIM TERRITORY")
print("=" * 70)

print(f"Rows: {result.count()}")

print("\nSchema:")
result.printSchema()

print("\nData:")
result.show(truncate=False)

# Territory Key Validation

total_rows = result.count()

distinct_keys = (
    result
    .select("territory_key")
    .distinct()
    .count()
)

print("\n" + "=" * 70)
print("TERRITORY KEY VALIDATION")
print("=" * 70)

print(f"Total territories   : {total_rows}")
print(f"Distinct territories: {distinct_keys}")
print(f"Duplicate keys      : {total_rows - distinct_keys}")

spark.stop()
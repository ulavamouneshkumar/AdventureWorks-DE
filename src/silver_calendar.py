from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from delta import configure_spark_with_delta_pip
from config import BRONZE_TABLES, SILVER_TABLES


# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

builder = (
    SparkSession.builder
    .appName("AdventureWorksSilverCalendar")
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
# 2. READ BRONZE CALENDAR
# ============================================================
bronze_path = str(BRONZE_TABLES["calendar"])
calendar = (
    spark.read
    .format("delta")
    .load(bronze_path)
)


# ============================================================
# 3. TRANSFORM CALENDAR
# ============================================================

silver_calendar = (
    calendar

    # Rename Date
    .withColumnRenamed(
        "Date",
        "date"
    )

    # Convert string → DATE
    .withColumn(
        "date",
        to_date(
            col("date"),
            "M/d/yyyy"
        )
    )
)


# ============================================================
# 4. WRITE SILVER DELTA
# ============================================================

target_path = str(SILVER_TABLES["calendar"])

(
    silver_calendar
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
print("SILVER CALENDAR")
print("=" * 70)

print(f"Rows: {result.count()}")

print("\nSchema:")
result.printSchema()

print("\nSample:")
result.show(10, truncate=False)


# ============================================================
# 7. DATE RANGE
# ============================================================

print("\nDate range:")

result.selectExpr(
    "min(date) as min_date",
    "max(date) as max_date"
).show()


# ============================================================
# 8. STOP SPARK
# ============================================================

spark.stop()
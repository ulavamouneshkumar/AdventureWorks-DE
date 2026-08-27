from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

builder = (
    SparkSession.builder
    .appName("InspectBronzeCalendar")
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

calendar = (
    spark.read
    .format("delta")
    .load("data/bronze/calendar")
)


# ============================================================
# 3. INSPECT
# ============================================================

print("\n" + "=" * 70)
print("BRONZE CALENDAR")
print("=" * 70)

print("\nSchema:")
calendar.printSchema()

print("\nSample:")
calendar.show(10, truncate=False)


# ============================================================
# 4. STOP
# ============================================================

spark.stop()

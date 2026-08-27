from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from delta import configure_spark_with_delta_pip


# ============================================================
# 1. CREATE SPARK SESSION WITH DELTA
# ============================================================

builder = (
    SparkSession.builder
    .appName("AdventureWorksSalesBronze")
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
# 2. SOURCE FILES
# ============================================================

sales_files = [
    (
        "2015",
        "data/landing/AdventureWorks_Sales_2015.csv"
    ),
    (
        "2016",
        "data/landing/AdventureWorks_Sales_2016.csv"
    ),
    (
        "2017",
        "data/landing/AdventureWorks_Sales_2017.csv"
    )
]


# ============================================================
# 3. READ EACH SALES FILE
# ============================================================

sales_dataframes = []

for year, file_path in sales_files:

    print("\n" + "=" * 70)
    print(f"Reading Sales {year}")
    print(f"File: {file_path}")
    print("=" * 70)

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("mode", "PERMISSIVE")
        .csv(file_path)
    )

    row_count = df.count()

    print(f"Rows: {row_count}")

    # Add source year
    df = df.withColumn(
        "_source_year",
        lit(year)
    )

    sales_dataframes.append(df)


# ============================================================
# 4. COMBINE ALL SALES DATA
# ============================================================

sales = (
    sales_dataframes[0]
    .unionByName(sales_dataframes[1])
    .unionByName(sales_dataframes[2])
)


# ============================================================
# 5. ADD INGESTION METADATA
# ============================================================

bronze_sales = (
    sales
    .withColumn(
        "_ingestion_timestamp",
        current_timestamp()
    )
    .withColumn(
        "_source_file",
        input_file_name()
    )
)


# ============================================================
# 6. VALIDATE COMBINED DATA
# ============================================================

print("\n" + "#" * 70)
print("# COMBINED SALES")
print("#" * 70)

total_rows = bronze_sales.count()

print(f"Total Sales rows: {total_rows}")

print("\nRows by source year:")

(
    bronze_sales
    .groupBy("_source_year")
    .count()
    .orderBy("_source_year")
    .show()
)


# ============================================================
# 7. WRITE TO BRONZE DELTA
# ============================================================

target_path = "data/bronze/sales"

(
    bronze_sales
    .write
    .format("delta")
    .mode("overwrite")
    .save(target_path)
)


# ============================================================
# 8. READ DELTA TABLE BACK
# ============================================================

result = (
    spark.read
    .format("delta")
    .load(target_path)
)


# ============================================================
# 9. VALIDATE BRONZE TABLE
# ============================================================

bronze_count = result.count()

print("\n" + "#" * 70)
print("# BRONZE SALES VALIDATION")
print("#" * 70)

print(f"Expected rows : 56046")
print(f"Bronze rows   : {bronze_count}")

if bronze_count == 56046:
    print("Row count validation : PASSED")
else:
    print("Row count validation : FAILED")


print("\nSchema:")
result.printSchema()


print("\nSample records:")
result.show(10, truncate=False)


# ============================================================
# 10. STOP SPARK
# ============================================================

spark.stop()
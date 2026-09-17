from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

from config import SOURCE_FILES, BRONZE_TABLES


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

full_load_files = [
    (
        "2015",
        str(SOURCE_FILES["sales_2015"])
    ),
    (
        "2016",
        str(SOURCE_FILES["sales_2016"])
    ),
    (
        "2017",
        str(SOURCE_FILES["sales_2017"])
    )
]

incremental_file = str(
    SOURCE_FILES["sales_incremental"]
)


# ============================================================
# 3. READ FULL LOAD FILES
# ============================================================

sales_dataframes = []

for year, file_path in full_load_files:

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

    df = df.withColumn(
        "_source_year",
        lit(year)
    )

    sales_dataframes.append(df)


# ============================================================
# 4. COMBINE FULL LOAD DATA
# ============================================================

full_sales = (
    sales_dataframes[0]
    .unionByName(sales_dataframes[1])
    .unionByName(sales_dataframes[2])
)


# ============================================================
# 5. CHECK WHETHER INCREMENTAL FILE EXISTS
# ============================================================

from pathlib import Path

incremental_path = Path(incremental_file)

if incremental_path.exists():

    print("\n" + "=" * 70)
    print("Incremental source detected")
    print(f"File: {incremental_file}")
    print("=" * 70)

    incremental_sales = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("mode", "PERMISSIVE")
        .csv(incremental_file)
    )

    incremental_count = incremental_sales.count()

    print(f"Incremental rows: {incremental_count}")

    incremental_sales = incremental_sales.withColumn(
        "_source_year",
        lit("incremental")
    )

else:

    print("\nNo incremental source detected.")

    incremental_sales = None


# ============================================================
# 6. DEFINE BRONZE TARGET
# ============================================================

target_path = str(
    BRONZE_TABLES["sales"]
)

print("\n" + "#" * 70)
print("# BRONZE DELTA LOAD")
print("#" * 70)

print(f"Target: {target_path}")


# ============================================================
# 7. INITIAL LOAD
# ============================================================

if not DeltaTable.isDeltaTable(
    spark,
    target_path
):

    print("\nDelta table does not exist.")
    print("Performing INITIAL FULL LOAD...")

    bronze_sales = (
        full_sales
        .withColumn(
            "_ingestion_timestamp",
            current_timestamp()
        )
        .withColumn(
            "_source_file",
            lit("full_load")
        )
    )

    (
        bronze_sales
        .write
        .format("delta")
        .mode("overwrite")
        .save(target_path)
    )

    print("Initial full load completed.")


# ============================================================
# 8. EXISTING DELTA TABLE
# ============================================================

else:

    delta_table = DeltaTable.forPath(
        spark,
        target_path
    )

    # --------------------------------------------------------
    # ONLY MERGE INCREMENTAL DATA
    # --------------------------------------------------------

    if incremental_sales is not None:

        print("\nPerforming INCREMENTAL MERGE...")

        bronze_incremental = (
            incremental_sales
            .withColumn(
                "_ingestion_timestamp",
                current_timestamp()
            )
            .withColumn(
                "_source_file",
                lit(incremental_file)
            )
        )

        incremental_count = bronze_incremental.count()

        print(
            f"Records presented for MERGE: "
            f"{incremental_count}"
        )

        # ----------------------------------------------------
        # Business key
        #
        # OrderNumber + OrderLineItem
        # ----------------------------------------------------

        merge_condition = """
            target.OrderNumber = source.OrderNumber
            AND
            target.OrderLineItem = source.OrderLineItem
        """

        (
            delta_table.alias("target")
            .merge(
                bronze_incremental.alias("source"),
                merge_condition
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        print("Incremental MERGE completed.")

    else:

        print(
            "\nNo incremental data available."
        )

        print(
            "Existing Bronze table remains unchanged."
        )


# ============================================================
# 9. READ BRONZE DELTA TABLE
# ============================================================

result = (
    spark.read
    .format("delta")
    .load(target_path)
)


# ============================================================
# 10. BRONZE VALIDATION
# ============================================================

bronze_count = result.count()

print("\n" + "#" * 70)
print("# BRONZE SALES VALIDATION")
print("#" * 70)

print(f"Bronze rows : {bronze_count}")


# ============================================================
# 11. BUSINESS KEY VALIDATION
# ============================================================

duplicate_keys = (
    result
    .groupBy(
        "OrderNumber",
        "OrderLineItem"
    )
    .count()
    .filter("count > 1")
)

duplicate_count = duplicate_keys.count()

print("\n" + "#" * 70)
print("# BUSINESS KEY VALIDATION")
print("#" * 70)

print(
    "Duplicate OrderNumber + OrderLineItem : "
    f"{duplicate_count}"
)

if duplicate_count == 0:

    print(
        "Business key validation : PASSED"
    )

else:

    print(
        "Business key validation : FAILED"
    )


# ============================================================
# 12. SOURCE YEAR VALIDATION
# ============================================================

print("\n" + "#" * 70)
print("# SOURCE YEAR VALIDATION")
print("#" * 70)

(
    result
    .groupBy("_source_year")
    .count()
    .orderBy("_source_year")
    .show()
)


# ============================================================
# 13. FINAL VALIDATION
# ============================================================

if duplicate_count == 0:

    print("\n" + "=" * 70)
    print("BRONZE SALES LOAD : PASSED")
    print("=" * 70)

else:

    print("\n" + "=" * 70)
    print("BRONZE SALES LOAD : FAILED")
    print("=" * 70)


# ============================================================
# 14. SAMPLE RECORDS
# ============================================================

print("\nSample records:")

result.show(
    10,
    truncate=False
)


# ============================================================
# 15. STOP SPARK
# ============================================================

spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from delta import configure_spark_with_delta_pip


# ============================================================
# 1. CREATE SPARK SESSION WITH DELTA LAKE
# ============================================================

builder = (
    SparkSession.builder
    .appName("AdventureWorksBronze")
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
# 2. REUSABLE BRONZE INGESTION FUNCTION
# ============================================================

def ingest_to_bronze(source_path, target_path):

    print("\n" + "=" * 70)
    print("BRONZE INGESTION")
    print("=" * 70)

    print(f"Source : {source_path}")
    print(f"Target : {target_path}")

    # --------------------------------------------------------
    # Read source CSV
    # --------------------------------------------------------

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("mode", "PERMISSIVE")
        .csv(source_path)
    )

    source_count = df.count()

    print(f"Source rows : {source_count}")

    # --------------------------------------------------------
    # Add ingestion metadata
    # --------------------------------------------------------

    bronze_df = (
        df
        .withColumn(
            "_ingestion_timestamp",
            current_timestamp()
        )
        .withColumn(
            "_source_file",
            input_file_name()
        )
    )

    # --------------------------------------------------------
    # Write to Delta
    # --------------------------------------------------------

    (
        bronze_df
        .write
        .format("delta")
        .mode("overwrite")
        .save(target_path)
    )

    # --------------------------------------------------------
    # Read Bronze table back for validation
    # --------------------------------------------------------

    result_df = (
        spark.read
        .format("delta")
        .load(target_path)
    )

    bronze_count = result_df.count()

    print(f"Bronze rows : {bronze_count}")

    # --------------------------------------------------------
    # Validate row count
    # --------------------------------------------------------

    if source_count == bronze_count:
        print("Status     : SUCCESS")
        print("Row count validation : PASSED")
    else:
        print("Status     : FAILED")
        print("Row count validation : FAILED")

    print("=" * 70)


# ============================================================
# 3. DATASET CONFIGURATION
# ============================================================

datasets = {

    "calendar": (
        "data/landing/AdventureWorks_Calendar.csv",
        "data/bronze/calendar"
    ),

    "customers": (
        "data/landing/AdventureWorks_Customers.csv",
        "data/bronze/customers"
    ),

    "product_categories": (
        "data/landing/AdventureWorks_Product_Categories.csv",
        "data/bronze/product_categories"
    ),

    "product_subcategories": (
        "data/landing/AdventureWorks_Product_Subcategories.csv",
        "data/bronze/product_subcategories"
    ),

    "products": (
        "data/landing/AdventureWorks_Products.csv",
        "data/bronze/products"
    ),

    "returns": (
        "data/landing/AdventureWorks_Returns.csv",
        "data/bronze/returns"
    ),

    "territories": (
        "data/landing/AdventureWorks_Territories.csv",
        "data/bronze/territories"
    )
}


# ============================================================
# 4. INGEST ALL NON-SALES DATASETS
# ============================================================

print("\n")
print("#" * 70)
print("# STARTING ADVENTUREWORKS BRONZE INGESTION")
print("#" * 70)


for dataset_name, (source_path, target_path) in datasets.items():

    print(f"\nStarting dataset: {dataset_name}")

    ingest_to_bronze(
        source_path,
        target_path
    )


# ============================================================
# 5. FINISH
# ============================================================

print("\n")
print("#" * 70)
print("# BRONZE INGESTION COMPLETED")
print("#" * 70)


# ============================================================
# 6. STOP SPARK
# ============================================================

spark.stop()
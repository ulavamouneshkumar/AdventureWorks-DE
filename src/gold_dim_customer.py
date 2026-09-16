from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, trim
from delta import configure_spark_with_delta_pip
from config import SILVER_TABLES, GOLD_TABLES

# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

builder = (
    SparkSession.builder
    .appName("AdventureWorksGoldDimCustomer")
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
# 2. READ SILVER CUSTOMERS
# ============================================================
silver_path = str(SILVER_TABLES["customers"])
customers = (
    spark.read
    .format("delta")
    .load(silver_path)
)


# ============================================================
# 3. CREATE GOLD CUSTOMER DIMENSION
# ============================================================

dim_customer = (
    customers

    # --------------------------------------------------------
    # Create full name
    # --------------------------------------------------------

    .withColumn(
        "full_name",
        concat_ws(
            " ",
            trim(col("first_name")),
            trim(col("last_name"))
        )
    )

    # --------------------------------------------------------
    # Select business-facing columns
    # --------------------------------------------------------

    .select(
        "customer_key",
        "prefix",
        "first_name",
        "last_name",
        "full_name",
        "birth_date",
        "marital_status",
        "gender",
        "email_address",
        "annual_income",
        "total_children",
        "education_level",
        "occupation",
        "home_owner"
    )
)


# ============================================================
# 4. WRITE GOLD DELTA
# ============================================================

target_path = str(GOLD_TABLES["dim_customer"])

(
    dim_customer
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
# 6. BASIC VALIDATION
# ============================================================

print("\n" + "=" * 70)
print("GOLD DIM CUSTOMER")
print("=" * 70)

total_customers = result.count()

print(f"Rows: {total_customers}")


# ============================================================
# 7. SCHEMA
# ============================================================

print("\nSchema:")
result.printSchema()


# ============================================================
# 8. SAMPLE
# ============================================================

print("\nSample:")
result.show(10, truncate=False)


# ============================================================
# 9. CUSTOMER KEY VALIDATION
# ============================================================

distinct_customer_keys = (
    result
    .select("customer_key")
    .distinct()
    .count()
)

print("\n" + "=" * 70)
print("CUSTOMER KEY VALIDATION")
print("=" * 70)

print(f"Total customers        : {total_customers}")
print(f"Distinct customer keys : {distinct_customer_keys}")

print(
    f"Duplicate customer keys: "
    f"{total_customers - distinct_customer_keys}"
)


# ============================================================
# 10. NULL KEY VALIDATION
# ============================================================

null_customer_keys = (
    result
    .filter(col("customer_key").isNull())
    .count()
)

print(
    f"Null customer keys     : "
    f"{null_customer_keys}"
)


# ============================================================
# 11. FULL NAME VALIDATION
# ============================================================

null_full_names = (
    result
    .filter(col("full_name").isNull())
    .count()
)

print(
    f"Null full names        : "
    f"{null_full_names}"
)


# ============================================================
# 12. STOP SPARK
# ============================================================

spark.stop()
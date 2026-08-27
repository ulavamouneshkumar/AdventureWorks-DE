from pyspark.sql import SparkSession

from delta import configure_spark_with_delta_pip

from data_quality.checks import (
    check_row_count,
    check_null_keys,
    check_unique_key,
    check_foreign_key,
    check_grain,
    check_positive_values,
    check_nulls
)


# ============================================================
# SPARK SESSION
# ============================================================

builder = (
    SparkSession.builder
    .appName("AdventureWorksDataQuality")
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

spark = configure_spark_with_delta_pip(
    builder
).getOrCreate()


# ============================================================
# READ TABLES
# ============================================================

print("\n" + "=" * 70)
print("DATA QUALITY FRAMEWORK")
print("=" * 70)

dim_customer = (
    spark.read
    .format("delta")
    .load("data/gold/dim_customer")
)

dim_product = (
    spark.read
    .format("delta")
    .load("data/gold/dim_product")
)

dim_date = (
    spark.read
    .format("delta")
    .load("data/gold/dim_date")
)

dim_territory = (
    spark.read
    .format("delta")
    .load("data/gold/dim_territory")
)

fact_sales = (
    spark.read
    .format("delta")
    .load("data/gold/fact_sales")
)

fact_returns = (
    spark.read
    .format("delta")
    .load("data/gold/fact_returns")
)


# ============================================================
# RESULTS
# ============================================================

results = []


# ============================================================
# DIM CUSTOMER
# ============================================================

print("\n" + "=" * 70)
print("DIM CUSTOMER")
print("=" * 70)

result = check_row_count(
    dim_customer,
    18148
)

results.append(result)
print(result)

result = check_unique_key(
    dim_customer,
    ["customer_key"]
)

results.append(result)
print(result)

result = check_null_keys(
    dim_customer,
    ["customer_key"]
)

results.append(result)
print(result)


# ============================================================
# DIM PRODUCT
# ============================================================

print("\n" + "=" * 70)
print("DIM PRODUCT")
print("=" * 70)

result = check_row_count(
    dim_product,
    293
)

results.append(result)
print(result)

result = check_unique_key(
    dim_product,
    ["product_key"]
)

results.append(result)
print(result)

result = check_null_keys(
    dim_product,
    ["product_key"]
)

results.append(result)
print(result)


# ============================================================
# DIM DATE
# ============================================================

print("\n" + "=" * 70)
print("DIM DATE")
print("=" * 70)

result = check_row_count(
    dim_date,
    912
)

results.append(result)
print(result)

result = check_unique_key(
    dim_date,
    ["date_key"]
)

results.append(result)
print(result)

result = check_null_keys(
    dim_date,
    ["date_key"]
)

results.append(result)
print(result)


# ============================================================
# DIM TERRITORY
# ============================================================

print("\n" + "=" * 70)
print("DIM TERRITORY")
print("=" * 70)

result = check_row_count(
    dim_territory,
    10
)

results.append(result)
print(result)

result = check_unique_key(
    dim_territory,
    ["territory_key"]
)

results.append(result)
print(result)


# ============================================================
# FACT SALES
# ============================================================

print("\n" + "=" * 70)
print("FACT SALES")
print("=" * 70)

result = check_row_count(
    fact_sales,
    56046
)

results.append(result)
print(result)


result = check_grain(
    fact_sales,
    [
        "order_number",
        "order_line_item"
    ]
)

results.append(result)
print(result)


result = check_null_keys(
    fact_sales,
    [
        "date_key",
        "product_key",
        "customer_key",
        "territory_key",
        "order_number"
    ]
)

results.append(result)
print(result)


result = check_foreign_key(
    fact_sales,
    "customer_key",
    dim_customer,
    "customer_key"
)

results.append(result)
print(result)


result = check_foreign_key(
    fact_sales,
    "product_key",
    dim_product,
    "product_key"
)

results.append(result)
print(result)


result = check_foreign_key(
    fact_sales,
    "date_key",
    dim_date,
    "date_key"
)

results.append(result)
print(result)


result = check_foreign_key(
    fact_sales,
    "territory_key",
    dim_territory,
    "territory_key"
)

results.append(result)
print(result)


result = check_positive_values(
    fact_sales,
    "order_quantity"
)

results.append(result)
print(result)


# ============================================================
# FACT RETURNS
# ============================================================

print("\n" + "=" * 70)
print("FACT RETURNS")
print("=" * 70)

result = check_row_count(
    fact_returns,
    1809
)

results.append(result)
print(result)


result = check_grain(
    fact_returns,
    [
        "date_key",
        "territory_key",
        "product_key"
    ]
)

results.append(result)
print(result)


result = check_null_keys(
    fact_returns,
    [
        "date_key",
        "product_key",
        "territory_key"
    ]
)

results.append(result)
print(result)


result = check_foreign_key(
    fact_returns,
    "product_key",
    dim_product,
    "product_key"
)

results.append(result)
print(result)


result = check_foreign_key(
    fact_returns,
    "territory_key",
    dim_territory,
    "territory_key"
)

results.append(result)


# ============================================================
# FINAL STATUS
# ============================================================

print("\n" + "=" * 70)
print("FINAL DATA QUALITY STATUS")
print("=" * 70)

failed_checks = [
    result
    for result in results
    if result["status"] == "FAILED"
]

passed_checks = [
    result
    for result in results
    if result["status"] == "PASSED"
]

print(
    f"Total checks  : {len(results)}"
)

print(
    f"Passed checks  : {len(passed_checks)}"
)

print(
    f"Failed checks  : {len(failed_checks)}"
)


if failed_checks:

    print("\nFAILED CHECKS:")

    for result in failed_checks:
        print(result)

    overall_status = "FAILED"

else:

    overall_status = "PASSED"


print(
    f"\nOverall Status : "
    f"{overall_status}"
)


spark.stop()
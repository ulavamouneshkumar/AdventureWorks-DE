from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("AdventureWorksReturnsProfiling")
    .master("local[*]")
    .getOrCreate()
)


# --------------------------------------------------
# 2. Read Returns
# --------------------------------------------------

returns = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/landing/AdventureWorks_Returns.csv")
)


# --------------------------------------------------
# 3. Read Products
# --------------------------------------------------

products = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/landing/AdventureWorks_Products.csv")
)


# --------------------------------------------------
# 4. Read Territories
# --------------------------------------------------

territories = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/landing/AdventureWorks_Territories.csv")
)


# --------------------------------------------------
# 5. Basic information
# --------------------------------------------------

print("\n========== RETURNS OVERVIEW ==========")

total_returns = returns.count()

print(f"Rows    : {total_returns}")
print(f"Columns : {len(returns.columns)}")


# --------------------------------------------------
# 6. Schema
# --------------------------------------------------

print("\n========== RETURNS SCHEMA ==========")

returns.printSchema()


# --------------------------------------------------
# 7. Duplicate analysis
# --------------------------------------------------

print("\n========== DUPLICATE ANALYSIS ==========")

distinct_returns = returns.dropDuplicates().count()

print(f"Total rows     : {total_returns}")
print(f"Distinct rows  : {distinct_returns}")
print(f"Duplicate rows : {total_returns - distinct_returns}")


# --------------------------------------------------
# 8. Product foreign-key validation
# --------------------------------------------------

print("\n========== RETURNS → PRODUCT ==========")

orphan_products = (
    returns
    .select("ProductKey")
    .distinct()
    .join(
        products
        .select("ProductKey")
        .distinct(),
        on="ProductKey",
        how="left_anti"
    )
)

orphan_product_count = orphan_products.count()

print(f"Orphan ProductKeys : {orphan_product_count}")

if orphan_product_count > 0:
    orphan_products.show()


# --------------------------------------------------
# 9. Territory foreign-key validation
# --------------------------------------------------

print("\n========== RETURNS → TERRITORY ==========")

orphan_territories = (
    returns
    .select("TerritoryKey")
    .distinct()
    .join(
        territories
        .select("SalesTerritoryKey")
        .distinct(),
        returns.TerritoryKey == territories.SalesTerritoryKey,
        how="left_anti"
    )
)

orphan_territory_count = orphan_territories.count()

print(f"Orphan TerritoryKeys : {orphan_territory_count}")

if orphan_territory_count > 0:
    orphan_territories.show()


# --------------------------------------------------
# 10. Grain analysis
# --------------------------------------------------

print("\n========== RETURNS GRAIN ANALYSIS ==========")

distinct_combinations = (
    returns
    .select(
        "ReturnDate",
        "TerritoryKey",
        "ProductKey"
    )
    .distinct()
    .count()
)

print(
    f"Distinct ReturnDate + TerritoryKey + ProductKey : "
    f"{distinct_combinations}"
)

print(f"Total rows : {total_returns}")

print(
    f"Duplicate combinations : "
    f"{total_returns - distinct_combinations}"
)


# --------------------------------------------------
# 11. Stop Spark
# --------------------------------------------------

spark.stop()
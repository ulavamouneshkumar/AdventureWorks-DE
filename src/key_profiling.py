from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("AdventureWorksKeyProfiling")
    .master("local[*]")
    .getOrCreate()
)


# --------------------------------------------------
# 2. Read Product Subcategories
# --------------------------------------------------

subcategories = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/landing/AdventureWorks_Product_Subcategories.csv")
)


# --------------------------------------------------
# 3. Read Product Categories
# --------------------------------------------------

categories = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/landing/AdventureWorks_Product_Categories.csv")
)


# --------------------------------------------------
# 4. Subcategory → Category relationship
# --------------------------------------------------

print("\n========== SUBCATEGORY → CATEGORY ==========")

orphan_categories = (
    subcategories
    .select("ProductCategoryKey")
    .distinct()
    .join(
        categories
        .select("ProductCategoryKey")
        .distinct(),
        on="ProductCategoryKey",
        how="left_anti"
    )
)

orphan_category_count = orphan_categories.count()

print(f"Orphan Category Keys : {orphan_category_count}")


if orphan_category_count > 0:
    print("\nOrphan category keys:")
    orphan_categories.show()


# --------------------------------------------------
# 5. Category key uniqueness
# --------------------------------------------------

print("\n========== CATEGORY KEY ANALYSIS ==========")

total_categories = categories.count()

distinct_category_keys = (
    categories
    .select("ProductCategoryKey")
    .distinct()
    .count()
)

duplicate_category_keys = (
    total_categories - distinct_category_keys
)

print(f"Total Category rows       : {total_categories}")
print(f"Distinct Category keys    : {distinct_category_keys}")
print(f"Duplicate Category keys   : {duplicate_category_keys}")


# --------------------------------------------------
# 6. Stop Spark
# --------------------------------------------------

spark.stop()
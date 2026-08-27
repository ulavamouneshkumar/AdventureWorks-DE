from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, concat_ws
from pyspark.sql.functions import to_date, min, max, datediff, sum, when

# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("AdventureWorksSalesProfiling")
    .master("local[*]")
    .getOrCreate()
)


# --------------------------------------------------
# 2. Read Sales files
# --------------------------------------------------

sales_2015 = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/landing/AdventureWorks_Sales_2015.csv")
)

sales_2016 = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/landing/AdventureWorks_Sales_2016.csv")
)

sales_2017 = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/landing/AdventureWorks_Sales_2017.csv")
)


# --------------------------------------------------
# 3. Combine yearly sales
# --------------------------------------------------

sales = (
    sales_2015
    .unionByName(sales_2016)
    .unionByName(sales_2017)
)


# --------------------------------------------------
# 4. Basic information
# --------------------------------------------------

print("\n========== SALES OVERVIEW ==========")

print(f"2015 rows : {sales_2015.count()}")
print(f"2016 rows : {sales_2016.count()}")
print(f"2017 rows : {sales_2017.count()}")
print(f"Total rows: {sales.count()}")

print(f"Columns   : {len(sales.columns)}")


# --------------------------------------------------
# 5. Schema
# --------------------------------------------------

print("\n========== SALES SCHEMA ==========")

sales.printSchema()


# --------------------------------------------------
# 6. Full-row duplicates
# --------------------------------------------------

print("\n========== DUPLICATE ANALYSIS ==========")

total_rows = sales.count()

distinct_rows = sales.dropDuplicates().count()

print(f"Total rows     : {total_rows}")
print(f"Distinct rows  : {distinct_rows}")
print(f"Duplicate rows : {total_rows - distinct_rows}")


# --------------------------------------------------
# 7. OrderNumber uniqueness
# --------------------------------------------------

print("\n========== ORDER NUMBER ANALYSIS ==========")

total_orders = (
    sales
    .select("OrderNumber")
    .distinct()
    .count()
)

print(f"Distinct OrderNumbers : {total_orders}")


# --------------------------------------------------
# 8. OrderNumber + OrderLineItem
# --------------------------------------------------

print("\n========== SALES GRAIN ANALYSIS ==========")

total_combinations = (
    sales
    .select(
        "OrderNumber",
        "OrderLineItem"
    )
    .distinct()
    .count()
)

print(
    f"Distinct OrderNumber + OrderLineItem : "
    f"{total_combinations}"
)

print(
    f"Total rows                           : "
    f"{total_rows}"
)

print(
    f"Duplicate combinations               : "
    f"{total_rows - total_combinations}"
)


# --------------------------------------------------
# 9. Show multiple line items
# --------------------------------------------------

print("\n========== SAMPLE ORDERS WITH MULTIPLE LINES ==========")

orders_with_multiple_lines = (
    sales
    .groupBy("OrderNumber")
    .agg(
        count("*").alias("LineItemCount")
    )
    .filter(col("LineItemCount") > 1)
    .orderBy(col("LineItemCount").desc())
)

orders_with_multiple_lines.show(10)


# --------------------------------------------------
# 11. Read dimension datasets
# --------------------------------------------------

customers = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/landing/AdventureWorks_Customers.csv")
)

products = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/landing/AdventureWorks_Products.csv")
)

territories = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/landing/AdventureWorks_Territories.csv")
)


# --------------------------------------------------
# 12. ProductKey validation
# --------------------------------------------------

print("\n========== SALES → PRODUCT ==========")

orphan_products = (
    sales
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
# 13. CustomerKey validation
# --------------------------------------------------

print("\n========== SALES → CUSTOMER ==========")

orphan_customers = (
    sales
    .select("CustomerKey")
    .distinct()
    .join(
        customers
        .select("CustomerKey")
        .distinct(),
        on="CustomerKey",
        how="left_anti"
    )
)

orphan_customer_count = orphan_customers.count()

print(f"Orphan CustomerKeys : {orphan_customer_count}")

if orphan_customer_count > 0:
    orphan_customers.show()


# --------------------------------------------------
# 14. TerritoryKey validation
# --------------------------------------------------

print("\n========== SALES → TERRITORY ==========")

orphan_territories = (
    sales
    .select("TerritoryKey")
    .distinct()
    .join(
        territories
        .select("SalesTerritoryKey")
        .distinct(),
        sales.TerritoryKey == territories.SalesTerritoryKey,
        how="left_anti"
    )
)

orphan_territory_count = orphan_territories.count()

print(f"Orphan TerritoryKeys : {orphan_territory_count}")

if orphan_territory_count > 0:
    orphan_territories.show()


# --------------------------------------------------
# 15. Date quality analysis
# --------------------------------------------------

print("\n========== DATE QUALITY ANALYSIS ==========")

sales_dates = (
    sales
    .withColumn(
        "OrderDateParsed",
        to_date("OrderDate", "M/d/yyyy")
    )
    .withColumn(
        "StockDateParsed",
        to_date("StockDate", "M/d/yyyy")
    )
)


# --------------------------------------------------
# Date ranges
# --------------------------------------------------

print("\n--- DATE RANGES ---")

sales_dates.select(
    min("OrderDateParsed").alias("MinOrderDate"),
    max("OrderDateParsed").alias("MaxOrderDate"),
    min("StockDateParsed").alias("MinStockDate"),
    max("StockDateParsed").alias("MaxStockDate")
).show()


# --------------------------------------------------
# StockDate after OrderDate
# --------------------------------------------------

print("\n--- STOCK DATE AFTER ORDER DATE ---")

stock_after_order = (
    sales_dates
    .filter(col("StockDateParsed") > col("OrderDateParsed"))
    .count()
)

print(
    f"Rows where StockDate > OrderDate : "
    f"{stock_after_order}"
)


# --------------------------------------------------
# StockDate before OrderDate
# --------------------------------------------------

print("\n--- STOCK DATE BEFORE ORDER DATE ---")

stock_before_order = (
    sales_dates
    .filter(col("StockDateParsed") < col("OrderDateParsed"))
    .count()
)

print(
    f"Rows where StockDate < OrderDate : "
    f"{stock_before_order}"
)


# --------------------------------------------------
# Same date
# --------------------------------------------------

print("\n--- SAME DATE ---")

same_date = (
    sales_dates
    .filter(col("StockDateParsed") == col("OrderDateParsed"))
    .count()
)

print(
    f"Rows where StockDate = OrderDate : "
    f"{same_date}"
)
spark.stop()
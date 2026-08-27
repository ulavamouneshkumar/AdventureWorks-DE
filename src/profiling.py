from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, trim


# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("AdventureWorksDataProfiling")
    .master("local[*]")
    .getOrCreate()
)


# --------------------------------------------------
# 2. Function to profile a dataset
# --------------------------------------------------

def profile_dataset(file_path):

    print("\n" + "=" * 70)
    print(f"DATASET: {file_path}")
    print("=" * 70)

    # Read CSV
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("mode", "PERMISSIVE")
        .csv(file_path)
    )

    # --------------------------------------------------
    # Basic information
    # --------------------------------------------------

    row_count = df.count()
    column_count = len(df.columns)

    print("\n========== BASIC INFORMATION ==========")
    print(f"Rows    : {row_count}")
    print(f"Columns : {column_count}")

    # --------------------------------------------------
    # Column names
    # --------------------------------------------------

    print("\n========== COLUMNS ==========")

    for column in df.columns:
        print(column)

    # --------------------------------------------------
    # Schema
    # --------------------------------------------------

    print("\n========== SCHEMA ==========")

    df.printSchema()

    # --------------------------------------------------
    # Sample data
    # --------------------------------------------------

    print("\n========== SAMPLE DATA ==========")

    df.show(5, truncate=False)

    # --------------------------------------------------
    # NULL analysis
    # --------------------------------------------------

    print("\n========== NULL ANALYSIS ==========")

    null_counts = df.select(
        [
            count(
                when(
                    col(c).isNull()
                    | (trim(col(c).cast("string")) == ""),
                    c
                )
            ).alias(c)
            for c in df.columns
        ]
    )

    null_counts.show(truncate=False)

    # --------------------------------------------------
    # Full-row duplicate analysis
    # --------------------------------------------------

    print("\n========== DUPLICATE ANALYSIS ==========")

    distinct_count = df.dropDuplicates().count()

    print(f"Total rows     : {row_count}")
    print(f"Distinct rows  : {distinct_count}")
    print(f"Duplicate rows : {row_count - distinct_count}")

    return df


# --------------------------------------------------
# 3. Files to profile
# --------------------------------------------------

files = [
    "data/landing/AdventureWorks_Calendar.csv",
    "data/landing/AdventureWorks_Customers.csv",
    "data/landing/AdventureWorks_Product_Categories.csv",
    "data/landing/AdventureWorks_Product_Subcategories.csv",
    "data/landing/AdventureWorks_Products.csv",
    "data/landing/AdventureWorks_Returns.csv",
    "data/landing/AdventureWorks_Sales_2015.csv",
    "data/landing/AdventureWorks_Sales_2016.csv",
    "data/landing/AdventureWorks_Sales_2017.csv",
    "data/landing/AdventureWorks_Territories.csv"
]


# --------------------------------------------------
# 4. Profile every dataset
# --------------------------------------------------

for file_path in files:
    profile_dataset(file_path)


# --------------------------------------------------
# 5. Stop Spark
# --------------------------------------------------

spark.stop()
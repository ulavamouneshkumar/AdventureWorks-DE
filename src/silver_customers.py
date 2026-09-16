from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    regexp_replace,
    to_date
)
from pyspark.sql.types import DecimalType
from delta import configure_spark_with_delta_pip
from config import SOURCE_FILES, BRONZE_TABLES, SILVER_TABLES

# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

builder = (
    SparkSession.builder
    .appName("AdventureWorksSilverCustomers")
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
# 2. READ BRONZE CUSTOMERS
# ============================================================

bronze_path = str(BRONZE_TABLES["customers"])

customers = (
    spark.read
    .format("delta")
    .load(bronze_path)
)


# ============================================================
# 3. TRANSFORM CUSTOMERS
# ============================================================

silver_customers = (
    customers

    # Rename columns
    .withColumnRenamed("CustomerKey", "customer_key")
    .withColumnRenamed("Prefix", "prefix")
    .withColumnRenamed("FirstName", "first_name")
    .withColumnRenamed("LastName", "last_name")
    .withColumnRenamed("BirthDate", "birth_date")
    .withColumnRenamed("MaritalStatus", "marital_status")
    .withColumnRenamed("Gender", "gender")
    .withColumnRenamed("EmailAddress", "email_address")
    .withColumnRenamed("AnnualIncome", "annual_income")
    .withColumnRenamed("TotalChildren", "total_children")
    .withColumnRenamed("EducationLevel", "education_level")
    .withColumnRenamed("Occupation", "occupation")
    .withColumnRenamed("HomeOwner", "home_owner")

    # Convert BirthDate from string → date
    .withColumn(
        "birth_date",
        to_date(col("birth_date"), "M/d/yyyy")
    )

    # Convert AnnualIncome from "$90,000" → 90000.00
    .withColumn(
        "annual_income",
        regexp_replace(
            col("annual_income"),
            "[$,]",
            ""
        ).cast(DecimalType(12, 2))
    )

    # Trim string columns
    .withColumn("prefix", trim(col("prefix")))
    .withColumn("first_name", trim(col("first_name")))
    .withColumn("last_name", trim(col("last_name")))
    .withColumn("email_address", trim(col("email_address")))
    .withColumn("education_level", trim(col("education_level")))
    .withColumn("occupation", trim(col("occupation")))
    .withColumn("home_owner", trim(col("home_owner")))

    # Standardize categorical values
    .withColumn("marital_status", upper(trim(col("marital_status"))))
    .withColumn("gender", upper(trim(col("gender"))))
)


# ============================================================
# 4. WRITE SILVER DELTA TABLE
# ============================================================

target_path = str(SILVER_TABLES["customers"])

(
    silver_customers
    .write
    .format("delta")
    .mode("overwrite")
    .save(target_path)
)


# ============================================================
# 5. VALIDATE
# ============================================================

result = (
    spark.read
    .format("delta")
    .load(target_path)
)

print("\n" + "=" * 70)
print("SILVER CUSTOMERS")
print("=" * 70)

print(f"Rows: {result.count()}")

print("\nSchema:")
result.printSchema()

print("\nSample:")
result.show(10, truncate=False)


# ============================================================
# 6. STOP SPARK
# ============================================================

spark.stop()
"""
AdventureWorks Data Engineering
Module 15 - Data Freshness Monitoring

Purpose:
    Validate that important Gold datasets
    contain data up to the expected business date.
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import max as spark_max
from delta import configure_spark_with_delta_pip


# ============================================================
# 1. PROJECT PATH
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

GOLD_DIR = PROJECT_ROOT / "data" / "gold"


# ============================================================
# 2. EXPECTED LATEST DATES
# ============================================================

EXPECTED_LATEST_DATES = {

    "sales": "2017-06-30",

    "returns": "2017-06-30",

    "calendar": "2017-06-30",
}


# ============================================================
# 3. TABLE PATHS
# ============================================================

TABLE_PATHS = {

    "sales":
        GOLD_DIR / "fact_sales",

    "returns":
        GOLD_DIR / "fact_returns",

    "calendar":
        GOLD_DIR / "dim_date",
}


# ============================================================
# 4. DATE COLUMNS
# ============================================================

DATE_COLUMNS = {

    "sales":
        "order_date",

    "returns":
        "return_date",

    "calendar":
        "date",
}


# ============================================================
# 5. CREATE SPARK SESSION
# ============================================================

def create_spark_session():

    builder = (
        SparkSession.builder
        .appName("AdventureWorks-Freshness-Monitoring")
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

    return spark


# ============================================================
# 6. CHECK SINGLE DATASET
# ============================================================

def check_freshness(
    spark,
    dataset_name,
    expected_date,
):

    table_path = TABLE_PATHS[dataset_name]

    date_column = DATE_COLUMNS[dataset_name]

    print()
    print("=" * 70)
    print(f"FRESHNESS CHECK: {dataset_name}")
    print("=" * 70)

    print(f"Table path       : {table_path}")
    print(f"Date column      : {date_column}")
    print(f"Expected latest  : {expected_date}")

    try:

        # ----------------------------------------------------
        # Check table exists
        # ----------------------------------------------------

        if not table_path.exists():

            print("Actual latest    : TABLE NOT FOUND")
            print("Status           : FAILED")

            return {
                "dataset": dataset_name,
                "expected": expected_date,
                "actual": None,
                "status": "FAILED",
            }

        # ----------------------------------------------------
        # Read Delta table
        # ----------------------------------------------------

        df = (
            spark.read
            .format("delta")
            .load(str(table_path))
        )

        # ----------------------------------------------------
        # Find latest business date
        # ----------------------------------------------------

        result = (
            df.select(
                spark_max(date_column).alias("latest_date")
            )
            .collect()[0]
        )

        actual_date = result["latest_date"]

        actual_date_string = (
            str(actual_date)
            if actual_date is not None
            else None
        )

        print(
            f"Actual latest    : "
            f"{actual_date_string}"
        )

        # ----------------------------------------------------
        # Compare dates
        # ----------------------------------------------------

        if actual_date_string == expected_date:

            print("Status           : PASSED")

            status = "PASSED"

        else:

            print("Status           : FAILED")

            status = "FAILED"

        return {
            "dataset": dataset_name,
            "expected": expected_date,
            "actual": actual_date_string,
            "status": status,
        }

    except Exception as e:

        print("Actual latest    : ERROR")
        print("Status           : FAILED")

        print()
        print("Error details:")
        print(str(e))

        return {
            "dataset": dataset_name,
            "expected": expected_date,
            "actual": None,
            "status": "FAILED",
            "error": str(e),
        }


# ============================================================
# 7. RUN ALL FRESHNESS CHECKS
# ============================================================

def run_freshness_checks(spark):

    results = []

    for dataset_name, expected_date in (
        EXPECTED_LATEST_DATES.items()
    ):

        result = check_freshness(
            spark,
            dataset_name,
            expected_date,
        )

        results.append(result)

    return results


# ============================================================
# 8. PRINT SUMMARY
# ============================================================

def print_summary(results):

    print()
    print()
    print("=" * 80)
    print("DATA FRESHNESS MONITORING SUMMARY")
    print("=" * 80)

    print(
        f"{'Dataset':<20}"
        f"{'Expected':>15}"
        f"{'Actual':>15}"
        f"{'Status':>15}"
    )

    print("-" * 80)

    passed = 0
    failed = 0

    for result in results:

        dataset = result["dataset"]

        expected = result["expected"]

        actual = result["actual"]

        status = result["status"]

        actual_display = (
            str(actual)
            if actual is not None
            else "N/A"
        )

        print(
            f"{dataset:<20}"
            f"{expected:>15}"
            f"{actual_display:>15}"
            f"{status:>15}"
        )

        if status == "PASSED":

            passed += 1

        else:

            failed += 1

    print("-" * 80)

    print(f"Checks Passed : {passed}")
    print(f"Checks Failed : {failed}")

    print("=" * 80)

    if failed == 0:

        print("OVERALL STATUS: PASSED")

    else:

        print("OVERALL STATUS: FAILED")


# ============================================================
# 9. MAIN
# ============================================================

def main():

    print()
    print("=" * 80)
    print("ADVENTUREWORKS - DATA FRESHNESS MONITORING")
    print("=" * 80)

    spark = None

    try:

        # ----------------------------------------------------
        # Create Spark
        # ----------------------------------------------------

        spark = create_spark_session()

        # ----------------------------------------------------
        # Run checks
        # ----------------------------------------------------

        results = run_freshness_checks(spark)

        # ----------------------------------------------------
        # Print summary
        # ----------------------------------------------------

        print_summary(results)

        # ----------------------------------------------------
        # Fail if any check failed
        # ----------------------------------------------------

        failed = sum(
            1
            for result in results
            if result["status"] != "PASSED"
        )

        if failed > 0:

            raise RuntimeError(
                f"Data freshness monitoring failed: "
                f"{failed} check(s) failed."
            )

    finally:

        if spark is not None:

            spark.stop()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()
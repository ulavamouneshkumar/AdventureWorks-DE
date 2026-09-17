"""
AdventureWorks Data Engineering
Module 15 - Row Count Monitoring

Purpose:
    Validate expected vs actual row counts
    for important Gold Delta tables.
"""

from pathlib import Path

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


# ============================================================
# 1. PROJECT PATH
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

GOLD_DIR = PROJECT_ROOT / "data" / "gold"


# ============================================================
# 2. EXPECTED ROW COUNTS
# ============================================================

EXPECTED_ROW_COUNTS = {
    "customers": 18148,
    "products": 293,
    "territories": 10,
    "calendar": 912,
    "sales": 56046,
    "returns": 1809,
}


# ============================================================
# 3. GOLD DELTA TABLE PATHS
# ============================================================

TABLE_PATHS = {
    "customers": GOLD_DIR / "dim_customer",
    "products": GOLD_DIR / "dim_product",
    "territories": GOLD_DIR / "dim_territory",
    "calendar": GOLD_DIR / "dim_date",
    "sales": GOLD_DIR / "fact_sales",
    "returns": GOLD_DIR / "fact_returns",
}


# ============================================================
# 4. CREATE SPARK SESSION
# ============================================================

def create_spark_session():

    builder = (
        SparkSession.builder
        .appName("AdventureWorks-RowCount-Monitoring")
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

    return spark


# ============================================================
# 5. CHECK SINGLE TABLE
# ============================================================

def check_row_count(
    spark,
    dataset_name,
    expected_count,
):

    table_path = TABLE_PATHS[dataset_name]

    print()
    print("=" * 70)
    print(f"ROW COUNT CHECK: {dataset_name}")
    print("=" * 70)

    print(f"Table path : {table_path}")
    print(f"Expected   : {expected_count}")

    try:

        # ----------------------------------------------------
        # Check whether Delta table exists
        # ----------------------------------------------------

        if not table_path.exists():

            print("Actual     : TABLE NOT FOUND")
            print("Status     : FAILED")

            return {
                "dataset": dataset_name,
                "expected": expected_count,
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
        # Count rows
        # ----------------------------------------------------

        actual_count = df.count()

        print(f"Actual     : {actual_count}")

        # ----------------------------------------------------
        # Compare expected vs actual
        # ----------------------------------------------------

        if actual_count == expected_count:

            print("Status     : PASSED")

            status = "PASSED"

        else:

            print("Status     : FAILED")

            status = "FAILED"

        return {
            "dataset": dataset_name,
            "expected": expected_count,
            "actual": actual_count,
            "status": status,
        }

    except Exception as e:

        print("Actual     : ERROR")
        print("Status     : FAILED")

        print()
        print("Error details:")
        print(str(e))

        return {
            "dataset": dataset_name,
            "expected": expected_count,
            "actual": None,
            "status": "FAILED",
            "error": str(e),
        }


# ============================================================
# 6. RUN ALL ROW COUNT CHECKS
# ============================================================

def run_row_count_checks(spark):

    results = []

    for dataset_name, expected_count in EXPECTED_ROW_COUNTS.items():

        result = check_row_count(
            spark,
            dataset_name,
            expected_count,
        )

        results.append(result)

    return results


# ============================================================
# 7. PRINT SUMMARY
# ============================================================

def print_summary(results):

    print()
    print()
    print("=" * 80)
    print("ROW COUNT MONITORING SUMMARY")
    print("=" * 80)

    print(
        f"{'Dataset':<20}"
        f"{'Expected':>12}"
        f"{'Actual':>12}"
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
            f"{expected:>12}"
            f"{actual_display:>12}"
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
# 8. MAIN
# ============================================================

def main():

    print()
    print("=" * 80)
    print("ADVENTUREWORKS - ROW COUNT MONITORING")
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

        results = run_row_count_checks(spark)

        # ----------------------------------------------------
        # Print summary
        # ----------------------------------------------------

        print_summary(results)

        # ----------------------------------------------------
        # Exit with failure if any check failed
        # ----------------------------------------------------

        failed = sum(
            1
            for result in results
            if result["status"] != "PASSED"
        )

        if failed > 0:

            raise RuntimeError(
                f"Row count monitoring failed: "
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
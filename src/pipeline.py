"""
AdventureWorks Data Engineering Pipeline

Purpose:
    Orchestrates the complete end-to-end data engineering pipeline.

Pipeline flow:
    Landing → Bronze → Silver → Gold → Data Quality

The pipeline executes each stage as a separate Python process.
"""

from pathlib import Path
import os
import subprocess
import sys
import time
from datetime import datetime

from config import PROJECT_ROOT, SRC_DIR, LOG_DIR


# ============================================================
# PIPELINE CONFIGURATION
# ============================================================

PIPELINE_STAGES = [
    # --------------------------------------------------------
    # BRONZE
    # --------------------------------------------------------
    ("Bronze - Dimension Ingestion", "bronze_ingestion.py"),
    ("Bronze - Sales Ingestion", "sales_bronze_ingestion.py"),

    # --------------------------------------------------------
    # SILVER
    # --------------------------------------------------------
    ("Silver - Customers", "silver_customers.py"),
    ("Silver - Products", "silver_products.py"),
    ("Silver - Product Categories", "silver_product_categories.py"),
    ("Silver - Product Subcategories", "silver_product_subcategories.py"),
    ("Silver - Territories", "silver_territories.py"),
    ("Silver - Calendar", "silver_calendar.py"),
    ("Silver - Returns", "silver_returns.py"),
    ("Silver - Sales", "silver_sales.py"),

    # --------------------------------------------------------
    # GOLD - DIMENSIONS
    # --------------------------------------------------------
    ("Gold - Date Dimension", "gold_dim_date.py"),
    ("Gold - Customer Dimension", "gold_dim_customer.py"),
    ("Gold - Product Dimension", "gold_dim_product.py"),
    ("Gold - Territory Dimension", "gold_dim_territory.py"),

    # --------------------------------------------------------
    # GOLD - FACTS
    # --------------------------------------------------------
    ("Gold - Sales Fact", "gold_fact_sales.py"),
    ("Gold - Returns Fact", "gold_fact_returns.py"),

    # --------------------------------------------------------
    # GOLD - ANALYTICS
    # --------------------------------------------------------
    ("Gold - Sales Summary", "gold_sales_summary.py"),
    ("Gold - Monthly Sales Summary", "gold_monthly_sales_summary.py"),
    ("Gold - Product Performance", "gold_product_performance.py"),
    ("Gold - Customer Performance", "gold_customer_performance.py"),

    # --------------------------------------------------------
    # DATA QUALITY
    # --------------------------------------------------------
    ("Data Quality Checks", "data_quality/run_quality_checks.py"),
]


# ============================================================
# LOGGING
# ============================================================

def create_log_file():
    """
    Creates a timestamped pipeline log file.
    """

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    return LOG_DIR / f"pipeline_{timestamp}.log"


def write_log(log_file, message):
    """
    Writes a message to both the terminal and log file.
    """

    print(message)

    with open(log_file, "a", encoding="utf-8") as file:
        file.write(message + "\n")


# ============================================================
# RUN PIPELINE STAGE
# ============================================================

def run_stage(stage_number, total_stages, stage_name, script_name, log_file):
    """
    Executes one pipeline stage.

    Returns:
        True  -> stage succeeded
        False -> stage failed
    """

    print("\n" + "=" * 80)
    print(f"STAGE {stage_number}/{total_stages}: {stage_name}")
    print("=" * 80)

    write_log(
        log_file,
        f"\n{'=' * 80}\n"
        f"STAGE {stage_number}/{total_stages}: {stage_name}\n"
        f"Script: {script_name}\n"
        f"{'=' * 80}"
    )

    script_path = SRC_DIR / script_name

    if not script_path.exists():
        error_message = (
            f"ERROR: Script not found: {script_path}"
        )

        write_log(log_file, error_message)

        return False

    start_time = time.time()

    # --------------------------------------------------------
    # Environment
    # --------------------------------------------------------

    environment = os.environ.copy()

    # Make src/ available for imports such as:
    # from config import ...
    environment["PYTHONPATH"] = str(SRC_DIR)

    # --------------------------------------------------------
    # Execute script
    # --------------------------------------------------------

    try:

        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=PROJECT_ROOT,
            env=environment,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        # ----------------------------------------------------
        # Write complete stage output to log
        # ----------------------------------------------------

        if result.stdout:
            print(result.stdout)

            with open(log_file, "a", encoding="utf-8") as file:
                file.write(result.stdout)

        elapsed_time = time.time() - start_time

        # ----------------------------------------------------
        # Success
        # ----------------------------------------------------

        if result.returncode == 0:

            success_message = (
                f"\n✓ SUCCESS: {stage_name}"
                f" | Time: {elapsed_time:.2f} seconds"
            )

            write_log(log_file, success_message)

            return True

        # ----------------------------------------------------
        # Failure
        # ----------------------------------------------------

        failure_message = (
            f"\n✗ FAILED: {stage_name}"
            f" | Exit Code: {result.returncode}"
            f" | Time: {elapsed_time:.2f} seconds"
        )

        write_log(log_file, failure_message)

        return False

    except Exception as error:

        elapsed_time = time.time() - start_time

        error_message = (
            f"\n✗ ERROR running {stage_name}: {error}"
            f" | Time: {elapsed_time:.2f} seconds"
        )

        write_log(log_file, error_message)

        return False


# ============================================================
# MAIN PIPELINE
# ============================================================

def main():

    pipeline_start = time.time()

    total_stages = len(PIPELINE_STAGES)

    log_file = create_log_file()

    # --------------------------------------------------------
    # Header
    # --------------------------------------------------------

    header = f"""
{'=' * 80}
ADVENTUREWORKS DATA ENGINEERING PIPELINE
{'=' * 80}

Start Time   : {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Project Root : {PROJECT_ROOT}
Source Dir   : {SRC_DIR}
Log File     : {log_file}

Total Stages : {total_stages}

Pipeline Flow:
    Landing
        ↓
    Bronze
        ↓
    Silver
        ↓
    Gold
        ↓
    Data Quality
{'=' * 80}
"""

    write_log(log_file, header)

    # --------------------------------------------------------
    # Track results
    # --------------------------------------------------------

    successful_stages = []
    failed_stages = []

    # --------------------------------------------------------
    # Execute stages
    # --------------------------------------------------------

    for stage_number, (stage_name, script_name) in enumerate(
        PIPELINE_STAGES,
        start=1
    ):

        success = run_stage(
            stage_number=stage_number,
            total_stages=total_stages,
            stage_name=stage_name,
            script_name=script_name,
            log_file=log_file,
        )

        if success:

            successful_stages.append(stage_name)

        else:

            failed_stages.append(stage_name)

            # ------------------------------------------------
            # Fail-fast behavior
            # ------------------------------------------------

            write_log(
                log_file,
                "\nPIPELINE STOPPED بسبب stage failure."
            )

            break

    # --------------------------------------------------------
    # Final statistics
    # --------------------------------------------------------

    total_time = time.time() - pipeline_start

    completed_stages = len(successful_stages)
    failed_count = len(failed_stages)

    # --------------------------------------------------------
    # Final summary
    # --------------------------------------------------------

    print("\n" + "=" * 80)
    print("PIPELINE SUMMARY")
    print("=" * 80)

    print(f"Total Stages     : {total_stages}")
    print(f"Completed Stages : {completed_stages}")
    print(f"Failed Stages    : {failed_count}")
    print(f"Total Time       : {total_time:.2f} seconds")
    print(f"Total Time       : {total_time / 60:.2f} minutes")
    print(f"Log File         : {log_file}")

    # --------------------------------------------------------
    # Successful stages
    # --------------------------------------------------------

    if successful_stages:

        print("\nSuccessful Stages:")

        for stage in successful_stages:
            print(f"  ✓ {stage}")

    # --------------------------------------------------------
    # Failed stages
    # --------------------------------------------------------

    if failed_stages:

        print("\nFailed Stages:")

        for stage in failed_stages:
            print(f"  ✗ {stage}")

    # --------------------------------------------------------
    # Final status
    # --------------------------------------------------------

    if failed_count == 0:

        final_message = f"""
{'=' * 80}
PIPELINE COMPLETED SUCCESSFULLY
{'=' * 80}

Stages Completed : {completed_stages}/{total_stages}
Total Runtime    : {total_time / 60:.2f} minutes
Log File         : {log_file}

STATUS: PASSED
{'=' * 80}
"""

        write_log(log_file, final_message)

        return 0

    else:

        final_message = f"""
{'=' * 80}
PIPELINE FAILED
{'=' * 80}

Stages Completed : {completed_stages}/{total_stages}
Failed Stages    : {failed_count}
Total Runtime    : {total_time / 60:.2f} minutes
Log File         : {log_file}

STATUS: FAILED
{'=' * 80}
"""

        write_log(log_file, final_message)

        return 1


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    sys.exit(main())
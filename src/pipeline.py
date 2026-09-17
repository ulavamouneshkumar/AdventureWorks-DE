

import os
import subprocess
import sys
import time
from datetime import datetime

from config import (
    PROJECT_ROOT,
    SRC_DIR,
    LOG_DIR,
    MAX_RETRIES,
    RETRY_DELAY_SECONDS,
)


# ============================================================
# PIPELINE STAGES
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

       # ========================================================
    # DATA QUALITY
    # ========================================================

    ("Data Quality Checks", "data_quality/run_quality_checks.py"),

    # ========================================================
    # MONITORING
    # ========================================================

    ("Row Count Monitoring", "monitoring/metrics.py"),
    ("Data Freshness Monitoring", "monitoring/freshness.py"),
]


# ============================================================
# LOGGING
# ============================================================

def create_log_file():
    """
    Create a timestamped pipeline log file.
    """

    LOG_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    timestamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    return LOG_DIR / f"pipeline_{timestamp}.log"


def write_log(log_file, message):
    """
    Write message to both terminal and log file.
    """

    print(message)

    with open(
        log_file,
        "a",
        encoding="utf-8"
    ) as file:

        file.write(message + "\n")


# ============================================================
# EXECUTE ONE STAGE ATTEMPT
# ============================================================

def execute_stage(
    stage_number,
    total_stages,
    stage_name,
    script_name,
    attempt,
    total_attempts,
    log_file,
):
    """
    Execute one attempt of a pipeline stage.

    Returns:
        (success, elapsed_time)
    """

    script_path = SRC_DIR / script_name

    start_time = time.time()

    # --------------------------------------------------------
    # Stage Header
    # --------------------------------------------------------

    stage_header = (
        f"\n{'=' * 80}\n"
        f"STAGE {stage_number}/{total_stages}: {stage_name}\n"
        f"Script       : {script_name}\n"
        f"Attempt      : {attempt}/{total_attempts}\n"
        f"Start Time   : "
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"{'=' * 80}"
    )

    write_log(
        log_file,
        stage_header
    )

    # --------------------------------------------------------
    # Check Script Exists
    # --------------------------------------------------------

    if not script_path.exists():

        elapsed_time = time.time() - start_time

        write_log(
            log_file,
            (
                f"ERROR: Script not found\n"
                f"Path     : {script_path}\n"
                f"Duration : {elapsed_time:.2f} seconds"
            )
        )

        return False, elapsed_time

    # --------------------------------------------------------
    # Environment
    # --------------------------------------------------------

    environment = os.environ.copy()

    environment["PYTHONPATH"] = str(SRC_DIR)

    # --------------------------------------------------------
    # Execute Script
    # --------------------------------------------------------

    try:

        result = subprocess.run(
            [
                sys.executable,
                str(script_path)
            ],
            cwd=PROJECT_ROOT,
            env=environment,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        # ----------------------------------------------------
        # Capture Script Output
        # ----------------------------------------------------

        if result.stdout:

            print(result.stdout)

            with open(
                log_file,
                "a",
                encoding="utf-8"
            ) as file:

                file.write(result.stdout)

        elapsed_time = time.time() - start_time

        # ----------------------------------------------------
        # SUCCESS
        # ----------------------------------------------------

        if result.returncode == 0:

            write_log(
                log_file,
                (
                    f"\n✓ SUCCESS\n"
                    f"Stage    : {stage_name}\n"
                    f"Attempt  : {attempt}/{total_attempts}\n"
                    f"Duration : {elapsed_time:.2f} seconds"
                )
            )

            return True, elapsed_time

        # ----------------------------------------------------
        # FAILURE
        # ----------------------------------------------------

        write_log(
            log_file,
            (
                f"\n✗ FAILED\n"
                f"Stage     : {stage_name}\n"
                f"Attempt   : {attempt}/{total_attempts}\n"
                f"Exit Code : {result.returncode}\n"
                f"Duration  : {elapsed_time:.2f} seconds"
            )
        )

        return False, elapsed_time

    except Exception as error:

        elapsed_time = time.time() - start_time

        write_log(
            log_file,
            (
                f"\n✗ EXCEPTION\n"
                f"Stage     : {stage_name}\n"
                f"Attempt   : {attempt}/{total_attempts}\n"
                f"Error     : {error}\n"
                f"Duration  : {elapsed_time:.2f} seconds"
            )
        )

        return False, elapsed_time


# ============================================================
# RUN STAGE WITH RETRY
# ============================================================

def run_stage(
    stage_number,
    total_stages,
    stage_name,
    script_name,
    log_file,
):
    """
    Execute a pipeline stage with retry support.

    Example:
        MAX_RETRIES = 2

    Total attempts:
        Attempt 1
        Attempt 2
        Attempt 3

    Returns:
        (success, duration, attempts_used)
    """

    total_attempts = MAX_RETRIES + 1

    last_duration = 0.0

    for attempt in range(
        1,
        total_attempts + 1
    ):

        success, elapsed_time = execute_stage(
            stage_number=stage_number,
            total_stages=total_stages,
            stage_name=stage_name,
            script_name=script_name,
            attempt=attempt,
            total_attempts=total_attempts,
            log_file=log_file,
        )

        last_duration = elapsed_time

        # ----------------------------------------------------
        # SUCCESS
        # ----------------------------------------------------

        if success:

            return (
                True,
                elapsed_time,
                attempt
            )

        # ----------------------------------------------------
        # FINAL FAILURE
        # ----------------------------------------------------

        if attempt == total_attempts:

            write_log(
                log_file,
                (
                    f"\n✗ FINAL FAILURE\n"
                    f"Stage: {stage_name}\n"
                    f"All {total_attempts} attempts failed."
                )
            )

            return (
                False,
                last_duration,
                attempt
            )

        # ----------------------------------------------------
        # RETRY
        # ----------------------------------------------------

        next_attempt = attempt + 1

        write_log(
            log_file,
            (
                f"\nRetrying stage...\n"
                f"Stage        : {stage_name}\n"
                f"Next Attempt : "
                f"{next_attempt}/{total_attempts}\n"
                f"Waiting      : "
                f"{RETRY_DELAY_SECONDS} seconds"
            )
        )

        time.sleep(
            RETRY_DELAY_SECONDS
        )

    return (
        False,
        last_duration,
        total_attempts
    )


# ============================================================
# PRINT STAGE METRICS
# ============================================================

def print_stage_metrics(
    stage_metrics,
    log_file,
):
    """
    Print stage execution metrics.
    """

    write_log(
        log_file,
        f"\n{'=' * 100}"
    )

    write_log(
        log_file,
        "STAGE EXECUTION METRICS"
    )

    write_log(
        log_file,
        f"{'=' * 100}"
    )

    write_log(
        log_file,
        (
            f"{'Stage':<42}"
            f"{'Status':<12}"
            f"{'Attempts':<12}"
            f"{'Duration':<15}"
        )
    )

    write_log(
        log_file,
        "-" * 100
    )

    for metric in stage_metrics:

        stage_name = metric["stage_name"]
        status = metric["status"]
        attempts = metric["attempts"]
        duration = metric["duration_seconds"]

        write_log(
            log_file,
            (
                f"{stage_name:<42}"
                f"{status:<12}"
                f"{attempts:<12}"
                f"{duration:.2f}s"
            )
        )

    write_log(
        log_file,
        "-" * 100
    )


# ============================================================
# MAIN PIPELINE
# ============================================================

def main():

    pipeline_start = time.time()

    total_stages = len(
        PIPELINE_STAGES
    )

    # --------------------------------------------------------
    # Generate Run ID
    # --------------------------------------------------------

    run_id = datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    # --------------------------------------------------------
    # Create Log File
    # --------------------------------------------------------

    log_file = create_log_file()

    # --------------------------------------------------------
    # Metrics Collection
    # --------------------------------------------------------

    stage_metrics = []

    successful_stages = []

    failed_stages = []

    # --------------------------------------------------------
    # Pipeline Header
    # --------------------------------------------------------

    header = f"""
{'=' * 80}
ADVENTUREWORKS DATA ENGINEERING PIPELINE
{'=' * 80}

Pipeline Run ID : {run_id}

Start Time      : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Project Root    : {PROJECT_ROOT}
Source Directory: {SRC_DIR}
Log File        : {log_file}

Total Stages    : {total_stages}

Max Retries     : {MAX_RETRIES}
Total Attempts  : {MAX_RETRIES + 1}
Retry Delay     : {RETRY_DELAY_SECONDS} seconds

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

    write_log(
        log_file,
        header
    )

    # --------------------------------------------------------
    # Execute Pipeline Stages
    # --------------------------------------------------------

    for stage_number, (
        stage_name,
        script_name
    ) in enumerate(
        PIPELINE_STAGES,
        start=1
    ):

        success, duration, attempts_used = run_stage(
            stage_number=stage_number,
            total_stages=total_stages,
            stage_name=stage_name,
            script_name=script_name,
            log_file=log_file,
        )

        # ----------------------------------------------------
        # Store Metrics
        # ----------------------------------------------------

        stage_metrics.append(
            {
                "stage_number": stage_number,
                "stage_name": stage_name,
                "status": (
                    "SUCCESS"
                    if success
                    else "FAILED"
                ),
                "duration_seconds": duration,
                "attempts": attempts_used,
            }
        )

        # ----------------------------------------------------
        # Track Stage Status
        # ----------------------------------------------------

        if success:

            successful_stages.append(
                stage_name
            )

        else:

            failed_stages.append(
                stage_name
            )

            # ------------------------------------------------
            # FAIL FAST
            # ------------------------------------------------

            write_log(
                log_file,
                (
                    "\nPIPELINE STOPPED "
                    "DUE TO STAGE FAILURE."
                )
            )

            break

    # ========================================================
    # FINAL STATISTICS
    # ========================================================

    total_time = (
        time.time() - pipeline_start
    )

    completed_stages = len(
        successful_stages
    )

    failed_count = len(
        failed_stages
    )

    # --------------------------------------------------------
    # Stage Metrics
    # --------------------------------------------------------

    print_stage_metrics(
        stage_metrics,
        log_file
    )

    # ========================================================
    # PIPELINE SUMMARY
    # ========================================================

    summary = f"""
{'=' * 80}
PIPELINE SUMMARY
{'=' * 80}

Pipeline Run ID  : {run_id}

Total Stages     : {total_stages}
Completed Stages : {completed_stages}
Failed Stages    : {failed_count}

Total Runtime    : {total_time:.2f} seconds
Total Runtime    : {total_time / 60:.2f} minutes

Log File         : {log_file}
"""

    write_log(
        log_file,
        summary
    )

    # ========================================================
    # SUCCESSFUL STAGES
    # ========================================================

    if successful_stages:

        write_log(
            log_file,
            "\nSuccessful Stages:"
        )

        for stage in successful_stages:

            write_log(
                log_file,
                f"  ✓ {stage}"
            )

    # ========================================================
    # FAILED STAGES
    # ========================================================

    if failed_stages:

        write_log(
            log_file,
            "\nFailed Stages:"
        )

        for stage in failed_stages:

            write_log(
                log_file,
                f"  ✗ {stage}"
            )

    # ========================================================
    # FINAL STATUS
    # ========================================================

    if failed_count == 0:

        final_message = f"""
{'=' * 80}
PIPELINE COMPLETED SUCCESSFULLY
{'=' * 80}

Pipeline Run ID  : {run_id}

Stages Completed : {completed_stages}/{total_stages}

Total Runtime    : {total_time / 60:.2f} minutes

Log File         : {log_file}

STATUS: PASSED

{'=' * 80}
"""

        write_log(
            log_file,
            final_message
        )

        return 0

    # ========================================================
    # FAILED PIPELINE
    # ========================================================

    final_message = f"""
{'=' * 80}
PIPELINE FAILED
{'=' * 80}

Pipeline Run ID  : {run_id}

Stages Completed : {completed_stages}/{total_stages}
Failed Stages    : {failed_count}

Total Runtime    : {total_time / 60:.2f} minutes

Log File         : {log_file}

STATUS: FAILED

{'=' * 80}
"""

    write_log(
        log_file,
        final_message
    )

    return 1


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    sys.exit(
        main()
    )
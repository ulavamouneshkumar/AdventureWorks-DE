import subprocess
import sys
import time
import logging
from pathlib import Path
from datetime import datetime


# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SRC_DIR = PROJECT_ROOT / "src"

LOG_DIR = PROJECT_ROOT / "logs"

LOG_DIR.mkdir(
    parents=True,
    exist_ok=True
)

LOG_FILE = (
    LOG_DIR /
    f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)


# ============================================================
# LOGGING CONFIGURATION
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(message)s"
    ),
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("AdventureWorksPipeline")


# ============================================================
# PIPELINE STAGES
# ============================================================

PIPELINE_STAGES = [

    # --------------------------------------------------------
    # BRONZE
    # --------------------------------------------------------

    (
        "Bronze Ingestion - Dimensions",
        "bronze_ingestion.py"
    ),

    (
        "Bronze Ingestion - Sales",
        "sales_bronze_ingestion.py"
    ),


    # --------------------------------------------------------
    # SILVER
    # --------------------------------------------------------

    (
        "Silver Calendar",
        "silver_calendar.py"
    ),

    (
        "Silver Customers",
        "silver_customers.py"
    ),

    (
        "Silver Product Categories",
        "silver_product_categories.py"
    ),

    (
        "Silver Product Subcategories",
        "silver_product_subcategories.py"
    ),

    (
        "Silver Products",
        "silver_products.py"
    ),

    (
        "Silver Territories",
        "silver_territories.py"
    ),

    (
        "Silver Returns",
        "silver_returns.py"
    ),

    (
        "Silver Sales",
        "silver_sales.py"
    ),


    # --------------------------------------------------------
    # GOLD DIMENSIONS
    # --------------------------------------------------------

    (
        "Gold Dimension Date",
        "gold_dim_date.py"
    ),

    (
        "Gold Dimension Customer",
        "gold_dim_customer.py"
    ),

    (
        "Gold Dimension Product",
        "gold_dim_product.py"
    ),

    (
        "Gold Dimension Territory",
        "gold_dim_territory.py"
    ),


    # --------------------------------------------------------
    # GOLD FACTS
    # --------------------------------------------------------

    (
        "Gold Fact Sales",
        "gold_fact_sales.py"
    ),

    (
        "Gold Fact Returns",
        "gold_fact_returns.py"
    ),


    # --------------------------------------------------------
    # GOLD ANALYTICS
    # --------------------------------------------------------

    (
        "Gold Sales Summary",
        "gold_sales_summary.py"
    ),

    (
        "Gold Monthly Sales Summary",
        "gold_monthly_sales_summary.py"
    ),

    (
        "Gold Product Performance",
        "gold_product_performance.py"
    ),

    (
        "Gold Customer Performance",
        "gold_customer_performance.py"
    ),


    # --------------------------------------------------------
    # DATA QUALITY
    # --------------------------------------------------------

    (
        "Data Quality Framework",
        "data_quality/run_quality_checks.py"
    ),
]


# ============================================================
# PIPELINE HEADER
# ============================================================

def print_header():

    logger.info("")
    logger.info("=" * 80)
    logger.info("ADVENTUREWORKS DATA ENGINEERING PIPELINE")
    logger.info("=" * 80)

    logger.info(
        "Pipeline started at: %s",
        datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )

    logger.info(
        "Project root: %s",
        PROJECT_ROOT
    )

    logger.info(
        "Total stages: %d",
        len(PIPELINE_STAGES)
    )

    logger.info("=" * 80)


# ============================================================
# RUN SINGLE STAGE
# ============================================================

def run_stage(
    stage_number,
    stage_name,
    script_name
):

    script_path = SRC_DIR / script_name

    logger.info("")
    logger.info("=" * 80)

    logger.info(
        "STAGE %d/%d",
        stage_number,
        len(PIPELINE_STAGES)
    )

    logger.info(
        "Stage: %s",
        stage_name
    )

    logger.info(
        "Script: %s",
        script_name
    )

    logger.info("=" * 80)


    # --------------------------------------------------------
    # Check script exists
    # --------------------------------------------------------

    if not script_path.exists():

        logger.error(
            "Script not found: %s",
            script_path
        )

        return False, 0


    # --------------------------------------------------------
    # Start timer
    # --------------------------------------------------------

    start_time = time.perf_counter()


    try:

        logger.info(
            "Starting stage: %s",
            stage_name
        )


        # ----------------------------------------------------
        # Execute Python script
        # ----------------------------------------------------

        result = subprocess.run(
            [
                sys.executable,
                str(script_path)
            ],

            cwd=PROJECT_ROOT,

            env={
                **__import__("os").environ,
                "PYTHONPATH": str(SRC_DIR)
            },

            text=True,

            check=False
        )


        # ----------------------------------------------------
        # Calculate execution time
        # ----------------------------------------------------

        elapsed_time = (
            time.perf_counter()
            - start_time
        )


        # ----------------------------------------------------
        # Check result
        # ----------------------------------------------------

        if result.returncode == 0:

            logger.info(
                "Stage completed successfully: %s",
                stage_name
            )

            logger.info(
                "Execution time: %.2f seconds",
                elapsed_time
            )

            return True, elapsed_time


        else:

            logger.error(
                "Stage FAILED: %s",
                stage_name
            )

            logger.error(
                "Exit code: %d",
                result.returncode
            )

            logger.error(
                "Execution time: %.2f seconds",
                elapsed_time
            )

            return False, elapsed_time


    except Exception as error:

        elapsed_time = (
            time.perf_counter()
            - start_time
        )

        logger.exception(
            "Unexpected error in stage '%s': %s",
            stage_name,
            error
        )

        return False, elapsed_time


# ============================================================
# MAIN PIPELINE
# ============================================================

def main():

    pipeline_start = time.perf_counter()

    print_header()


    stage_results = []


    # ========================================================
    # EXECUTE STAGES
    # ========================================================

    for stage_number, (
        stage_name,
        script_name
    ) in enumerate(
        PIPELINE_STAGES,
        start=1
    ):

        success, elapsed_time = run_stage(
            stage_number,
            stage_name,
            script_name
        )


        stage_results.append(
            {
                "stage": stage_name,
                "script": script_name,
                "status": (
                    "PASSED"
                    if success
                    else "FAILED"
                ),
                "execution_time": elapsed_time
            }
        )


        # ----------------------------------------------------
        # STOP PIPELINE ON FAILURE
        # ----------------------------------------------------

        if not success:

            logger.error("")
            logger.error("=" * 80)

            logger.error(
                "PIPELINE FAILED"
            )

            logger.error(
                "Failed stage: %s",
                stage_name
            )

            logger.error(
                "Pipeline stopped to prevent downstream "
                "processing on invalid data."
            )

            logger.error("=" * 80)

            return 1


    # ========================================================
    # PIPELINE COMPLETED
    # ========================================================

    total_time = (
        time.perf_counter()
        - pipeline_start
    )


    logger.info("")
    logger.info("=" * 80)
    logger.info("PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 80)


    for result in stage_results:

        logger.info(
            "%-40s | %-7s | %.2f sec",
            result["stage"],
            result["status"],
            result["execution_time"]
        )


    # ========================================================
    # FINAL STATUS
    # ========================================================

    logger.info("")
    logger.info("=" * 80)

    logger.info(
        "Total stages executed : %d",
        len(stage_results)
    )

    logger.info(
        "Passed stages         : %d",
        sum(
            1
            for result in stage_results
            if result["status"] == "PASSED"
        )
    )

    logger.info(
        "Failed stages         : %d",
        sum(
            1
            for result in stage_results
            if result["status"] == "FAILED"
        )
    )

    logger.info(
        "Total pipeline time    : %.2f seconds",
        total_time
    )

    logger.info(
        "Pipeline log           : %s",
        LOG_FILE
    )

    logger.info("")
    logger.info(
        "OVERALL PIPELINE STATUS : PASSED"
    )

    logger.info("=" * 80)


    return 0


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    sys.exit(
        main()
    )
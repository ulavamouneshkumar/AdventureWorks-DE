from pathlib import Path


# ============================================================
# PROJECT ROOT
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SRC_DIR = PROJECT_ROOT / "src"
# ============================================================
# DATA DIRECTORIES
# ============================================================

DATA_DIR = PROJECT_ROOT / "data"

LANDING_DIR = DATA_DIR / "landing"

BRONZE_DIR = DATA_DIR / "bronze"

SILVER_DIR = DATA_DIR / "silver"

GOLD_DIR = DATA_DIR / "gold"


# ============================================================
# LOG DIRECTORY
# ============================================================

LOG_DIR = PROJECT_ROOT / "logs"


# ============================================================
# SOURCE FILES
# ============================================================

SOURCE_FILES = {

    "calendar":
        LANDING_DIR /
        "AdventureWorks_Calendar.csv",

    "customers":
        LANDING_DIR /
        "AdventureWorks_Customers.csv",

    "product_categories":
        LANDING_DIR /
        "AdventureWorks_Product_Categories.csv",

    "product_subcategories":
        LANDING_DIR /
        "AdventureWorks_Product_Subcategories.csv",

    "products":
        LANDING_DIR /
        "AdventureWorks_Products.csv",

    "returns":
        LANDING_DIR /
        "AdventureWorks_Returns.csv",

    "territories":
        LANDING_DIR /
        "AdventureWorks_Territories.csv",

    "sales_2015":
        LANDING_DIR /
        "AdventureWorks_Sales_2015.csv",

    "sales_2016":
        LANDING_DIR /
        "AdventureWorks_Sales_2016.csv",

    "sales_2017":
        LANDING_DIR /
        "AdventureWorks_Sales_2017.csv",
}


# ============================================================
# BRONZE TABLE LOCATIONS
# ============================================================

BRONZE_TABLES = {

    "calendar":
        BRONZE_DIR / "calendar",

    "customers":
        BRONZE_DIR / "customers",

    "product_categories":
        BRONZE_DIR / "product_categories",

    "product_subcategories":
        BRONZE_DIR / "product_subcategories",

    "products":
        BRONZE_DIR / "products",

    "returns":
        BRONZE_DIR / "returns",

    "territories":
        BRONZE_DIR / "territories",

    "sales":
        BRONZE_DIR / "sales",
}


# ============================================================
# SILVER TABLE LOCATIONS
# ============================================================

SILVER_TABLES = {

    "calendar":
        SILVER_DIR / "calendar",

    "customers":
        SILVER_DIR / "customers",

    "product_categories":
        SILVER_DIR / "product_categories",

    "product_subcategories":
        SILVER_DIR / "product_subcategories",

    "products":
        SILVER_DIR / "products",

    "returns":
        SILVER_DIR / "returns",

    "territories":
        SILVER_DIR / "territories",

    "sales":
        SILVER_DIR / "sales",
}


# ============================================================
# GOLD TABLE LOCATIONS
# ============================================================

GOLD_TABLES = {

    "dim_date":
        GOLD_DIR / "dim_date",

    "dim_customer":
        GOLD_DIR / "dim_customer",

    "dim_product":
        GOLD_DIR / "dim_product",

    "dim_territory":
        GOLD_DIR / "dim_territory",

    "fact_sales":
        GOLD_DIR / "fact_sales",

    "fact_returns":
        GOLD_DIR / "fact_returns",

    "sales_summary":
        GOLD_DIR / "sales_summary",

    "monthly_sales_summary":
        GOLD_DIR / "monthly_sales_summary",

    "product_performance":
        GOLD_DIR / "product_performance",

    "customer_performance":
        GOLD_DIR / "customer_performance",
}
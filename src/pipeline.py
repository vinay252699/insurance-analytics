import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark import StorageLevel

from transformations import (
    clean_customers,
    enrich_claims,
    prepare_products,
    resolve_product_versions
)

from analytics import build_analytics_marts

from data_quality import (
    profile_claims,
    profile_customers,
    profile_products,
    profile_fact_claims
)


# ============================================================
# Configuration
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RULES_PATH = PROJECT_ROOT / "config" / "business_rules.json"


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Insurance Analytics PySpark Pipeline"
    )

    parser.add_argument(
        "--claims",
        required=True,
        help="Path to claims parquet dataset"
    )

    parser.add_argument(
        "--customers",
        required=True,
        help="Path to customers parquet dataset"
    )

    parser.add_argument(
        "--products",
        required=True,
        help="Path to products parquet dataset"
    )

    parser.add_argument(
        "--output",
        required=True,
        help="Output directory for analytics marts"
    )

    parser.add_argument(
        "--rules",
        default=str(DEFAULT_RULES_PATH),
        help="Business rules JSON file"
    )

    parser.add_argument(
        "--master",
        default=None,
        help="Optional Spark master URL"
    )

    return parser.parse_args()


def load_business_rules(rules_path):
    with open(rules_path, "r", encoding="utf-8") as file:
        return json.load(file)


# ============================================================
# Spark
# ============================================================

def create_spark_session(master_url=None):

    builder = (
        SparkSession.builder
        .appName("InsuranceAnalyticsPipeline")
    )

    if master_url:
        builder = builder.master(master_url)

    spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ============================================================
# Pipeline
# ============================================================

def run_pipeline(args):

    rules = load_business_rules(args.rules)

    allowed_statuses = list(
        rules["claim_status"].values()
    )

    allowed_claim_types = sorted({
        product_code
        for product_codes in rules["portfolios"].values()
        for product_code in product_codes
    })

    allowed_customer_tiers = rules.get(
        "customer_tiers",
        []
    )

    spark = create_spark_session(args.master)

    try:

        # ----------------------------------------------------
        # Read
        # ----------------------------------------------------

        print("=" * 60)
        print("READING SOURCE DATA")
        print("=" * 60)

        claims = spark.read.parquet(
            args.claims
        )

        customers = spark.read.parquet(
            args.customers
        )

        products = spark.read.parquet(
            args.products
        )

        print("Source datasets loaded.")

        # ----------------------------------------------------
        # Data Quality
        # ----------------------------------------------------

        print("=" * 60)
        print("DATA QUALITY")
        print("=" * 60)

        claims_dq = profile_claims(
            claims,
            allowed_statuses,
            allowed_claim_types
        )

        customers_dq = profile_customers(
            customers,
            allowed_customer_tiers
        )

        products_prepared = prepare_products(products)

        products_dq = profile_products(
            products_prepared
        )

        print("Claims DQ:")
        print(claims_dq)

        print("Customers DQ:")
        print(customers_dq)

        print("Products DQ:")
        print(products_dq)

        # ----------------------------------------------------
        # Transform
        # ----------------------------------------------------

        print("=" * 60)
        print("TRANSFORMATIONS")
        print("=" * 60)

        clean_customers_df = clean_customers(
            customers
        )

        claims_enriched_df = enrich_claims(
            claims,
            clean_customers_df
        )

        fact_claims_df = resolve_product_versions(
            claims_enriched_df,
            products_prepared
        )

        # ----------------------------------------------------
        # Persist Fact
        # ----------------------------------------------------

        fact_claims_df = fact_claims_df.persist(
            StorageLevel.MEMORY_AND_DISK
        )

        raw_claim_count = claims_dq["row_count"]

        fact_dq = profile_fact_claims(
            fact_claims_df,
            raw_claim_count
        )

        print("=" * 60)
        print("FACT VALIDATION")
        print("=" * 60)

        print(fact_dq)

        # ----------------------------------------------------
        # Fail Fast on Structural Problems
        # ----------------------------------------------------

        if not fact_dq["row_count_reconciliation"]:
            raise RuntimeError(
                "Fact row count does not reconcile with raw claims."
            )

        if fact_dq["duplicate_claim_ids"] > 0:
            raise RuntimeError(
                "Duplicate claim IDs detected in fact table."
            )

        # ----------------------------------------------------
        # Analytics
        # ----------------------------------------------------

        print("=" * 60)
        print("BUILDING ANALYTICS MARTS")
        print("=" * 60)

        analytics = build_analytics_marts(
            fact_claims_df
        )

        # ----------------------------------------------------
        # Write Outputs
        # ----------------------------------------------------

        print("=" * 60)
        print("WRITING OUTPUTS")
        print("=" * 60)

        output_root = Path(args.output)

        for name, dataframe in analytics.items():

            output_path = str(
                output_root / name
            )

            print(f"Writing: {name}")

            (
                dataframe.write
                .mode("overwrite")
                .parquet(output_path)
            )

        print("=" * 60)
        print("PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 60)

        print(
            f"Output location: {args.output}"
        )

    finally:

        spark.stop()


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":

    arguments = parse_arguments()

    run_pipeline(arguments)
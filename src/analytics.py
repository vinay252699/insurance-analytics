import json
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# CONFIGURATION
# ============================================================

CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "business_rules.json"

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    BUSINESS_RULES = json.load(f)

PORTFOLIOS = BUSINESS_RULES["portfolios"]
STATUS = BUSINESS_RULES["claim_status"]
UNKNOWN_TIER = BUSINESS_RULES["unknown_customer_tier"]


# ============================================================
# COMMON METRICS
# ============================================================

def approval_rate():
    """
    Calculate approval percentage safely.

    Expects:
        approved_claims
        claim_count
    """
    return F.round(
        F.when(
            F.col("claim_count") > 0,
            F.col("approved_claims") / F.col("claim_count") * 100
        ).otherwise(F.lit(0)),
        2
    )


def status_count(status_value):
    """
    Create a conditional claim count for a configured status.
    """
    return F.sum(
        F.when(F.col("status") == status_value, 1)
         .otherwise(0)
    )


# ============================================================
# EXECUTIVE KPIs
# ============================================================

def calculate_kpis(fact_claims):

    return (
        fact_claims
        .agg(
            F.count("claim_id").alias("total_claims"),

            F.sum("claim_amount")
             .alias("total_claim_amount"),

            F.avg("claim_amount")
             .alias("average_claim_amount"),

            status_count(STATUS["approved"])
             .alias("approved_claims"),

            status_count(STATUS["rejected"])
             .alias("rejected_claims"),

            status_count(STATUS["pending"])
             .alias("pending_claims")
        )
        .withColumn(
            "approval_rate",
            F.round(
                F.when(
                    F.col("total_claims") > 0,
                    F.col("approved_claims")
                    / F.col("total_claims") * 100
                ).otherwise(F.lit(0)),
                2
            )
        )
        .withColumn(
            "rejection_rate",
            F.round(
                F.when(
                    F.col("total_claims") > 0,
                    F.col("rejected_claims")
                    / F.col("total_claims") * 100
                ).otherwise(F.lit(0)),
                2
            )
        )
        .withColumn(
            "pending_rate",
            F.round(
                F.when(
                    F.col("total_claims") > 0,
                    F.col("pending_claims")
                    / F.col("total_claims") * 100
                ).otherwise(F.lit(0)),
                2
            )
        )
    )


# ============================================================
# PRODUCT ANALYTICS
# ============================================================

def claims_by_product(fact_claims):

    return (
        fact_claims
        .groupBy(
            "product_code",
            "product_name",
            "product_version",
            "risk_category"
        )
        .agg(
            F.count("claim_id").alias("claim_count"),

            F.sum("claim_amount")
             .alias("total_claim_amount"),

            F.avg("claim_amount")
             .alias("average_claim_amount"),

            status_count(STATUS["approved"])
             .alias("approved_claims"),

            status_count(STATUS["rejected"])
             .alias("rejected_claims"),

            F.min("avg_premium")
             .alias("avg_premium"),

            F.min("commission_rate")
             .alias("commission_rate")
        )
        .withColumn(
            "approval_rate",
            approval_rate()
        )
        .withColumn(
            "estimated_premium",
            F.col("claim_count") * F.col("avg_premium")
        )
        .withColumn(
            "estimated_commission",
            F.col("estimated_premium")
            * F.col("commission_rate")
            / 100
        )
    )


# ============================================================
# RISK ANALYTICS
# ============================================================

def claims_by_risk(fact_claims):

    return (
        fact_claims
        .groupBy("risk_category")
        .agg(
            F.count("claim_id").alias("claim_count"),

            F.sum("claim_amount")
             .alias("total_claim_amount"),

            F.avg("claim_amount")
             .alias("average_claim_amount"),

            status_count(STATUS["approved"])
             .alias("approved_claims")
        )
        .withColumn(
            "approval_rate",
            approval_rate()
        )
    )


# ============================================================
# CUSTOMER TIER ANALYTICS
# ============================================================

def claims_by_customer_tier(fact_claims):

    return (
        fact_claims
        .withColumn(
            "customer_tier",
            F.coalesce(
                F.col("tier"),
                F.lit(UNKNOWN_TIER)
            )
        )
        .groupBy("customer_tier")
        .agg(
            F.count("claim_id").alias("claim_count"),

            F.sum("claim_amount")
             .alias("total_claim_amount"),

            F.avg("claim_amount")
             .alias("average_claim_amount"),

            F.avg("lifetime_value")
             .alias("average_customer_lifetime_value"),

            F.countDistinct("customer_id")
             .alias("unique_customers")
        )
        .withColumn(
            "claims_per_customer",
            F.round(
                F.when(
                    F.col("unique_customers") > 0,
                    F.col("claim_count")
                    / F.col("unique_customers")
                ).otherwise(F.lit(0)),
                2
            )
        )
    )


# ============================================================
# GEOGRAPHIC ANALYTICS
# ============================================================

def claims_by_country(fact_claims):

    return (
        fact_claims
        .groupBy("claim_country")
        .agg(
            F.count("claim_id").alias("claim_count"),

            F.sum("claim_amount")
             .alias("total_claim_amount"),

            F.avg("claim_amount")
             .alias("average_claim_amount"),

            status_count(STATUS["approved"])
             .alias("approved_claims"),

            status_count(STATUS["rejected"])
             .alias("rejected_claims")
        )
        .withColumn(
            "approval_rate",
            approval_rate()
        )
    )


# ============================================================
# STATUS ANALYTICS
# ============================================================

def claims_by_status(fact_claims):
    """
    Claims and financial metrics by claim status.
    """

    status_summary = (
        fact_claims
        .groupBy("status")
        .agg(
            F.count("claim_id").alias("claim_count"),
            F.sum("claim_amount").alias("total_claim_amount"),
            F.avg("claim_amount").alias("average_claim_amount")
        )
    )

    total_summary = (
        status_summary
        .agg(
            F.sum("claim_count").alias("_total_claims")
        )
    )

    return (
        status_summary
        .crossJoin(total_summary)
        .withColumn(
            "claim_percentage",
            F.round(
                F.when(
                    F.col("_total_claims") > 0,
                    F.col("claim_count")
                    / F.col("_total_claims")
                    * 100
                ).otherwise(F.lit(0)),
                2
            )
        )
        .drop("_total_claims")
    )

# ============================================================
# MONTHLY ANALYTICS
# ============================================================

def claims_by_month(fact_claims):

    return (
        fact_claims
        .withColumn(
            "claim_month",
            F.date_format(
                F.col("claim_date"),
                "yyyy-MM"
            )
        )
        .groupBy("claim_month")
        .agg(
            F.count("claim_id").alias("claim_count"),

            F.sum("claim_amount")
             .alias("total_claim_amount"),

            F.avg("claim_amount")
             .alias("average_claim_amount"),

            status_count(STATUS["approved"])
             .alias("approved_claims")
        )
        .withColumn(
            "approval_rate",
            approval_rate()
        )
    )


# ============================================================
# CUSTOMER SUMMARY
# ============================================================

def customer_claim_summary(fact_claims):

    return (
        fact_claims
        .groupBy(
            "customer_id",
            "customer_name",
            "tier",
            "claim_country"
        )
        .agg(
            F.count("claim_id").alias("claim_count"),

            F.sum("claim_amount")
             .alias("total_claim_amount"),

            F.avg("claim_amount")
             .alias("average_claim_amount"),

            F.max("claim_date")
             .alias("latest_claim_date"),

            F.first("lifetime_value")
             .alias("lifetime_value")
        )
    )


# ============================================================
# Q1 — HEALTH & LIFE PORTFOLIO
# ============================================================

def health_life_portfolio(fact_claims):
    """
    Health & Life portfolio analytics.

    Includes all configured customer tiers and preserves
    zero-claim tiers in the final output.
    """

    products = PORTFOLIOS["health_life"]

    portfolio = (
        fact_claims
        .filter(
            F.col("product_code").isin(*products)
        )
        .withColumn(
            "customer_tier",
            F.coalesce(
                F.col("tier"),
                F.lit(UNKNOWN_TIER)
            )
        )
    )

    aggregated = (
        portfolio
        .groupBy(
            "customer_tier",
            "claim_country",
            "product_code",
            "product_name"
        )
        .agg(
            F.count("claim_id").alias("claim_count"),
            F.sum("claim_amount").alias("total_claim_amount"),
            F.avg("claim_amount").alias("average_claim_amount"),

            status_count(
                STATUS["approved"]
            ).alias("approved_claims"),

            status_count(
                STATUS["rejected"]
            ).alias("rejected_claims"),

            status_count(
                STATUS["pending"]
            ).alias("pending_claims")
        )
        .withColumn(
            "approval_rate",
            approval_rate()
        )
    )

    return aggregated


# ============================================================
# Q2 — PROPERTY & CASUALTY PORTFOLIO
# ============================================================

def property_casualty_portfolio(fact_claims):

    products = PORTFOLIOS["property_casualty"]

    return (
        fact_claims
        .filter(
            F.col("product_code").isin(*products)
        )
        .groupBy(
            "claim_country",
            "risk_category",
            "product_code",
            "product_name",
            "product_version"
        )
        .agg(
            F.count("claim_id").alias("claim_count"),

            F.sum("claim_amount")
             .alias("total_claim_amount"),

            F.avg("claim_amount")
             .alias("average_claim_amount"),

            status_count(STATUS["approved"])
             .alias("approved_claims"),

            status_count(STATUS["rejected"])
             .alias("rejected_claims"),

            F.min("avg_premium")
             .alias("avg_premium"),

            F.min("commission_rate")
             .alias("commission_rate")
        )
        .withColumn(
            "approval_rate",
            approval_rate()
        )
        .withColumn(
            "estimated_premium",
            F.col("claim_count")
            * F.col("avg_premium")
        )
        .withColumn(
            "estimated_commission",
            F.col("estimated_premium")
            * F.col("commission_rate")
            / 100
        )
    )


# ============================================================
# BUILD ALL ANALYTICS MARTS
# ============================================================

def build_analytics_marts(fact_claims):

    return {
        "kpi_summary": calculate_kpis(fact_claims),

        "product_summary": claims_by_product(fact_claims),

        "risk_summary": claims_by_risk(fact_claims),

        "customer_tier_summary":
            claims_by_customer_tier(fact_claims),

        "country_summary":
            claims_by_country(fact_claims),

        "status_summary":
            claims_by_status(fact_claims),

        "monthly_summary":
            claims_by_month(fact_claims),

        "customer_summary":
            customer_claim_summary(fact_claims),

        "health_life_portfolio":
            health_life_portfolio(fact_claims),

        "property_casualty_portfolio":
            property_casualty_portfolio(fact_claims)
    }
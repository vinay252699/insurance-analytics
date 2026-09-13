from pyspark.sql import functions as F
from pyspark.sql.window import Window


def clean_customers(customers):
    """
    Create one conformed customer record per customer_id.

    Deduplication is deterministic:
    1. Prefer the canonical customer name.
    2. Prefer the most recent registration date.
    3. Prefer the highest lifetime value as a final tie-breaker.
    """

    customers_valid = customers.filter(
        F.col("customer_id").isNotNull()
    )

    customers_prepared = (
        customers_valid
        .withColumn(
            "is_canonical_name",
            F.col("name") == F.concat(
                F.lit("Customer_"),
                F.col("customer_id").cast("string")
            )
        )
    )

    customer_window = (
        Window
        .partitionBy("customer_id")
        .orderBy(
            F.col("is_canonical_name").desc(),
            F.col("registration_date").desc_nulls_last(),
            F.col("lifetime_value").desc_nulls_last()
        )
    )

    return (
        customers_prepared
        .withColumn(
            "rn",
            F.row_number().over(customer_window)
        )
        .filter(F.col("rn") == 1)
        .drop("rn", "is_canonical_name")
    )


def enrich_claims(claims, clean_customers):
    """
    Enrich claims with customer attributes while preserving
    the one-row-per-claim grain.
    """

    customer_lookup = (
        clean_customers
        .select(
            "customer_id",
            "name",
            "country",
            "registration_date",
            "tier",
            "lifetime_value"
        )
        .withColumn(
            "_customer_exists",
            F.lit(1)
        )
    )

    return (
        claims.alias("c")
        .join(
            customer_lookup.alias("cu"),
            F.col("c.customer_id") == F.col("cu.customer_id"),
            "left"
        )
        .select(
            F.col("c.claim_id"),
            F.col("c.customer_id"),
            F.to_date(F.col("c.claim_date")).alias("claim_date"),
            F.col("c.claim_type"),
            F.col("c.claim_amount"),
            F.col("c.status"),
            F.col("c.country").alias("claim_country"),

            F.col("cu.name").alias("customer_name"),
            F.col("cu.country").alias("customer_country"),
            F.col("cu.registration_date"),
            F.col("cu.tier"),
            F.col("cu.lifetime_value"),

            F.when(
                F.col("cu._customer_exists") == 1,
                F.lit("MATCHED")
            )
            .otherwise(F.lit("UNMATCHED"))
            .alias("customer_match_status")
        )
    )


def prepare_products(products):
    """
    Normalize product effective and end dates.
    """

    return (
        products
        .withColumn(
            "effective_date",
            F.to_date("effective_date")
        )
        .withColumn(
            "end_date",
            F.to_date("end_date")
        )
    )


def resolve_product_versions(claims_enriched, products_prepared):
    """
    Resolve the product version applicable to each claim date.

    Temporal join:
        claim_type = product_code
        claim_date >= effective_date
        claim_date <= end_date
        OR end_date is NULL

    The product master is expected to contain non-overlapping
    effective-date ranges for each product code.
    """

    product_condition = (
        (F.col("c.claim_type") == F.col("p.product_code"))
        &
        (
            F.col("c.claim_date")
            >= F.col("p.effective_date")
        )
        &
        (
            F.col("p.end_date").isNull()
            |
            (
                F.col("c.claim_date")
                <= F.col("p.end_date")
            )
        )
    )

    joined = (
        claims_enriched.alias("c")
        .join(
            products_prepared.alias("p"),
            product_condition,
            "left"
        )
    )

    return joined.select(
        F.col("c.claim_id"),
        F.col("c.customer_id"),
        F.col("c.claim_date"),
        F.col("c.claim_type"),
        F.col("c.claim_amount"),
        F.col("c.status"),
        F.col("c.claim_country"),

        F.col("c.customer_name"),
        F.col("c.customer_country"),
        F.col("c.registration_date"),
        F.col("c.tier"),
        F.col("c.lifetime_value"),
        F.col("c.customer_match_status"),

        F.col("p.product_code"),
        F.col("p.product_name"),
        F.col("p.avg_premium"),
        F.col("p.commission_rate"),
        F.col("p.risk_category"),

        F.col("p.effective_date")
        .alias("product_effective_date"),

        F.col("p.end_date")
        .alias("product_end_date"),

        F.col("p.version")
        .alias("product_version"),

        F.col("p.is_current")
        .alias("product_is_current")
    )
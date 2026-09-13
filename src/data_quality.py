from pyspark.sql import functions as F
from pyspark.sql.window import Window


def check_nulls(df, columns):
    """
    Return null counts for selected columns.
    """
    return df.select([
        F.sum(
            F.col(c).isNull().cast("long")
        ).alias(c)
        for c in columns
    ])


def check_duplicates(df, keys):
    """
    Return number of duplicate key groups.
    """
    return (
        df.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )


def check_negative_values(df, column):
    """
    Count negative values.
    """
    return (
        df.filter(
            F.col(column).isNotNull()
            & (F.col(column) < 0)
        )
        .count()
    )


def check_allowed_values(df, column, allowed_values):
    """
    Count non-null records containing values
    outside the configured allowed set.
    """
    return (
        df.filter(
            F.col(column).isNotNull()
            & ~F.col(column).isin(allowed_values)
        )
        .count()
    )


def check_overlapping_product_versions(products):
    """
    Detect overlapping effective-date ranges for each product.

    A product version should not overlap the next version
    for the same product_code.
    """

    product_window = (
        Window
        .partitionBy("product_code")
        .orderBy(F.col("effective_date"))
    )

    with_next = products.withColumn(
        "next_effective_date",
        F.lead("effective_date").over(product_window)
    )

    return (
        with_next
        .filter(
            F.col("next_effective_date").isNotNull()
            &
            F.col("end_date").isNotNull()
            &
            (
                F.col("end_date")
                >= F.col("next_effective_date")
            )
        )
        .count()
    )


def profile_claims(claims, allowed_statuses, allowed_claim_types):
    """
    Run core data quality checks on claims.
    """

    return {
        "row_count": claims.count(),

        "duplicate_claim_ids": check_duplicates(
            claims,
            ["claim_id"]
        ),

        "negative_claim_amounts": check_negative_values(
            claims,
            "claim_amount"
        ),

        "invalid_status": check_allowed_values(
            claims,
            "status",
            allowed_statuses
        ),

        "invalid_claim_type": check_allowed_values(
            claims,
            "claim_type",
            allowed_claim_types
        )
    }


def profile_customers(customers, allowed_customer_tiers):
    """
    Run core data quality checks on customers.
    """

    return {
        "row_count": customers.count(),

        "duplicate_customer_ids": check_duplicates(
            customers,
            ["customer_id"]
        ),

        "null_customer_ids": (
            customers
            .filter(F.col("customer_id").isNull())
            .count()
        ),

        "invalid_tier": (
            customers
            .filter(
                F.col("tier").isNotNull()
                &
                ~F.col("tier").isin(allowed_customer_tiers)
            )
            .count()
        )
    }


def profile_products(products):
    """
    Run core data quality checks on product history.
    """

    duplicate_versions = check_duplicates(
        products,
        ["product_code", "version"]
    )

    invalid_date_ranges = (
        products
        .filter(
            F.col("effective_date").isNull()
            |
            (
                F.col("end_date").isNotNull()
                &
                (
                    F.col("end_date")
                    < F.col("effective_date")
                )
            )
        )
        .count()
    )

    overlapping_versions = check_overlapping_product_versions(
        products
    )

    current_version_counts = (
        products
        .groupBy("product_code")
        .agg(
            F.sum(
                F.col("is_current").cast("int")
            ).alias("current_versions")
        )
        .filter(
            F.col("current_versions") != 1
        )
        .count()
    )

    return {
        "row_count": products.count(),

        "duplicate_product_versions":
            duplicate_versions,

        "invalid_date_ranges":
            invalid_date_ranges,

        "overlapping_product_versions":
            overlapping_versions,

        "products_with_invalid_current_version_count":
            current_version_counts
    }


def profile_fact_claims(fact_claims, raw_claim_count):
    """
    Validate the final fact table against the raw claims.
    """

    fact_row_count = fact_claims.count()

    duplicate_claims = check_duplicates(
        fact_claims,
        ["claim_id"]
    )

    unmatched_products = (
        fact_claims
        .filter(F.col("product_code").isNull())
        .count()
    )

    unmatched_customers = (
        fact_claims
        .filter(
            F.col("customer_match_status") == "UNMATCHED"
        )
        .count()
    )

    return {
        "fact_row_count":
            fact_row_count,

        "raw_claim_count":
            raw_claim_count,

        "row_count_reconciliation":
            fact_row_count == raw_claim_count,

        "duplicate_claim_ids":
            duplicate_claims,

        "unmatched_products":
            unmatched_products,

        "unmatched_customers":
            unmatched_customers
    }
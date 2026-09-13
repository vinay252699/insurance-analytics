\# Insurance Analytics – PySpark Data Engineering Assessment



\## 1. Overview



This project implements an end-to-end insurance analytics pipeline using Apache Spark / PySpark.



The solution reads claims, customer, and product-version data, performs data-quality validation and conformance, resolves historical product versions using effective-date logic, enriches claims with customer attributes, and produces reusable analytical marts for portfolio reporting.



The implementation is designed to be:



\- Distributed and scalable using Spark

\- Reproducible through a parameterized pipeline

\- Config-driven for business rules

\- Data-quality aware

\- Robust to duplicate customer records

\- Capable of resolving historical product versions

\- Reusable for downstream BI and analytics



\---



\## 2. Assessment Requirements



\### Q1 – Health \& Life Portfolio



The Health \& Life portfolio is analyzed for:



\- Customer tier

\- Country

\- Product

\- Claim volume

\- Claim amount

\- Approval / rejection / pending counts

\- Approval rate



All observed customer segments are retained, including an `UNKNOWN` segment for claims where the customer tier is unavailable.



\### Q2 – Property \& Casualty Portfolio



The Property \& Casualty portfolio covers Auto and Home products and provides:



\- Country

\- Product

\- Product version

\- Risk category

\- Claim volume

\- Claim amount

\- Approval rate

\- Average premium

\- Commission rate

\- Estimated premium

\- Estimated commission



Product attributes are resolved according to the product version that was effective on the claim date.

---

## 3. Architecture

```text
                    SOURCE DATA
                         |
        +----------------+----------------+
        |                |                |
      Claims          Customers        Products
        |                |                |
        +----------------+----------------+
                         |
                  Data Quality Checks
                         |
              +----------+----------+
              |                     |
       Customer Conformance    Product Preparation
       & Deduplication         & Version Validation
              |                     |
              +----------+----------+
                         |
                  Claim Enrichment
                         |
             Temporal Product Join
             (Effective-date logic)
                         |
                  Conformed Fact
                  Claims Dataset
                         |
              +----------+----------+
              |                     |
        Analytics Layer       Portfolio Layer
              |                     |
      Reusable Analytics      Q1 Health & Life
            Marts             Q2 Property & Casualty
              |
         Parquet Outputs

4. Technology Stack
Python
PySpark / Apache Spark 3.3
Parquet
Docker
JupyterLab
JSON configuration
Spark SQL / DataFrame APIs

The processing logic uses Spark DataFrame transformations rather than collecting the large datasets into local Python or Pandas memory.

5. Repository Structure
insurance-analytics/
│
├── README.md
├── .gitignore
│
├── config/
│   └── business_rules.json
│
├── data/
│   └── samples/
│       ├── claims.parquet
│       ├── customers.parquet
│       └── products.parquet
│
├── notebooks/
│   └── 01_data_profiling.ipynb
│
└── src/
    ├── analytics.py
    ├── data_quality.py
    ├── pipeline.py
    └── transformations.py
6. Source Data

The supplied sample data contains:

Dataset	Approximate Rows
Claims	1,000,000
Customers	500,000
Products	11

Claims contain Auto, Home, Health, and Life claim types.

The product dataset represents historical product versions using effective and end dates.

## 7. Data Quality

The pipeline performs validation before analytical processing.

### Claims

Validated for:

- Row count
- Duplicate claim IDs
- Negative claim amounts
- Invalid claim statuses
- Invalid claim types

Results:

- 1,000,000 claims
- 0 duplicate claim IDs
- 0 negative claim amounts
- 0 invalid statuses
- 0 invalid claim types

### Customers

Validated for:

- Duplicate customer IDs
- Null customer IDs
- Invalid customer tiers

Results:

- 500,000 source rows
- 7,391 duplicate customer-ID groups
- 4,137 null customer IDs
- 0 invalid customer tiers

Customer records with null IDs are excluded from the conformed customer dimension.

Duplicate customer records are resolved deterministically.

### Products

Validated for:

- Duplicate product/version combinations
- Invalid effective/end-date ranges
- Overlapping product versions
- Invalid current-version definitions

Results:

- 11 product-version records
- 0 duplicate product/version combinations
- 0 invalid date ranges
- 0 overlapping versions
- 0 products with an invalid current-version count

---

## 8. Customer Conformance

The customer source contains duplicate records.

The pipeline creates one conformed customer record per valid customer ID.

The deterministic selection logic is:

1. Prefer the canonical customer name (`Customer_<customer_id>`)
2. Prefer the most recent registration date
3. Use lifetime value as the final tie-breaker

This prevents customer duplication from multiplying claim rows during enrichment.

Claims with no matching customer are retained rather than dropped.

A `customer_match_status` attribute identifies:

- `MATCHED`
- `UNMATCHED`

The final fact dataset contained 26,024 claims with no matching customer record. These claims remain available for portfolio-level analytics.

---

## 9. Historical Product Version Resolution

Product information is maintained as versioned records.

The pipeline performs a temporal join using:

```text
claim_type = product_code
AND claim_date >= effective_date
AND (
    end_date IS NULL
    OR claim_date <= end_date
)

---

## 10. Config-Driven Business Rules

Business definitions are externalized in:

`config/business_rules.json`

The configuration defines:

- Health & Life products
- Property & Casualty products
- Claim statuses
- Customer tiers
- Unknown customer-tier handling

For example:

```json
{
  "portfolios": {
    "health_life": ["HEALTH", "LIFE"],
    "property_casualty": ["AUTO", "HOME"]
  }
}

This avoids embedding portfolio definitions directly inside the pipeline.

11. Conformed Fact Dataset

The pipeline creates a claim-level analytical fact dataset.

The intended grain is:

One row per claim

The pipeline validates this grain by checking:

Fact row count = raw claim row count
Duplicate claim IDs = 0

The final validation successfully reconciled:

Raw claims : 1,000,000
Fact claims: 1,000,000

Therefore, no claim rows were lost or duplicated during enrichment and product-version resolution.

12. Analytics Marts

The pipeline generates reusable analytical marts:

Mart	Purpose
kpi_summary	Executive-level claim KPIs
product_summary	Product and version metrics
risk_summary	Risk-category analysis
customer_tier_summary	Customer-tier performance
country_summary	Geographic analysis
status_summary	Claim-status distribution
monthly_summary	Monthly claim trends
customer_summary	Customer-level claim metrics
health_life_portfolio	Q1 Health & Life analysis
property_casualty_portfolio	Q2 Property & Casualty analysis

The marts are written as Parquet datasets and can be consumed by downstream BI tools such as Power BI, Looker, or other analytical applications.

13. Key Results
Overall Claims
Total claims: 1,000,000
Total claim amount: approximately 25.25 billion
Average claim amount: approximately 25,251
Approved claims: 333,912
Rejected claims: 333,057
Pending claims: 333,031
Approval rate: approximately 33.39%
Health & Life

The Q1 Health & Life mart contains 120 analytical rows, covering combinations of:

Customer tier
Country
Product

The output includes claim volume, financial metrics, status counts, and approval rates.

Property & Casualty

The Q2 Property & Casualty mart contains 24 analytical rows, covering:

Country
Risk category
Product
Product version

The output includes claim metrics and product-version-specific premium/commission attributes.

14. Premium and Commission Assumption

The supplied data contains product-level:

Average premium
Commission rate

It does not contain actual premium or commission transactions for individual claims.

Therefore:

Estimated Premium = Claim Count × Average Premium

Estimated Commission = Estimated Premium × Commission Rate

These values are explicitly treated as estimated/proxy analytical metrics, not actual financial transactions.

Actual earned premium, written premium, or commission accounting would require policy or transaction-level financial data.

15. Scalability and Design Considerations

The solution uses Spark-native transformations rather than collecting large datasets into Python memory.

Distributed Processing

Large datasets are processed using Spark DataFrames and distributed aggregations.

Parameterized Pipeline

The pipeline accepts input and output locations through command-line arguments:

--claims
--customers
--products
--output
--rules
--master

This separates execution configuration from business logic.

Reusable Business Rules

Portfolio and status definitions are externalized into JSON configuration.

Deterministic Conformance

Customer duplicates are resolved using explicit ranking rules rather than arbitrary record selection.

Temporal Data Handling

Product versions are resolved based on effective dates, making the solution adaptable to future product-version changes.

Data Quality Gates

The pipeline validates the fact dataset before generating analytical marts.

Parquet Output

Parquet provides a columnar format suitable for downstream analytics and BI workloads.

16. Running the Pipeline

The pipeline can be submitted to the Spark cluster with:

docker exec bd-spark-master /spark/bin/spark-submit `
  --master spark://bd-spark-master:7077 `
  /data/insurance-analytics/src/pipeline.py `
  --claims /data/insurance-analytics/data/samples/claims.parquet `
  --customers /data/insurance-analytics/data/samples/customers.parquet `
  --products /data/insurance-analytics/data/samples/products.parquet `
  --output /data/insurance-analytics/output

The pipeline performs:

Read
  ↓
Data Quality
  ↓
Customer Conformance
  ↓
Claim Enrichment
  ↓
Temporal Product Resolution
  ↓
Fact Validation
  ↓
Analytics Marts
  ↓
Parquet Outputs
17. Design Summary

The solution separates the implementation into four logical layers.

Data Quality

data_quality.py

Validates source and conformed datasets.

Transformations

transformations.py

Handles customer conformance, claim enrichment, and temporal product resolution.

Analytics

analytics.py

Contains reusable KPI and portfolio aggregation logic.

Orchestration

pipeline.py

Coordinates the complete workflow and controls execution parameters.

This separation keeps the pipeline maintainable and allows individual components to evolve independently.

18. Assumptions
claim_type corresponds to product_code.
Product effective-date ranges are intended to be inclusive.
Product versions for a product do not overlap.
A claim should resolve to at most one product version.
Customer IDs are the business key for customer conformance.
Claims without a matching customer are retained for completeness.
Null customer tiers are represented as UNKNOWN.
Premium and commission values are estimates based on the supplied product-level attributes.
19. Outcome

The completed solution provides a reproducible Spark-based analytics pipeline that validates source quality, creates conformed data, handles historical product versions, preserves claim-level completeness, and produces reusable analytical marts addressing both assessment questions.
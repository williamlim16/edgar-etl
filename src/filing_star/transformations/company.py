from pyspark import pipelines as dp
from pyspark.sql.functions import col, from_json, explode, arrays_zip, row_number
from utilities.payload_schema import payload_schema

# Catalog/schema come from pipeline configuration (src/filing_star/filing_star.yml),
# which the DAB resolves per target — no hardcoded catalog here.
CATALOG = spark.conf.get("catalog")
STG_SCHEMA = spark.conf.get("stg_schema")
DIM_SCHEMA = spark.conf.get("dim_schema")


@dp.materialized_view(name=f"{CATALOG}.{DIM_SCHEMA}.dim_company")
def dim_company():
    data = spark.read.table(f"{CATALOG}.{STG_SCHEMA}.stg_submissions_raw")
    base = data.withColumn("payload", from_json(col("raw_payload"), payload_schema))
    return base.select(
        col("cik"),
        col("company_name"),
        col("ticker"),
        col("payload.sic").alias("sic_code"),
        col("payload.category").alias("category"),
        col("payload.ein").alias("ein"),
        col("payload.exchanges").alias("exchanges"),
    ).distinct()

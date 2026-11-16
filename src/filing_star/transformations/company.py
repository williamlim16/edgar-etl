from pyspark import pipelines as dp
from pyspark.sql.functions import col, from_json, explode, arrays_zip, row_number
from utilities.payload_schema import payload_schema


@dp.materialized_view(name="edgar.dim.dim_company")
def dim_company():
    data = spark.read.table("edgar.stg.stg_submissions_raw")
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

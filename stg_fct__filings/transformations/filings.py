from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.functions import col, from_json, explode, arrays_zip, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    ArrayType, LongType, MapType
)

payload_schema = """
STRUCT<addresses: STRUCT<business: STRUCT<city: STRING, country: STRING, countryCode: STRING, foreignStateTerritory: STRING, isForeignLocation: STRING, stateOrCountry: STRING, stateOrCountryDescription: STRING, street1: STRING, street2: STRING, zipCode: STRING>, mailing: STRUCT<city: STRING, country: STRING, countryCode: STRING, foreignStateTerritory: STRING, isForeignLocation: BIGINT, stateOrCountry: STRING, stateOrCountryDescription: STRING, street1: STRING, street2: STRING, zipCode: STRING>>, category: STRING, cik: STRING, description: STRING, ein: STRING, entityType: STRING, exchanges: ARRAY<STRING>, filings: STRUCT<files: ARRAY<STRUCT<filingCount: BIGINT, filingFrom: STRING, filingTo: STRING, name: STRING>>, recent: STRUCT<acceptanceDateTime: ARRAY<STRING>, accessionNumber: ARRAY<STRING>, act: ARRAY<STRING>, core_type: ARRAY<STRING>, fileNumber: ARRAY<STRING>, filingDate: ARRAY<STRING>, filmNumber: ARRAY<STRING>, form: ARRAY<STRING>, isInlineXBRL: ARRAY<BIGINT>, isXBRL: ARRAY<BIGINT>, isXBRLNumeric: ARRAY<BIGINT>, items: ARRAY<STRING>, primaryDocDescription: ARRAY<STRING>, primaryDocument: ARRAY<STRING>, reportDate: ARRAY<STRING>, size: ARRAY<BIGINT>>>, fiscalYearEnd: STRING, flags: STRING, formerNames: ARRAY<STRUCT<from: STRING, name: STRING, to: STRING>>, insiderTransactionForIssuerExists: BIGINT, insiderTransactionForOwnerExists: BIGINT, investorWebsite: STRING, lei: STRING, name: STRING, ownerOrg: STRING, phone: STRING, sic: STRING, sicDescription: STRING, stateOfIncorporation: STRING, stateOfIncorporationDescription: STRING, tickers: ARRAY<STRING>, website: STRING>
"""
@dp.materialized_view(name="silver_filings")
def silver_filings():
    data = spark.read.table("edgar.companies.bronze_submissions_raw")

    base = data.withColumn(
        "payload", from_json(col("raw_payload"), payload_schema)
    )

    zipped_filings = base.withColumn(
            "filing_record",
            explode(
                arrays_zip(
                        col("payload.filings.recent.accessionNumber"),
                        col("payload.filings.recent.core_type"),
                        col("payload.filings.recent.acceptanceDateTime"),
                        col("payload.filings.recent.act"),
                        col("payload.filings.recent.filingDate"),
                        col("payload.filings.recent.fileNumber"),
                        col("payload.filings.recent.isXBRL"),
                        col("payload.filings.recent.isXBRLNumeric"),
                        col("payload.filings.recent.items"),
                        col("payload.filings.recent.primaryDocDescription"),
                        col("payload.filings.recent.primaryDocument"),
                        col("payload.filings.recent.reportDate"),
                        col("payload.filings.recent.size"),
            )
        )
    )

    columnized = zipped_filings.select(
            col("cik"),
            col("company_name"),
            col("ticker"),
            col("payload.sic").alias("sic_code"),
            col("payload.category").alias("category"),
            col("payload.ein").alias("ein"),
            col("payload.exchanges").alias("exchanges"),
            col("ingested_at"),
            col("filing_record.accessionNumber").alias("accession_number"),
            col("filing_record.core_type").alias("core_type"),
            col("filing_record.acceptanceDateTime").alias("acceptance_datetime"),
            col("filing_record.act").alias("act"),
            col("filing_record.filingDate").alias("filing_date"),
            col("filing_record.fileNumber").alias("file_number"),
            col("filing_record.isXBRL").alias("is_xbrl"),
            col("filing_record.isXBRLNumeric").alias("is_xbrl_numeric"),
            col("filing_record.items").alias("items"),
            col("filing_record.primaryDocDescription").alias("primary_doc_description"),
            col("filing_record.primaryDocument").alias("primary_document"),
            col("filing_record.reportDate").alias("report_date"),
            col("filing_record.size").alias("filing_size"),
    )

    latest_snapshot = Window.partitionBy("cik", "accession_number").orderBy(
        col("ingested_at").desc()
    )

    return (
        columnized
        .withColumn("_rn", row_number().over(latest_snapshot))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )
 
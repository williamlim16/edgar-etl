# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 3
import hashlib
import json
import requests
import io
from datetime import datetime, timezone
import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 1,Config: CIK + destination catalog/schemas
COMPANY_CIK = "CIK0001045810"

# Destination is DAB-configurable: the deployed job passes these as notebook
# parameters (base_parameters in src/extract/edgar_ingestion.yml, resolved from
# bundle variables per target); interactive runs fall back to the defaults below.
dbutils.widgets.text("catalog", "edgar")
dbutils.widgets.text("stg_schema", "stg")
dbutils.widgets.text("fct_schema", "fct")
dbutils.widgets.text("dim_schema", "dim")
dbutils.widgets.text("mrt_schema", "mrt")

CATALOG = dbutils.widgets.get("catalog")
STG_SCHEMA = dbutils.widgets.get("stg_schema")
FCT_SCHEMA = dbutils.widgets.get("fct_schema")
DIM_SCHEMA = dbutils.widgets.get("dim_schema")
MRT_SCHEMA = dbutils.widgets.get("mrt_schema")

STG = f"{CATALOG}.{STG_SCHEMA}"
VOL = f"/Volumes/{CATALOG}/{STG_SCHEMA}/vol"

# COMMAND ----------

# MAGIC %md
# MAGIC # `/submissions` Endpoint

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setting up schemas and tables

# COMMAND ----------

# DBTITLE 1,Setup schemas and objects
# Idempotent setup — schemas, tables, and the volume live in the DAB-configured
# catalog/schemas defined in the config cell above.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{STG_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{FCT_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{DIM_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{MRT_SCHEMA}")

spark.sql(f"CREATE TABLE IF NOT EXISTS {STG}.stg_submissions_raw")
# Control table keyed on (cik, endpoint) so both endpoints can track one CIK.
# One-time migration of the existing table (add + backfill `endpoint`) runs in the next Python cell.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {STG}.ingestion_control (
        cik STRING,
        endpoint STRING,
        payload_hash STRING,
        max_acceptance_datetime STRING,
        last_fetch_at TIMESTAMP
    )
""")
# Thin snapshot table for /xbrl: the raw payload stays in the volume, not in the table.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {STG}.stg_xbrl_raw (
        cik STRING,
        entity_name STRING,
        max_filed STRING,
        volume_path STRING,
        ingested_at TIMESTAMP,
        endpoint STRING
    )
""")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{STG_SCHEMA}.vol")

# COMMAND ----------

# DBTITLE 1,Migrate ingestion_control to (cik, endpoint) key
# One-time migration: the control table predates the `endpoint` column (keyed on (cik, endpoint) now).
cols = {r["col_name"] for r in spark.sql(f"DESCRIBE TABLE {STG}.ingestion_control").collect()}
if "endpoint" not in cols:
    spark.sql(f"ALTER TABLE {STG}.ingestion_control ADD COLUMN endpoint STRING")
    print(f"Added `endpoint` column to {STG}.ingestion_control.")
spark.sql(f"UPDATE {STG}.ingestion_control SET endpoint = '/submissions' WHERE endpoint IS NULL")
print("Existing control rows backfilled with endpoint = '/submissions'.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Submissions Data

# COMMAND ----------

# DBTITLE 1,Fetch submissions JSON to volume
headers = {"User-Agent": "User Agent"}
nvidia = requests.get(f"https://data.sec.gov/submissions/{COMPANY_CIK}.json", headers=headers)
data = nvidia.json()

# Stable serialization + content hash for idempotency
payload_str = json.dumps(data, sort_keys=True)
payload_hash = hashlib.sha256(payload_str.encode()).hexdigest()
max_acc = max(data["filings"]["recent"]["acceptanceDateTime"])

# Look up the last ingested hash for this CIK + endpoint
cik = str(data["cik"])
ctrl_rows = (spark.table(f"{STG}.ingestion_control")
             .filter((F.col("cik") == cik) & (F.col("endpoint") == "/submissions"))
             .collect())
stored_hash = ctrl_rows[0]["payload_hash"] if ctrl_rows else None
changed = stored_hash != payload_hash
print(f"Hash {'differs from' if changed else 'matches'} stored hash — payload {'will be' if changed else 'will NOT be'} ingested.")


# COMMAND ----------

# DBTITLE 1,Append to stg_submissions_raw
if changed:
    # (i) write raw JSON file to the volume
    dbutils.fs.put(f"{VOL}/raw_submissions/{COMPANY_CIK}.json", payload_str, overwrite=True)
    print("File uploaded successfully!")

    # (ii) append snapshot to stg_submissions_raw
    stg_df = spark.createDataFrame([{
        "cik": cik,
        "ticker": ",".join(data.get("tickers", [])),
        "company_name": data["name"],
        "raw_payload": payload_str,
        "ingested_at": datetime.now(timezone.utc),
        "endpoint": "/submissions"
    }])
    stg_df.write.mode("append").saveAsTable(f"{STG}.stg_submissions_raw", mergeSchema=True)

    # (iii) upsert the control row
    spark.sql(f"""
        MERGE INTO {STG}.ingestion_control AS t
        USING (SELECT '{cik}' AS cik, '/submissions' AS endpoint, '{payload_hash}' AS payload_hash,
                      '{max_acc}' AS max_acceptance_datetime,
                      current_timestamp() AS last_fetch_at) AS s
        ON t.cik = s.cik AND t.endpoint = s.endpoint
        WHEN MATCHED THEN UPDATE SET t.payload_hash = s.payload_hash,
                                    t.max_acceptance_datetime = s.max_acceptance_datetime,
                                    t.last_fetch_at = s.last_fetch_at
        WHEN NOT MATCHED THEN INSERT (cik, endpoint, payload_hash, max_acceptance_datetime, last_fetch_at)
        VALUES (s.cik, s.endpoint, s.payload_hash, s.max_acceptance_datetime, s.last_fetch_at)
    """)
    print("New payload ingested: file written, stg row appended, control updated.")
else:
    print(f"No change since last fetch for CIK {cik} — skipping file write, stg append, and control update.")

# COMMAND ----------

# MAGIC %md
# MAGIC # `/xbrl` Endpoint

# COMMAND ----------

# DBTITLE 1,Fetch xbrl companyfacts JSON + hash
headers = {"User-Agent": "User Agent"}
nvidia = requests.get(f"https://data.sec.gov/api/xbrl/companyfacts/{COMPANY_CIK}.json", headers=headers)
data = nvidia.json()

# Stable serialization + content hash for idempotency (same pattern as /submissions)
payload_str = json.dumps(data, sort_keys=True)
payload_hash = hashlib.sha256(payload_str.encode()).hexdigest()

# companyfacts has no top-level acceptanceDateTime; recency lives per fact.
# Compute max `filed` across every fact (taxonomy -> concept -> unit -> fact list) for observability.
all_facts = [
    fact
    for concepts in data["facts"].values()
    for concept in concepts.values()
    for fact_list in concept["units"].values()
    for fact in fact_list
]
max_filed = max(fact["filed"] for fact in all_facts)

# Look up the last ingested hash for this CIK + endpoint
# companyfacts returns cik as an int (no zero-padding) — pad to match the /submissions CIK format
cik = str(data["cik"]).zfill(10)
ctrl_rows = (spark.table(f"{STG}.ingestion_control")
             .filter((F.col("cik") == cik) & (F.col("endpoint") == "/xbrl"))
             .collect())
stored_hash = ctrl_rows[0]["payload_hash"] if ctrl_rows else None
changed = stored_hash != payload_hash
print(f"Hash {'differs from' if changed else 'matches'} stored hash — payload {'will be' if changed else 'will NOT be'} ingested.")
print(f"Max filed date across {len(all_facts)} facts: {max_filed}")

# COMMAND ----------

# DBTITLE 1,Append xbrl snapshot to stg_xbrl_raw
if changed:
    # (i) write raw JSON file to the volume — the full payload lives here, not in the table
    volume_path = f"{VOL}/raw_xbrl/{COMPANY_CIK}.json"
    dbutils.fs.put(volume_path, payload_str, overwrite=True)
    print("File uploaded successfully!")

    # (ii) append a THIN snapshot row to stg_xbrl_raw
    stg_df = spark.createDataFrame([{
        "cik": cik,
        "entity_name": data.get("entityName"),
        "max_filed": max_filed,
        "volume_path": volume_path,
        "ingested_at": datetime.now(timezone.utc),
        "endpoint": "/xbrl"
    }])
    stg_df.write.mode("append").saveAsTable(f"{STG}.stg_xbrl_raw", mergeSchema=True)

    # (iii) upsert the control row
    spark.sql(f"""
        MERGE INTO {STG}.ingestion_control AS t
        USING (SELECT '{cik}' AS cik, '/xbrl' AS endpoint, '{payload_hash}' AS payload_hash,
                      '{max_filed}' AS max_acceptance_datetime,
                      current_timestamp() AS last_fetch_at) AS s
        ON t.cik = s.cik AND t.endpoint = s.endpoint
        WHEN MATCHED THEN UPDATE SET t.payload_hash = s.payload_hash,
                                    t.max_acceptance_datetime = s.max_acceptance_datetime,
                                    t.last_fetch_at = s.last_fetch_at
        WHEN NOT MATCHED THEN INSERT (cik, endpoint, payload_hash, max_acceptance_datetime, last_fetch_at)
        VALUES (s.cik, s.endpoint, s.payload_hash, s.max_acceptance_datetime, s.last_fetch_at)
    """)
    print("New payload ingested: file written, thin stg row appended, control updated.")
else:
    print(f"No change since last fetch for CIK {cik} — skipping file write, stg append, and control update.")
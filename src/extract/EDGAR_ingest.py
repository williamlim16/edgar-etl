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

COMPANY_CIK = "CIK0001045810"

# COMMAND ----------

# MAGIC %md
# MAGIC # `/submissions` Endpoint

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setting up schemas and tables

# COMMAND ----------

# DBTITLE 1,Setup schemas and objects
# MAGIC %sql
# MAGIC USE CATALOG edgar;
# MAGIC CREATE SCHEMA IF NOT EXISTS stg;
# MAGIC CREATE SCHEMA IF NOT EXISTS fct;
# MAGIC CREATE SCHEMA IF NOT EXISTS mrt;
# MAGIC USE SCHEMA stg;
# MAGIC CREATE TABLE IF NOT EXISTS stg_submissions_raw;
# MAGIC CREATE TABLE IF NOT EXISTS edgar.stg.ingestion_control (
# MAGIC     cik STRING,
# MAGIC     endpoint STRING,
# MAGIC     payload_hash STRING,
# MAGIC     max_acceptance_datetime STRING,
# MAGIC     last_fetch_at TIMESTAMP
# MAGIC );
# MAGIC -- The control table is now keyed on (cik, endpoint) so both endpoints can track one CIK.
# MAGIC -- One-time migration of the existing table (add + backfill `endpoint`) runs in the next Python cell.
# MAGIC -- Thin snapshot table for /xbrl: the raw payload stays in the volume, not in the table.
# MAGIC CREATE TABLE IF NOT EXISTS edgar.stg.stg_xbrl_raw (
# MAGIC     cik STRING,
# MAGIC     entity_name STRING,
# MAGIC     max_filed STRING,
# MAGIC     volume_path STRING,
# MAGIC     ingested_at TIMESTAMP,
# MAGIC     endpoint STRING
# MAGIC );
# MAGIC CREATE VOLUME IF NOT EXISTS edgar.stg.vol;

# COMMAND ----------

# DBTITLE 1,Migrate ingestion_control to (cik, endpoint) key
# One-time migration: the control table predates the `endpoint` column (keyed on (cik, endpoint) now).
cols = {r["col_name"] for r in spark.sql("DESCRIBE TABLE edgar.stg.ingestion_control").collect()}
if "endpoint" not in cols:
    spark.sql("ALTER TABLE edgar.stg.ingestion_control ADD COLUMN endpoint STRING")
    print("Added `endpoint` column to edgar.stg.ingestion_control.")
spark.sql("UPDATE edgar.stg.ingestion_control SET endpoint = '/submissions' WHERE endpoint IS NULL")
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
ctrl_rows = (spark.table("edgar.stg.ingestion_control")
             .filter((F.col("cik") == cik) & (F.col("endpoint") == "/submissions"))
             .collect())
stored_hash = ctrl_rows[0]["payload_hash"] if ctrl_rows else None
changed = stored_hash != payload_hash
print(f"Hash {'differs from' if changed else 'matches'} stored hash — payload {'will be' if changed else 'will NOT be'} ingested.")


# COMMAND ----------

# DBTITLE 1,Append to stg_submissions_raw
if changed:
    # (i) write raw JSON file to the volume
    dbutils.fs.put(f"/Volumes/edgar/stg/vol/raw_submissions/{COMPANY_CIK}.json", payload_str, overwrite=True)
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
    stg_df.write.mode("append").saveAsTable("edgar.stg.stg_submissions_raw", mergeSchema=True)

    # (iii) upsert the control row
    spark.sql(f"""
        MERGE INTO edgar.stg.ingestion_control AS t
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
ctrl_rows = (spark.table("edgar.stg.ingestion_control")
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
    volume_path = f"/Volumes/edgar/stg/vol/raw_xbrl/{COMPANY_CIK}.json"
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
    stg_df.write.mode("append").saveAsTable("edgar.stg.stg_xbrl_raw", mergeSchema=True)

    # (iii) upsert the control row
    spark.sql(f"""
        MERGE INTO edgar.stg.ingestion_control AS t
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
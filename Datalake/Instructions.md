# Delta Lake Study Guide
---

## Table of Contents

1. [Pipeline overview](#pipeline-overview)
2. [Setup](#setup)
3. [Bronze — raw ingest, append-only](#bronze)
4. [Silver — clean and deduplicate](#silver)
5. [Gold — SCD Type 2 dimension](#gold)
6. [Querying the dimension](#querying)
7. [OPTIMIZE, Z-ORDER and Liquid Clustering](#optimize)
8. [Auto Loader from ADLS Gen2](#autoloader)
9. [Key concepts to know cold](#keyconcepts)

---

## 1. Pipeline overview <a name="pipeline-overview"></a>

```
ADLS Gen2 /customers/incoming/*.json
         ↓
    Auto Loader (cloudFiles)
    checkpoint → never reprocesses
         ↓
    bronze.customers_raw        ← append only, never modify
         ↓
    MERGE into silver.customers ← deduplicated, typed
         ↓
    SCD Type 2 MERGE → gold / silver.customers_dim
         ↓
    OPTIMIZE ... CLUSTER BY (customer_id)
    or ZORDER BY (customer_id)  ← run after each load
```

**SCD Type 2 idea in one sentence:** every time something changes, close the old row and open a new one.

The three cases your MERGE must handle:

```
Incoming record → Does a current row exist?
                         │
              ┌──────────┴──────────┐
             YES                    NO
              │                     │
    Did anything change?        INSERT it
       │            │           (new customer)
      YES           NO
       │            │
   EXPIRE old    do nothing
   INSERT new
```

---

## 2. Setup <a name="setup"></a>

```sql
CREATE CATALOG IF NOT EXISTS sandbox;
CREATE SCHEMA  IF NOT EXISTS sandbox.adtech;
USE sandbox.adtech;

-- Community Edition (no Unity Catalog):
-- CREATE DATABASE IF NOT EXISTS adtech;
-- USE adtech;
```

---

## 3. Bronze — raw ingest, append-only <a name="bronze"></a>

Bronze is a landing zone. Never clean, never deduplicate. Preserve exactly what arrived and when.

```python
from pyspark.sql import functions as F

raw_batch_1 = [
    ("C001", "Acme Corp",   "ES", "premium",    "2024-01-10"),
    ("C002", "BetaAds",     "FR", "standard",   "2024-01-10"),
    ("C003", "Clickstream", "DE", "premium",    "2024-01-10"),
]
raw_batch_2 = [
    ("C001", "Acme Corp",   "ES", "enterprise", "2024-01-11"),  # tier changed
    ("C004", "DeltaMedia",  "IT", "standard",   "2024-01-11"),  # new customer
]

schema = "customer_id STRING, name STRING, country STRING, tier STRING, source_date STRING"

df_b1 = spark.createDataFrame(raw_batch_1, schema)
df_b2 = spark.createDataFrame(raw_batch_2, schema)

def add_bronze_meta(df, batch_id):
    return df.withColumn("_ingested_at", F.current_timestamp()) \
             .withColumn("_batch_id", F.lit(batch_id)) \
             .withColumn("_source_file", F.lit(f"batch_{batch_id}.json"))

df_b1 = add_bronze_meta(df_b1, 1)
df_b2 = add_bronze_meta(df_b2, 2)

# APPEND only — never overwrite
(df_b1.write.format("delta").mode("append").saveAsTable("adtech.raw_customers"))
(df_b2.write.format("delta").mode("append").saveAsTable("adtech.raw_customers"))
```

Verify — C001 appears twice (both versions preserved):

```sql
SELECT customer_id, name, tier, source_date, _batch_id
FROM adtech.raw_customers
ORDER BY customer_id, source_date;
```

---

## 4. Silver — clean and deduplicate <a name="silver"></a>

Silver produces one clean, typed, deduplicated record per business key per version.

### Create table

```sql
CREATE TABLE IF NOT EXISTS silver.customers (
  surrogate_key  BIGINT GENERATED ALWAYS AS IDENTITY, -- synthetic PK, unique per row
  customer_id    STRING,      -- business key — repeats across versions
  full_name      STRING,
  email          STRING,
  segment        STRING,
  country        STRING,
  valid_from     TIMESTAMP,
  valid_to       TIMESTAMP,
  is_current     BOOLEAN,
  _updated_at    TIMESTAMP
) USING DELTA;
```

> **Note:** `surrogate_key` is synthetic — never use `customer_id` as PK in a dimension because it repeats across versions.

### First load

```sql
INSERT INTO silver.customers
  (customer_id, full_name, email, segment, country,
   valid_from, valid_to, is_current, _updated_at)
SELECT
    customer_id,
    full_name,
    email,
    segment,
    country,
    _ingested_at        AS valid_from,
    NULL                AS valid_to,
    true                AS is_current,
    current_timestamp() AS _updated_at
FROM bronze.customers_raw;
```

### Deduplicate in pure SQL (for incremental loads)

```sql
-- Read Bronze, cast types, deduplicate — one shot, no Python needed
CREATE OR REPLACE TEMP VIEW silver_incoming AS
SELECT
    customer_id,
    name,
    country,
    tier,
    CAST(source_date AS DATE)  AS source_date,
    current_timestamp()        AS _processed_at
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, source_date, tier, country
            ORDER BY _ingested_at DESC   -- keep latest if true duplicates exist
        ) AS _rn
    FROM adtech.raw_customers
    WHERE _ingested_at >= '2024-01-11'   -- only new batch
)
WHERE _rn = 1;
```

> `ROW_NUMBER() ORDER BY _ingested_at DESC` is strictly better than `dropDuplicates()` — behavior is deterministic and auditable.

---

## 5. Gold — SCD Type 2 dimension <a name="gold"></a>

### Minimum viable SCD Type 2

**Step 1 — expire old row if something changed:**

```sql
MERGE INTO silver.customers AS target
USING (
    SELECT * FROM bronze.customers_raw
    WHERE _ingested_at >= '2024-01-11'
) AS source
  ON  target.customer_id = source.customer_id
  AND target.is_current  = true

WHEN MATCHED AND (
    target.segment   <> source.segment  OR
    target.country   <> source.country  OR
    target.email     <> source.email    OR
    target.full_name <> source.full_name
) THEN UPDATE SET
    target.valid_to    = source._ingested_at,
    target.is_current  = false,
    target._updated_at = current_timestamp();
```

**Step 2 — insert new version:**

```sql
INSERT INTO silver.customers
  (customer_id, full_name, email, segment, country,
   valid_from, valid_to, is_current, _updated_at)
SELECT
    source.customer_id,
    source.full_name,
    source.email,
    source.segment,
    source.country,
    source._ingested_at AS valid_from,
    NULL                AS valid_to,
    true                AS is_current,
    current_timestamp() AS _updated_at
FROM bronze.customers_raw AS source
WHERE source._ingested_at >= '2024-01-11'
  AND NOT EXISTS (
      SELECT 1 FROM silver.customers existing
      WHERE existing.customer_id = source.customer_id
        AND existing.valid_from  = source._ingested_at
        AND existing.is_current  = true
  );
```

> **Why two statements?** Delta Lake MERGE can't expire a row AND insert a new one in a single `WHEN MATCHED` block. `WHEN MATCHED` can only UPDATE or DELETE — not INSERT. Always two steps.

### Expected result after incremental load

| surrogate_key | customer_id | segment | valid_from | valid_to | is_current |
|---|---|---|---|---|---|
| 1 | C001 | premium | 2024-01-10 | 2024-01-11 | false |
| 2 | C002 | standard | 2024-01-10 | null | true |
| 3 | C001 | enterprise | 2024-01-11 | null | true |
| 4 | C003 | standard | 2024-01-11 | null | true |

---

## 6. Querying the dimension <a name="querying"></a>

```sql
-- Current state only
SELECT * FROM silver.customers WHERE is_current = true;

-- Full history for one customer
SELECT customer_id, segment, valid_from, valid_to
FROM silver.customers
WHERE customer_id = 'C001'
ORDER BY valid_from;

-- Point-in-time: what was C001's segment on 2024-01-10?
SELECT * FROM silver.customers
WHERE customer_id = 'C001'
  AND valid_from <= '2024-01-10'
  AND (valid_to > '2024-01-10' OR valid_to IS NULL);
```

### Delta Lake time travel

```sql
-- Check transaction history
DESCRIBE HISTORY silver.customers;

-- What did the table look like before the MERGE?
SELECT * FROM silver.customers VERSION AS OF 0;

-- Point-in-time by timestamp
SELECT * FROM silver.customers TIMESTAMP AS OF '2024-01-10T00:00:00';
```

---

## 7. OPTIMIZE, Z-ORDER and Liquid Clustering <a name="optimize"></a>

### The problem

Delta tables accumulate many small Parquet files over time — every INSERT, MERGE, streaming micro-batch adds files. Spark has to open and scan all of them.

```sql
-- See current file layout
DESCRIBE DETAIL silver.customers;
-- Look at: numFiles, sizeInBytes
```

### OPTIMIZE — compact small files

```sql
-- Compact all small files into larger ones (~1GB target per file)
OPTIMIZE silver.customers;
```

### Z-ORDER — co-locate related rows

```sql
-- Co-locate rows by customer_id and country in the same files
OPTIMIZE silver.customers ZORDER BY (customer_id, country);
```

### Before vs after — data skipping

**Before OPTIMIZE:**
```
Scan parquet silver.customers
  Files read: 847          ← opens every file
  Files skipped: 0
  Rows read: 4,200,000     ← full table scan
```

**After OPTIMIZE + ZORDER BY (customer_id):**
```
Scan parquet silver.customers
  Files read: 3            ← skipped 844 files
  Files skipped: 844       ← data skipping via min/max stats
  Rows read: 12,400        ← only files containing C001
```

Delta stores min/max statistics per column per file. After Z-ORDER, all C001 rows are clustered in the same few files — every other file proves it can't contain C001 and gets skipped.

### Z-ORDER — good vs bad candidates

```sql
-- Good: high cardinality, frequently filtered
OPTIMIZE silver.customers ZORDER BY (customer_id);
OPTIMIZE silver.customers ZORDER BY (country, segment);
OPTIMIZE silver.customers ZORDER BY (valid_from);

-- Bad
OPTIMIZE silver.customers ZORDER BY (is_current);  -- ❌ boolean, only 2 values
OPTIMIZE silver.customers ZORDER BY (full_name);   -- ❌ rarely filtered
```

### Liquid Clustering — when to use instead

Z-ORDER rewrites the whole table every run. Liquid Clustering clusters **incrementally** — only new/changed files, and clustering columns can change without a full rewrite.

```sql
-- Enable at table creation
CREATE TABLE IF NOT EXISTS silver.customers_lc (
  surrogate_key  BIGINT GENERATED ALWAYS AS IDENTITY,
  customer_id    STRING,
  full_name      STRING,
  email          STRING,
  segment        STRING,
  country        STRING,
  valid_from     TIMESTAMP,
  valid_to       TIMESTAMP,
  is_current     BOOLEAN,
  _updated_at    TIMESTAMP
)
USING DELTA
CLUSTER BY (customer_id, country);  -- replaces ZORDER BY

-- Trigger incremental clustering
OPTIMIZE silver.customers_lc;       -- no ZORDER needed

-- Change clustering columns later — no full rewrite
ALTER TABLE silver.customers_lc
CLUSTER BY (customer_id, segment);
```

### Decision rule

| Situation | Use |
|---|---|
| Databricks Runtime < 13.3 | Z-ORDER |
| DBR 13.3+, new table | Liquid Clustering |
| Existing table, stable query patterns | Z-ORDER is fine |
| Query patterns change frequently | Liquid Clustering |
| Very large table, rewrite cost too high | Liquid Clustering |

For large AdTech tables on a recent DBR — **default to Liquid Clustering on new tables**.

### Maintenance

```sql
-- Remove old file versions (default 7-day retention)
VACUUM silver.customers RETAIN 168 HOURS;
```

---

## 8. Auto Loader from ADLS Gen2 <a name="autoloader"></a>

Auto Loader tracks which files have been processed using a **checkpoint**. On each run it only reads files it hasn't seen before — never rescans the entire folder.

### ADLS folder structure

```
datalake (storage account)
└── raw (container)
    └── customers/incoming/
            ├── 2024-01-10_batch.json
            ├── 2024-01-11_batch.json
            └── ...  ← new files land here
```

### Auto Loader job

```python
checkpoint_path = "abfss://raw@datalake.dfs.core.windows.net/_checkpoints/customers"
source_path     = "abfss://raw@datalake.dfs.core.windows.net/customers/incoming/"
bronze_table    = "bronze.customers_raw"

(spark.readStream
    .format("cloudFiles")                       # Auto Loader format
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation",
            checkpoint_path + "/schema")        # schema inferred + stored here
    .option("cloudFiles.inferColumnTypes", True)

    .load(source_path)

    # Bronze metadata
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_file_size",   F.col("_metadata.file_size"))

    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)  # ← fault-tolerance key
    .option("mergeSchema", True)                    # handle schema evolution
    .trigger(availableNow=True)                     # process pending files, then stop
    .toTable(bronze_table)
)
```

### What the checkpoint contains

```
_checkpoints/customers/
    ├── schema/       ← inferred schema stored here
    ├── commits/      ← one file per micro-batch that committed successfully
    ├── offsets/      ← which files were assigned to each micro-batch
    └── sources/
          └── 0/
                └── rocksdb/  ← file tracking index (which files already processed)
```

When the job restarts after failure, Spark reads the checkpoint and replays from the last successful commit — **exactly-once semantics**.

> ⚠️ Never delete the checkpoint unless doing a full intentional reload. Deleting it causes Auto Loader to reprocess all files → duplicate Bronze rows.

### Trigger modes

```python
# Batch / catch-up — most common for daily pipelines
.trigger(availableNow=True)
# Processes all files landed since last run, then stops.
# Schedule via Databricks Workflow (e.g. hourly).

# Continuous streaming — low latency
.trigger(processingTime="30 seconds")
# Cluster stays alive. Use for near-real-time Bronze ingestion.

# once=True is deprecated in newer DBR — use availableNow instead
```

### Verify ingestion

```sql
SELECT
    _source_file,
    COUNT(*)          AS rows_ingested,
    MIN(_ingested_at) AS first_seen,
    MAX(_ingested_at) AS last_seen
FROM bronze.customers_raw
GROUP BY _source_file
ORDER BY first_seen;

-- Each streaming micro-batch = one commit in the Delta log
DESCRIBE HISTORY bronze.customers_raw;
```

---

## 9. Key concepts to know cold <a name="keyconcepts"></a>

### SCD Type 2 checklist

- [ ] `surrogate_key` = synthetic, auto-generated. Never use `customer_id` as PK in a dim.
- [ ] `valid_to = NULL` means still active. Some teams use `9999-12-31` — both valid, NULL is cleaner.
- [ ] `is_current` is a convenience flag — redundant with `valid_to IS NULL` but improves readability.
- [ ] MERGE can only UPDATE/DELETE on `WHEN MATCHED`. INSERT of the new version is always a second statement.
- [ ] `NOT EXISTS` guard on the INSERT makes the pipeline **idempotent** — safe to rerun.
- [ ] Late-arriving data (record dated before current `valid_from`) breaks naive MERGE — needs extra guard.

### Delta Lake operations

```sql
DESCRIBE HISTORY table_name;             -- full transaction log
SELECT * FROM table VERSION AS OF 0;    -- time travel by version
OPTIMIZE table ZORDER BY (col);         -- compact + co-locate
VACUUM table RETAIN 168 HOURS;          -- remove old file versions
```

### Auto Loader checklist

- [ ] `cloudFiles.schemaLocation` — always set, stores inferred schema between runs.
- [ ] `checkpointLocation` — never delete in production.
- [ ] `availableNow=True` — batch mode, process pending files then stop.
- [ ] `mergeSchema=True` — handle source schema evolution without failing.
- [ ] Bronze is always `outputMode("append")` — never overwrite.

### Z-ORDER vs Liquid Clustering

| | Z-ORDER | Liquid Clustering |
|---|---|---|
| DBR requirement | Any | 13.3+ |
| Rewrites on OPTIMIZE | Full table | Incremental only |
| Change clustering columns | Full rewrite needed | ALTER TABLE, no rewrite |
| Best for | Existing tables, stable patterns | New tables, evolving patterns |

---

*Next topics: ADF metadata-driven ingestion · Databricks Workflow orchestration · Data Contracts with ODCS*

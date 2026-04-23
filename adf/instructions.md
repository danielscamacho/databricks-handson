# ADF Metadata-Driven Pipeline — Study Guide

> Consolidated from hands-on exercises. Last updated: April 2026.

---

## Table of Contents

1. [Architecture overview](#architecture)
2. [Config table](#config-table)
3. [Pipeline structure](#pipeline-structure)
4. [Lookup Activity](#lookup)
5. [Filter Activity](#filter)
6. [ForEach Activity](#foreach)
7. [Web Activity — Key Vault auth](#keyvault)
8. [Copy Activity — REST to ADLS Gen2](#copy)
9. [Update watermark](#watermark)
10. [Failure handling](#failures)
11. [Trigger Databricks after copy](#databricks)
12. [Adding a new source](#new-source)
13. [Local simulation (no Azure subscription)](#local)

---

## 1. Architecture overview <a name="architecture"></a>

Instead of one pipeline per source, one generic pipeline driven by a config table.
Adding a new source = inserting a row, not touching pipeline code.

```
Config table (Azure SQL / Delta)
     ↓ Lookup Activity
   ADF Pipeline (generic, parameterized)
     ↓ ForEach Activity
   REST API calls (one per source)
     ↓ Copy Activity
   ADLS Gen2 /bronze/{source}/{date}/
     ↓
   Databricks Auto Loader
     ↓
   bronze.{table} (Delta)
```

---

## 2. Config table <a name="config-table"></a>

The brain of the system. Store in Azure SQL DB or as a Delta table.

```sql
CREATE TABLE ingestion_config (
    source_id          INT           NOT NULL,
    source_name        VARCHAR(100)  NOT NULL,
    is_active          BIT           NOT NULL DEFAULT 1,  -- toggle without deleting
    base_url           VARCHAR(500)  NOT NULL,
    endpoint           VARCHAR(200)  NOT NULL,
    http_method        VARCHAR(10)   NOT NULL DEFAULT 'GET',
    auth_type          VARCHAR(50)   NOT NULL,
    secret_name        VARCHAR(200)  NOT NULL,  -- Key Vault reference, never raw keys
    pagination_type    VARCHAR(50),             -- cursor | offset | none
    page_size          INT           DEFAULT 1000,
    result_json_path   VARCHAR(200),            -- $.data or $.results[*]
    bronze_container   VARCHAR(100)  NOT NULL DEFAULT 'bronze',
    bronze_folder      VARCHAR(200)  NOT NULL,
    bronze_format      VARCHAR(20)   NOT NULL DEFAULT 'json',
    load_type          VARCHAR(20)   NOT NULL DEFAULT 'full',  -- full | incremental
    watermark_column   VARCHAR(100),            -- for incremental only
    last_watermark     DATETIME,                -- ADF updates after each run
    schedule_cron      VARCHAR(50),
    target_delta_table VARCHAR(200)  NOT NULL,
    created_at         DATETIME      NOT NULL DEFAULT GETDATE(),
    updated_at         DATETIME      NOT NULL DEFAULT GETDATE()
);
```

Seed with two sources:

```sql
INSERT INTO ingestion_config VALUES
(1, 'advertisers_api', 1,
 'https://api.adplatform.com/v2', '/advertisers',
 'GET', 'bearer_token', 'kv-adplatform-token',
 'cursor', 1000, '$.data',
 'bronze', '/advertisers/incoming', 'json',
 'full', NULL, NULL,
 '0 2 * * *', 'bronze.advertisers_raw',
 GETDATE(), GETDATE()),

(2, 'campaigns_api', 1,
 'https://api.adplatform.com/v2', '/campaigns',
 'GET', 'bearer_token', 'kv-adplatform-token',
 'cursor', 500, '$.data',
 'bronze', '/campaigns/incoming', 'json',
 'incremental', 'updated_at', '2024-01-01 00:00:00',
 '0 */4 * * *', 'bronze.campaigns_raw',
 GETDATE(), GETDATE());
```

**Key design decisions:**
- `secret_name` stores the Key Vault reference — never the actual token.
- `is_active = 0` pauses a source without touching the pipeline.
- `last_watermark` is updated by ADF after each successful incremental run — self-bookmarking.
- `result_json_path` handles APIs that wrap results differently (`$.data` vs `$.items`).

---

## 3. Pipeline structure <a name="pipeline-structure"></a>

One pipeline, activities in sequence:

```
[Lookup]   Read config table
     ↓
[Filter]   is_active = true only
     ↓
[ForEach]  iterate over active sources (parallel up to 4)
     ↓ inside ForEach:
     ├── [Web]               fetch auth token from Key Vault
     ├── [Copy]              REST API → ADLS Gen2
     └── [StoredProcedure]   update last_watermark
```

Pipeline parameters:

```json
{
  "parameters": {
    "config_table":    { "type": "string", "defaultValue": "ingestion_config" },
    "run_date":        { "type": "string", "defaultValue": "@utcnow()" },
    "override_source": { "type": "string", "defaultValue": "" }
  }
}
```

`override_source` lets you trigger a single source manually without changing config.

---

## 4. Lookup Activity <a name="lookup"></a>

Reads the config table. First activity in the pipeline.

```json
{
  "name": "LookupActiveConfigs",
  "type": "Lookup",
  "typeProperties": {
    "source": {
      "type": "AzureSqlSource",
      "sqlReaderQuery": "SELECT * FROM ingestion_config WHERE is_active = 1"
    },
    "dataset": { "referenceName": "DS_AzureSQL_Config" },
    "firstRowOnly": false
  }
}
```

Output: `@activity('LookupActiveConfigs').output.value` — array of config rows.

---

## 5. Filter Activity <a name="filter"></a>

Handles `override_source` — run one specific source in isolation:

```json
{
  "name": "FilterBySource",
  "type": "Filter",
  "typeProperties": {
    "items": "@activity('LookupActiveConfigs').output.value",
    "condition": "@or(
        equals(pipeline().parameters.override_source, ''),
        equals(item().source_name, pipeline().parameters.override_source)
      )"
  }
}
```

---

## 6. ForEach Activity <a name="foreach"></a>

Iterates over each config row. `batchCount: 4` = up to 4 sources in parallel.

```json
{
  "name": "ForEachSource",
  "type": "ForEach",
  "typeProperties": {
    "isSequential": false,
    "batchCount": 4,
    "items": "@activity('FilterBySource').output.value",
    "activities": []
  }
}
```

Inside the ForEach, `@item()` gives the current config row:
`@item().base_url`, `@item().endpoint`, `@item().secret_name` etc.

---

## 7. Web Activity — Key Vault auth <a name="keyvault"></a>

Never hardcode credentials. ADF managed identity fetches secrets at runtime:

```json
{
  "name": "GetAuthToken",
  "type": "WebActivity",
  "typeProperties": {
    "url": "@concat(
        'https://your-keyvault.vault.azure.net/secrets/',
        item().secret_name,
        '?api-version=7.4'
      )",
    "method": "GET",
    "authentication": {
      "type": "MSI",
      "resource": "https://vault.azure.net"
    }
  }
}
```

Output: `@activity('GetAuthToken').output.value` — raw token string.

---

## 8. Copy Activity — REST to ADLS Gen2 <a name="copy"></a>

### Source (REST API)

```json
{
  "type": "RestSource",
  "httpRequestTimeout": "00:05:00",
  "requestMethod": "@item().http_method",
  "additionalHeaders": {
    "Authorization": "@concat('Bearer ', activity('GetAuthToken').output.value)"
  },
  "paginationRules": {
    "QueryParameters.cursor": "$.next_cursor",
    "EndCondition:$.next_cursor": "Empty"
  }
}
```

For offset pagination instead:

```json
"paginationRules": {
  "QueryParameters.offset": "RANGE:0:@item().page_size",
  "EndCondition:$.data": "Empty"
}
```

### Sink (ADLS Gen2)

```json
{
  "type": "DelimitedTextSink",
  "storeSettings": {
    "type": "AzureBlobFSWriteSettings",
    "fileNamePrefix": "@concat(item().source_name, '_', formatDateTime(utcnow(), 'yyyyMMdd_HHmmss'))"
  }
}
```

### Dynamic sink path

```
@concat(
  item().bronze_container,
  '/',
  item().bronze_folder,
  '/',
  formatDateTime(utcnow(), 'yyyy/MM/dd')
)
```

Final ADLS structure:

```
bronze/
  advertisers/incoming/2024/01/11/advertisers_api_20240111_020000.json
  campaigns/incoming/2024/01/11/campaigns_api_20240111_020000.json
```

---

## 9. Update watermark <a name="watermark"></a>

For incremental sources — runs only after successful copy:

```json
{
  "name": "UpdateWatermark",
  "type": "SqlServerStoredProcedure",
  "typeProperties": {
    "storedProcedureName": "usp_update_watermark",
    "storedProcedureParameters": {
      "source_id":     { "value": "@item().source_id", "type": "Int32" },
      "new_watermark": { "value": "@utcnow()",         "type": "DateTime" }
    }
  },
  "dependsOn": [{
    "activity": "CopyFromRestToBronze",
    "dependencyConditions": ["Succeeded"]
  }]
}
```

```sql
CREATE PROCEDURE usp_update_watermark
    @source_id     INT,
    @new_watermark DATETIME
AS
    UPDATE ingestion_config
    SET    last_watermark = @new_watermark,
           updated_at     = GETDATE()
    WHERE  source_id = @source_id;
```

---

## 10. Failure handling <a name="failures"></a>

Log errors without stopping the ForEach — if one source fails, others keep running:

```json
{
  "name": "LogFailure",
  "type": "SqlServerStoredProcedure",
  "typeProperties": {
    "storedProcedureName": "usp_log_pipeline_error",
    "storedProcedureParameters": {
      "source_name":   { "value": "@item().source_name" },
      "error_message": { "value": "@activity('CopyFromRestToBronze').error.message" },
      "run_id":        { "value": "@pipeline().RunId" },
      "failed_at":     { "value": "@utcnow()" }
    }
  },
  "dependsOn": [{
    "activity": "CopyFromRestToBronze",
    "dependencyConditions": ["Failed"]
  }]
}
```

---

## 11. Trigger Databricks after copy <a name="databricks"></a>

Outside the ForEach, after all copies complete:

```json
{
  "name": "TriggerDatabricksJob",
  "type": "DatabricksSparkPython",
  "typeProperties": {
    "pythonFile": "dbfs:/jobs/bronze_to_silver.py",
    "parameters": ["@formatDateTime(utcnow(), 'yyyy-MM-dd')"],
    "existingClusterId": "your-job-cluster-id"
  },
  "dependsOn": [{
    "activity": "ForEachSource",
    "dependencyConditions": ["Succeeded", "Failed"]
  }]
}
```

`"Succeeded", "Failed"` — runs even if some sources failed, processes whatever landed.

---

## 12. Adding a new source — zero pipeline changes <a name="new-source"></a>

```sql
INSERT INTO ingestion_config VALUES
(3, 'creatives_api', 1,
 'https://api.adplatform.com/v2', '/creatives',
 'GET', 'bearer_token', 'kv-adplatform-token',
 'none', NULL, '$.items',
 'bronze', '/creatives/incoming', 'json',
 'full', NULL, NULL,
 '0 3 * * *', 'bronze.creatives_raw',
 GETDATE(), GETDATE());
-- Done. Next pipeline run picks it up automatically.
```

---

## 13. Local simulation — no Azure subscription <a name="local"></a>

Full local simulation using Python + SQLite + public free APIs.
Same logic, same config table, same metadata-driven pattern.

### Stack mapping

| Local simulation | Real ADF equivalent |
|---|---|
| SQLite config table | Azure SQL DB or Delta table |
| `get_active_configs()` | Lookup Activity |
| `for config in configs` | ForEach Activity |
| `fetch_from_api()` | Copy Activity — REST source |
| `write_to_bronze()` | Copy Activity — ADLS Gen2 sink |
| `update_watermark()` | Stored Procedure Activity |
| `log_result()` | ADF Monitor + LogFailure Activity |
| local `/bronze/` folder | ADLS Gen2 container |
| `python pipeline.py` | ADF trigger |

### Install

```bash
pip install requests
# sqlite3, json, pathlib — all built-in, nothing else needed
```

### Config DB

```python
# config_db.py
import sqlite3

conn = sqlite3.connect("ingestion_config.db")
conn.row_factory = sqlite3.Row
cur = conn.cursor()

cur.executescript("""
CREATE TABLE IF NOT EXISTS ingestion_config (
    source_id          INTEGER PRIMARY KEY,
    source_name        TEXT NOT NULL,
    is_active          INTEGER NOT NULL DEFAULT 1,
    base_url           TEXT NOT NULL,
    endpoint           TEXT NOT NULL,
    http_method        TEXT NOT NULL DEFAULT 'GET',
    result_json_path   TEXT,
    bronze_folder      TEXT NOT NULL,
    load_type          TEXT NOT NULL DEFAULT 'full',
    watermark_column   TEXT,
    last_watermark     TEXT,
    target_delta_table TEXT NOT NULL
);

INSERT OR IGNORE INTO ingestion_config VALUES (
    1, 'countries_api', 1,
    'https://restcountries.com/v3.1', '/all',
    'GET', NULL, 'bronze/countries/incoming',
    'full', NULL, NULL, 'bronze.countries_raw'
);

INSERT OR IGNORE INTO ingestion_config VALUES (
    2, 'advertisers_api', 1,
    'https://jsonplaceholder.typicode.com', '/users',
    'GET', NULL, 'bronze/advertisers/incoming',
    'full', NULL, NULL, 'bronze.advertisers_raw'
);

INSERT OR IGNORE INTO ingestion_config VALUES (
    3, 'campaigns_api', 1,
    'https://jsonplaceholder.typicode.com', '/posts',
    'GET', NULL, 'bronze/campaigns/incoming',
    'incremental', 'id', '0', 'bronze.campaigns_raw'
);
""")

conn.commit()
conn.close()
print("Config DB ready.")
```

### Pipeline runner

```python
# pipeline.py
import sqlite3, requests, json, pathlib
from datetime import datetime, timezone

def get_active_configs(db_path="ingestion_config.db", override_source=None):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    if override_source:
        cur.execute(
            "SELECT * FROM ingestion_config WHERE is_active=1 AND source_name=?",
            (override_source,)
        )
    else:
        cur.execute("SELECT * FROM ingestion_config WHERE is_active=1")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows

def fetch_from_api(config):
    url = config["base_url"] + config["endpoint"]
    params = {}
    if config["load_type"] == "incremental" and config["last_watermark"]:
        params["_start"] = config["last_watermark"]
    print(f"  → GET {url}")
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    if config["result_json_path"] and isinstance(data, dict):
        data = data[config["result_json_path"]]
    return data

def write_to_bronze(data, config, run_ts):
    date_partition = run_ts.strftime("%Y/%m/%d")
    folder = pathlib.Path(config["bronze_folder"]) / date_partition
    folder.mkdir(parents=True, exist_ok=True)
    filename = f"{config['source_name']}_{run_ts.strftime('%Y%m%d_%H%M%S')}.json"
    filepath = folder / filename
    enriched = [
        {**record,
         "_ingested_at": run_ts.isoformat(),
         "_source_name": config["source_name"],
         "_source_file": str(filepath)}
        for record in data
    ]
    with open(filepath, "w") as f:
        json.dump(enriched, f, indent=2)
    print(f"  ✓ {len(enriched)} records → {filepath}")
    return str(filepath), len(enriched)

def update_watermark(source_id, new_watermark, db_path="ingestion_config.db"):
    conn = sqlite3.connect(db_path)
    conn.execute(
        "UPDATE ingestion_config SET last_watermark=? WHERE source_id=?",
        (str(new_watermark), source_id)
    )
    conn.commit()
    conn.close()

def log_result(source_name, status, rows, error=None, db_path="ingestion_config.db"):
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_run_log (
            run_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            source_name TEXT, status TEXT,
            rows_loaded INTEGER, error_msg TEXT, ran_at TEXT
        )
    """)
    conn.execute(
        "INSERT INTO pipeline_run_log VALUES (NULL,?,?,?,?,?)",
        (source_name, status, rows, error, datetime.now(timezone.utc).isoformat())
    )
    conn.commit()
    conn.close()

def run_pipeline(override_source=None):
    run_ts  = datetime.now(timezone.utc)
    configs = get_active_configs(override_source=override_source)
    print(f"\nPipeline run: {run_ts.strftime('%Y-%m-%d %H:%M:%S')} UTC — {len(configs)} sources\n")
    results = []
    for config in configs:
        print(f"[{config['source_name']}]")
        try:
            data = fetch_from_api(config)
            filepath, row_count = write_to_bronze(data, config, run_ts)
            if config["load_type"] == "incremental" and data:
                wm_col = config["watermark_column"]
                if wm_col and wm_col in data[-1]:
                    update_watermark(config["source_id"], data[-1][wm_col])
                    print(f"  ✓ watermark → {data[-1][wm_col]}")
            log_result(config["source_name"], "SUCCESS", row_count)
            results.append({"source": config["source_name"], "status": "SUCCESS", "rows": row_count})
        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            log_result(config["source_name"], "FAILED", 0, error=str(e))
            results.append({"source": config["source_name"], "status": "FAILED"})

    print("\nSummary:")
    for r in results:
        icon = "✓" if r["status"] == "SUCCESS" else "✗"
        print(f"  {icon} {r['source']:<25} {r['status']} {r.get('rows','')}")

if __name__ == "__main__":
    import sys
    run_pipeline(override_source=sys.argv[1] if len(sys.argv) > 1 else None)
```

### Run

```bash
python config_db.py          # create config DB
python pipeline.py           # run all sources
python pipeline.py campaigns_api  # run one source only
```

### Add a new source — zero code changes

```python
import sqlite3
conn = sqlite3.connect("ingestion_config.db")
conn.execute("""
    INSERT INTO ingestion_config VALUES (
        4, 'todos_api', 1,
        'https://jsonplaceholder.typicode.com', '/todos',
        'GET', NULL, 'bronze/todos/incoming',
        'full', NULL, NULL, 'bronze.todos_raw'
    )
""")
conn.commit()
conn.close()
# Next run picks it up automatically.
```

---

*Next topics: Databricks Workflow orchestration · PySpark transforms · Data Contracts with ODCS*

---

## 14. Triggers — event-based, schedule, tumbling window

### Three trigger types

| Trigger | When it fires | Use case |
|---|---|---|
| **Schedule** | Fixed time interval | "Run every day at 2am" |
| **Tumbling Window** | Fixed intervals, no overlap, with memory | "Process each hour, retry that exact hour if it fails" |
| **Event-based** | When something happens in storage | "Run when a file lands in ADLS" |

### Event-based trigger — Blob created

```json
{
  "name": "TriggerOnBlobCreated",
  "type": "BlobEventsTrigger",
  "typeProperties": {
    "blobPathBeginsWith": "/bronze/advertisers/incoming/",
    "blobPathEndsWith": ".json",
    "events": ["Microsoft.Storage.BlobCreated"],
    "scope": "/subscriptions/{sub-id}/resourceGroups/{rg}/providers/Microsoft.Storage/storageAccounts/{account}"
  }
}
```

Pipeline receives the file automatically:
```
@triggerBody().fileName     → name of the file that arrived
@triggerBody().folderPath   → folder where it landed
```

### Tumbling Window — memory across runs

Schedule has no memory — if the 2am run fails, that gap is lost forever.
Tumbling Window tracks each interval as an independent unit with its own state.

```json
{
  "name": "TumblingWindowHourly",
  "type": "TumblingWindowTrigger",
  "typeProperties": {
    "frequency": "Hour",
    "interval": 1,
    "startTime": "2024-01-01T00:00:00Z",
    "retryPolicy": { "count": 3, "intervalInSeconds": 300 }
  }
}
```

The pipeline knows which window it is processing:
```
@trigger().outputs.windowStartTime   → 2024-01-11T03:00:00Z
@trigger().outputs.windowEndTime     → 2024-01-11T04:00:00Z
```

### Decision rule

| Need | Trigger |
|---|---|
| Strict SLAs per hour, backfill needed | Tumbling Window |
| Simple daily pipeline, no backfill | Schedule |
| Data arrives irregularly, not at fixed times | Event-based |

---

## 15. ADF → Databricks — parameters and monitoring

### Linked Service

```json
{
  "name": "LS_Databricks",
  "type": "AzureDatabricks",
  "typeProperties": {
    "domain": "https://adb-xxx.azuredatabricks.net",
    "authentication": "MSI",
    "existingClusterId": "0111-xxx"
  }
}
```

### Three ways to call Databricks

| Activity | What it runs | When to use |
|---|---|---|
| `DatabricksNotebook` | Notebook by path | Development, prototyping |
| `DatabricksSparkPython` | `.py` file on DBFS | Production |
| `DatabricksSparkJar` | Compiled JAR | Scala/Java in production |

### Pass parameters

```json
{
  "name": "RunSilverTransform",
  "type": "DatabricksSparkPython",
  "typeProperties": {
    "pythonFile": "dbfs:/jobs/bronze_to_silver.py",
    "parameters": [
      "@formatDateTime(utcnow(), 'yyyy-MM-dd')",
      "@pipeline().parameters.override_source",
      "@trigger().outputs.windowStartTime"
    ]
  }
}
```

Receive in Python:
```python
import sys
run_date     = sys.argv[1]   # "2024-01-11"
source       = sys.argv[2]   # "advertisers_api"
window_start = sys.argv[3]   # "2024-01-11T03:00:00Z"
```

ADF Monitor shows status, duration, and a direct `runPageUrl` link to the Spark job in Databricks.

---

## 16. Failures — retry policies, dependency conditions, Azure Monitor

### Retry policy per activity

```json
{
  "policy": {
    "timeout": "0.01:00:00",
    "retry": 3,
    "retryIntervalInSeconds": 60
  }
}
```

3 retries with 60s wait resolves ~95% of transient failures (rate limits, timeouts).

### Dependency conditions

```
[Succeeded]              → only if previous activity succeeded
[Failed]                 → only if it failed — use for error logging
[Succeeded", "Failed"]   → always — use for Databricks trigger after ForEach
[Completed]              → Succeeded + Failed + Skipped
```

Standard robust pattern:
```
CopyFromRestToBronze
    ├── [Succeeded] → UpdateWatermark
    ├── [Succeeded] → TriggerDatabricksJob
    └── [Failed]    → LogFailure
```

### Azure Monitor alert

```
Resource:   your Data Factory
Signal:     PipelineFailedRuns
Condition:  Count > 0 in last 5 minutes
Action:     Email / Teams webhook
```

Without this, failures are silent — nobody knows Bronze didn't update until someone notices stale data in a dashboard hours later.

---

*Next topics: PySpark transforms · Data Contracts with ODCS*

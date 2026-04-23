# pipeline.py
import sqlite3
import requests
import json
import pathlib
from datetime import datetime, timezone

# ── helpers ──────────────────────────────────────────────────────────────────

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
    """Equivalent to ADF Copy Activity REST source."""
    url = config["base_url"] + config["endpoint"]
    params = {}

    # Incremental: add watermark filter if supported by this API
    if config["load_type"] == "incremental" and config["last_watermark"]:
        params["_start"] = config["last_watermark"]   # JSONPlaceholder ignores this
                                                       # but the pattern is correct

    print(f"  → GET {url}  params={params}")
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    # If API wraps results in a key (e.g. {"results": [...]}) extract it
    if config["result_json_path"] and isinstance(data, dict):
        data = data[config["result_json_path"]]

    return data


def write_to_bronze(data, config, run_ts):
    """
    Equivalent to ADF Copy Activity sink.
    Mirrors ADLS Gen2 path structure:
      bronze/{folder}/{yyyy}/{MM}/{dd}/{source}_{timestamp}.json
    """
    date_partition = run_ts.strftime("%Y/%m/%d")
    folder = pathlib.Path(config["bronze_folder"]) / date_partition
    folder.mkdir(parents=True, exist_ok=True)

    filename = f"{config['source_name']}_{run_ts.strftime('%Y%m%d_%H%M%S')}.json"
    filepath = folder / filename

    # Add ingestion metadata to every record — same as _ingested_at in Auto Loader
    enriched = [
        {
            **record,
            "_ingested_at":  run_ts.isoformat(),
            "_source_name":  config["source_name"],
            "_source_file":  str(filepath),
            "_pipeline_run": run_ts.strftime("%Y%m%d_%H%M%S"),
        }
        for record in data
    ]

    with open(filepath, "w") as f:
        json.dump(enriched, f, indent=2)

    print(f"  ✓ wrote {len(enriched)} records → {filepath}")
    return str(filepath), len(enriched)


def update_watermark(source_id, new_watermark, db_path="ingestion_config.db"):
    """Equivalent to ADF stored procedure activity."""
    conn = sqlite3.connect(db_path)
    conn.execute(
        "UPDATE ingestion_config SET last_watermark=? WHERE source_id=?",
        (str(new_watermark), source_id)
    )
    conn.commit()
    conn.close()


def log_result(source_name, status, rows, error=None, db_path="ingestion_config.db"):
    """Simple run log — equivalent to ADF monitor + LogFailure activity."""
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_run_log (
            run_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            source_name TEXT,
            status      TEXT,
            rows_loaded INTEGER,
            error_msg   TEXT,
            ran_at      TEXT
        )
    """)
    conn.execute(
        "INSERT INTO pipeline_run_log VALUES (NULL,?,?,?,?,?)",
        (source_name, status, rows, error, datetime.now(timezone.utc).isoformat())
    )
    conn.commit()
    conn.close()


# ── main pipeline ─────────────────────────────────────────────────────────────

def run_pipeline(override_source=None):
    run_ts = datetime.now(timezone.utc)
    configs = get_active_configs(override_source=override_source)

    print(f"\n{'='*60}")
    print(f"Pipeline run: {run_ts.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"Active sources: {len(configs)}")
    print(f"{'='*60}\n")

    results = []

    for config in configs:
        print(f"[{config['source_name']}]")
        try:
            # Step 1: fetch
            data = fetch_from_api(config)

            # Step 2: write to Bronze
            filepath, row_count = write_to_bronze(data, config, run_ts)

            # Step 3: update watermark for incremental sources
            if config["load_type"] == "incremental" and data:
                wm_col = config["watermark_column"]
                if wm_col and wm_col in data[-1]:
                    new_wm = data[-1][wm_col]
                    update_watermark(config["source_id"], new_wm)
                    print(f"  ✓ watermark updated → {new_wm}")

            log_result(config["source_name"], "SUCCESS", row_count)
            results.append({"source": config["source_name"], "status": "SUCCESS", "rows": row_count})

        except Exception as e:
            print(f"  ✗ FAILED: {e}")
            log_result(config["source_name"], "FAILED", 0, error=str(e))
            results.append({"source": config["source_name"], "status": "FAILED", "error": str(e)})
            # continue to next source — don't abort the whole pipeline

    print(f"\n{'='*60}")
    print("Run summary:")
    for r in results:
        icon = "✓" if r["status"] == "SUCCESS" else "✗"
        rows = r.get("rows", "-")
        print(f"  {icon} {r['source']:<25} {r['status']:<10} {rows} rows")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    import sys
    override = sys.argv[1] if len(sys.argv) > 1 else None
    run_pipeline(override_source=override)

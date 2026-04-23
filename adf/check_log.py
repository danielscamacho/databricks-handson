# check_log.py
import sqlite3
conn = sqlite3.connect("ingestion_config.db")
conn.row_factory = sqlite3.Row
rows = conn.execute(
    "SELECT * FROM pipeline_run_log ORDER BY ran_at DESC LIMIT 20"
).fetchall()
for r in rows:
    print(dict(r))
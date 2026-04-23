# inspect_bronze.py — verify what landed
import json, pathlib

for f in sorted(pathlib.Path("bronze").rglob("*.json")):
    with open(f) as fh:
        data = json.load(fh)
    print(f"\n{f}")
    print(f"  rows: {len(data)}")
    print(f"  keys: {list(data[0].keys())[:6]}...")
    print(f"  sample _ingested_at: {data[0]['_ingested_at']}")
    print(f"  sample _source_file: {data[0]['_source_file']}")


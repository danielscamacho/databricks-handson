# config_db.py
import sqlite3

conn = sqlite3.connect("ingestion_config.db")
conn.row_factory = sqlite3.Row   # lets you access columns by name
cur = conn.cursor()

cur.executescript("""
CREATE TABLE IF NOT EXISTS ingestion_config (
    source_id          INTEGER PRIMARY KEY,
    source_name        TEXT NOT NULL,
    is_active          INTEGER NOT NULL DEFAULT 1,
    base_url           TEXT NOT NULL,
    endpoint           TEXT NOT NULL,
    http_method        TEXT NOT NULL DEFAULT 'GET',
    result_json_path   TEXT,          -- top-level key where records live e.g. 'results'
    bronze_folder      TEXT NOT NULL, -- local path mirrors ADLS structure
    load_type          TEXT NOT NULL DEFAULT 'full',
    watermark_column   TEXT,
    last_watermark     TEXT,
    target_delta_table TEXT NOT NULL
);

-- Source 1: public REST countries API (no auth)
INSERT OR IGNORE INTO ingestion_config VALUES (
    1, 'countries_api', 1,
    'https://restcountries.com/v3.1', '/all',
    'GET', NULL,
    'bronze/countries/incoming',
    'full', NULL, NULL,
    'bronze.countries_raw'
);

-- Source 2: public JSONPlaceholder users API (simulates advertiser data)
INSERT OR IGNORE INTO ingestion_config VALUES (
    2, 'advertisers_api', 1,
    'https://jsonplaceholder.typicode.com', '/users',
    'GET', NULL,
    'bronze/advertisers/incoming',
    'full', NULL, NULL,
    'bronze.advertisers_raw'
);

-- Source 3: posts — simulates campaigns, incremental load
INSERT OR IGNORE INTO ingestion_config VALUES (
    3, 'campaigns_api', 1,
    'https://jsonplaceholder.typicode.com', '/posts',
    'GET', NULL,
    'bronze/campaigns/incoming',
    'incremental', 'id', '0',
    'bronze.campaigns_raw'
);
""")

conn.commit()
conn.close()
print("Config DB ready.")

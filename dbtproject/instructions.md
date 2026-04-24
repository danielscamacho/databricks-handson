# dbt Snapshots — SCD Type 2 without writing MERGE

> Consolidated from hands-on exercises. Last updated: April 2026.

---

## Table of Contents

1. [Why it exists](#why)
2. [Setup](#setup)
3. [The snapshot](#snapshot)
4. [Columns dbt adds automatically](#columns)
5. [Two snapshot strategies](#strategies)
6. [Type 1 dims in dbt — incremental model](#type1)
7. [Using the snapshot downstream](#downstream)
8. [Hands-on exercise — simulate SCD Type 2](#exercise)
9. [MERGE manual vs dbt snapshot](#comparison)

---

## 1. Why it exists <a name="why"></a>

The MERGE INTO for SCD Type 2 is ~30 lines and easy to break. In production with 20 dimensions, you maintain 20 separate MERGEs. dbt snapshots centralizes that logic — define the strategy once, dbt does the rest.

---

## 2. Setup <a name="setup"></a>

```bash
# Install with uv (recommended — handles Python version conflicts)
uv venv .venv-dbt --python 3.11
source .venv-dbt/bin/activate
uv pip install dbt-databricks

# Initialize project
dbt init platform
cd platform
```

Project structure:

```
platform/
    ├── dbt_project.yml
    ├── profiles.yml
    ├── models/
    │     └── gold/
    │           └── dim_campaign.sql
    └── snapshots/
          ├── schema.yml          ← source declarations
          └── snap_dim_user.sql
```

### profiles.yml

```yaml
platform:
  target: dev
  outputs:
    dev:
      type: databricks
      host: adb-xxx.azuredatabricks.net
      http_path: /sql/1.0/warehouses/xxx
      token: "{{ env_var('DBT_DATABRICKS_TOKEN') }}"
      catalog: dbt_sandbox
      schema: silver
      threads: 4
```

### snapshots/schema.yml — declare sources here

```yaml
version: 2

sources:
  - name: bronze
    catalog: dbt_sandbox
    schema: bronze
    tables:
      - name: users_raw
```

> dbt does not infer sources automatically — you must declare them.

> Do NOT use `--` comments inside `config()` blocks — Jinja does not support them and throws a compilation error.

---

## 3. The snapshot <a name="snapshot"></a>

```sql
-- snapshots/snap_dim_user.sql

{% snapshot snap_dim_user %}

{{
    config(
        target_schema='gold',
        target_database='dbt_sandbox',
        unique_key='user_id',
        strategy='check',
        check_cols=['segment', 'country', 'device_type'],
        invalidate_hard_deletes=True
    )
}}

SELECT
    user_id,
    segment,
    country,
    device_type,
    browser,
    current_timestamp() AS _source_ts
FROM {{ source('bronze', 'users_raw') }}
WHERE is_current_record = true

{% endsnapshot %}
```

```bash
dbt snapshot
```

dbt generates and executes the MERGE INTO automatically — expires old row, inserts new version.

---

## 4. Columns dbt adds automatically <a name="columns"></a>

| dbt column | Your equivalent | What it contains |
|---|---|---|
| `dbt_scd_id` | surrogate_key | Unique hash per version |
| `dbt_valid_from` | valid_from | When this version became active |
| `dbt_valid_to` | valid_to | When superseded (NULL = still active) |
| `dbt_is_current` | is_current | true for the active version |

---

## 5. Two snapshot strategies <a name="strategies"></a>

### strategy: check — detect changes by columns

```sql
{{
    config(
        strategy='check',
        unique_key='user_id',
        check_cols=['segment', 'country', 'device_type']
    )
}}
```

Use when you know exactly which columns matter. More explicit.

### strategy: timestamp — detect changes by timestamp

```sql
{{
    config(
        strategy='timestamp',
        unique_key='user_id',
        updated_at='updated_at'
    )
}}
```

Use when the source already has a reliable `updated_at`. Faster — no column-by-column comparison.

**Rule:** source has `updated_at` → `timestamp`. Otherwise → `check`.

---

## 6. Type 1 dims in dbt — incremental model <a name="type1"></a>

For Type 1 dimensions, use a regular model with `unique_key` — not a snapshot:

```sql
-- models/gold/dim_campaign.sql

{{ config(
    materialized='incremental',
    unique_key='campaign_id'
) }}

SELECT
    campaign_id,
    advertiser_id,
    name,
    objective,
    channel,
    budget_usd,
    start_date,
    end_date,
    status,
    current_timestamp() AS _updated_at
FROM {{ source('silver', 'campaigns_clean') }}

{% if is_incremental() %}
    WHERE updated_at > (SELECT MAX(_updated_at) FROM {{ this }})
{% endif %}
```

`materialized='incremental'` with `unique_key` does an automatic Type 1 MERGE — if `campaign_id` exists, update. If not, insert.

---

## 7. Using the snapshot downstream <a name="downstream"></a>

```sql
-- models/gold/fact_impressions.sql

SELECT
    i.impression_id,
    i.impression_ts,
    u.dbt_scd_id     AS user_key,
    c.campaign_id    AS campaign_key,
    i.bid_price_usd,
    i.is_clicked,
    i.is_converted
FROM {{ source('silver', 'impressions_clean') }} i
JOIN {{ ref('snap_dim_user') }} u
  ON  u.user_id        = i.user_id
  AND u.dbt_valid_from <= i.impression_ts
  AND (u.dbt_valid_to  > i.impression_ts OR u.dbt_valid_to IS NULL)
JOIN {{ ref('dim_campaign') }} c
  ON  c.campaign_id = i.campaign_id
```

The join to the snapshot uses `valid_from / valid_to` to find the user version **at the exact moment of the impression**.

---

## 8. Hands-on exercise — simulate SCD Type 2 <a name="exercise"></a>

### Create source table in Databricks

```sql
CREATE CATALOG IF NOT EXISTS dbt_sandbox;
CREATE SCHEMA  IF NOT EXISTS dbt_sandbox.bronze;
CREATE SCHEMA  IF NOT EXISTS dbt_sandbox.gold;

CREATE OR REPLACE TABLE dbt_sandbox.bronze.users_raw (
    user_id           STRING,
    segment           STRING,
    country           STRING,
    device_type       STRING,
    browser           STRING,
    is_current_record BOOLEAN,
    updated_at        TIMESTAMP
);

INSERT INTO dbt_sandbox.bronze.users_raw VALUES
('U001', 'standard', 'ES', 'desktop', 'chrome',  true, '2024-01-10T00:00:00'),
('U002', 'premium',  'FR', 'mobile',  'safari',  true, '2024-01-10T00:00:00'),
('U003', 'new_user', 'DE', 'desktop', 'firefox', true, '2024-01-10T00:00:00');
```

### Run first snapshot

```bash
dbt snapshot
```

Expected — 3 rows, all active:

| user_id | segment | dbt_valid_from | dbt_valid_to | dbt_is_current |
|---|---|---|---|---|
| U001 | standard | 2024-01-10 | null | true |
| U002 | premium | 2024-01-10 | null | true |
| U003 | new_user | 2024-01-10 | null | true |

### Simulate changes

```sql
-- U001 upgrades segment
UPDATE dbt_sandbox.bronze.users_raw
SET segment = 'premium', updated_at = '2024-02-01T00:00:00'
WHERE user_id = 'U001';

-- U003 changes device
UPDATE dbt_sandbox.bronze.users_raw
SET device_type = 'mobile', updated_at = '2024-02-01T00:00:00'
WHERE user_id = 'U003';
```

```bash
dbt snapshot
```

Expected — 5 rows, U001 and U003 have history:

| user_id | segment | device_type | dbt_valid_from | dbt_valid_to | dbt_is_current |
|---|---|---|---|---|---|
| U001 | standard | desktop | 2024-01-10 | 2024-02-01 | false |
| U001 | premium | desktop | 2024-02-01 | null | true |
| U002 | premium | mobile | 2024-01-10 | null | true |
| U003 | new_user | desktop | 2024-01-10 | 2024-02-01 | false |
| U003 | new_user | mobile | 2024-02-01 | null | true |

### One more change — U001 changes again

```sql
UPDATE dbt_sandbox.bronze.users_raw
SET segment = 'enterprise', updated_at = '2024-03-01T00:00:00'
WHERE user_id = 'U001';
```

```bash
dbt snapshot
```

U001 full history:

| user_id | segment | dbt_valid_from | dbt_valid_to | dbt_is_current |
|---|---|---|---|---|
| U001 | standard | 2024-01-10 | 2024-02-01 | false |
| U001 | premium | 2024-02-01 | 2024-03-01 | false |
| U001 | enterprise | 2024-03-01 | null | true |

Three versions. Complete history. No MERGE written.

---

## 9. MERGE manual vs dbt snapshot <a name="comparison"></a>

| Situation | Use |
|---|---|
| Full control over SQL logic | MERGE manual |
| Complex expiration logic | MERGE manual |
| Team already uses dbt | dbt snapshot |
| Multiple SCD2 dimensions | dbt snapshot — less maintenance |
| Databricks SQL only, no dbt | MERGE manual |
| Want lineage + auto documentation | dbt snapshot |

---

## Common errors and fixes

| Error | Cause | Fix |
|---|---|---|
| `invalid syntax for function call` | `--` comments inside `config()` | Remove all comments from config block |
| `source not found` | Missing `schema.yml` declaration | Add source to `snapshots/schema.yml` |
| `TABLE_OR_VIEW_NOT_FOUND` | Source table doesn't exist in Databricks | Create the table first |
| Python import error (`mashumaro`) | Python 3.14 incompatible with dbt | Use `uv venv --python 3.11` |

---

*Next topics: Late-arriving records in SCD Type 2 · Kimball dimensional model · Data Contracts with ODCS*

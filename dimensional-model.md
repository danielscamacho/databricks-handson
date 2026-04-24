# Dimensional Model — Study Guide

> Consolidated from hands-on exercises. Last updated: April 2026.

---

## Table of Contents

1. [RTB flow — the domain](#rtb)
2. [Entities](#entities)
3. [Granularity — the most important decision](#granularity)
4. [The full dimensional model](#model)
5. [fact_impressions](#fact)
6. [dim_date — no SCD](#dim-date)
7. [dim_advertiser — SCD Type 1](#dim-advertiser)
8. [dim_campaign — SCD Type 1](#dim-campaign)
9. [dim_user — SCD Type 2](#dim-user)
10. [The join that connects everything](#join)
11. [Why each dim has its SCD type](#why)
12. [SCD Types — full reference](#scd-reference)

---

## 1. RTB flow — the domain <a name="rtb"></a>

```
Advertiser defines a campaign
        ↓
User visits a website
        ↓
In ~100ms: real-time auction (RTB)
        ↓
Company wins the auction → serves the ad
        ↓
IMPRESSION recorded   ← main fact, millions per minute
        ↓
User clicks the ad    ← click event
        ↓
User buys something   ← conversion event
```

Three events. Each is a fact. The most voluminous — millions per minute — is the **impression**.

---

## 2. Entities <a name="entities"></a>

Before designing tables, identify what exists in this domain:

```
CAMPAIGN    — what is being advertised, who pays, how much
ADVERTISER  — the company paying (Adidas, Nike, Amazon)
USER        — who sees the ad (anonymous, identified by cookie/device ID)
PLACEMENT   — where the ad appears (website, app, position)
CREATIVE    — the ad itself (300x250 banner, 15s video)
DATE/TIME   — when it happened
```

Each impression connects all these entities at a single point in time.

---

## 3. Granularity — the most important decision <a name="granularity"></a>

**Granularity** = what exactly ONE row in your fact table represents.

| Granularity | One row = | Volume |
|---|---|---|
| One impression | One ad served to one user | Very high — billions/day |
| One click | One user who clicked | High — millions/day |
| Hourly aggregate per campaign | Total impressions per campaign per hour | Low — thousands/day |

For an analytical Lakehouse: **one row = one impression**. Most atomic granularity — you can always aggregate up, never disaggregate down.

---

## 4. The full dimensional model <a name="model"></a>

```
                    dim_date
                       │
dim_advertiser ──── fact_impressions ──── dim_campaign
                       │
                    dim_user (SCD2)
                       │
                    dim_creative
                       │
                    dim_placement
```

---

## 5. fact_impressions <a name="fact"></a>

```sql
CREATE TABLE gold.fact_impressions (
    -- surrogate keys → joins to dims
    impression_id       STRING    NOT NULL,  -- natural PK of the event
    date_key            INT       NOT NULL,  -- FK → dim_date (20240111)
    campaign_key        BIGINT    NOT NULL,  -- FK → dim_campaign.surrogate_key
    advertiser_key      BIGINT    NOT NULL,  -- FK → dim_advertiser.surrogate_key
    user_key            BIGINT    NOT NULL,  -- FK → dim_user.surrogate_key ← SCD2
    creative_key        BIGINT    NOT NULL,  -- FK → dim_creative.surrogate_key
    placement_key       BIGINT    NOT NULL,  -- FK → dim_placement.surrogate_key

    -- measures
    impression_ts       TIMESTAMP NOT NULL,  -- exact timestamp of the event
    bid_price_usd       DECIMAL(10,6),       -- what was paid in the auction
    floor_price_usd     DECIMAL(10,6),       -- auction minimum price
    is_viewable         BOOLEAN,             -- ad was visible for >1s
    is_clicked          BOOLEAN,             -- click happened on this impression
    is_converted        BOOLEAN,             -- conversion attributed to this impression

    -- partition
    event_date          DATE      NOT NULL   -- for partition pruning
)
USING DELTA
PARTITIONED BY (event_date)
CLUSTER BY (campaign_key, user_key);
```

> **Rule:** a fact table contains only **foreign keys and numeric measures**. Never descriptive text — those go in the dims.

---

## 6. dim_date — no SCD <a name="dim-date"></a>

```sql
CREATE TABLE gold.dim_date (
    date_key        INT     NOT NULL,  -- 20240111 — integer, faster join than DATE
    full_date       DATE    NOT NULL,
    year            INT,
    quarter         INT,
    month           INT,
    week            INT,
    day_of_week     INT,
    is_weekend      BOOLEAN,
    is_holiday      BOOLEAN
)
USING DELTA;
-- Generated once for 10 years, never touched again
```

---

## 7. dim_advertiser — SCD Type 1 <a name="dim-advertiser"></a>

Name or country of the advertiser may be corrected. No history needed — if Adidas corrects their name, all historical impressions should reflect the correct name.

```sql
CREATE TABLE gold.dim_advertiser (
    surrogate_key   BIGINT  GENERATED ALWAYS AS IDENTITY,
    advertiser_id   STRING  NOT NULL,
    name            STRING,
    country         STRING,
    industry        STRING,   -- retail, fashion, travel...
    _updated_at     TIMESTAMP
)
USING DELTA;

-- Type 1: overwrite, no history
MERGE INTO gold.dim_advertiser AS target
USING incoming AS source
  ON target.advertiser_id = source.advertiser_id
WHEN MATCHED THEN
  UPDATE SET
    target.name        = source.name,
    target.country     = source.country,
    target.industry    = source.industry,
    target._updated_at = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT (advertiser_id, name, country, industry, _updated_at)
  VALUES (source.advertiser_id, source.name, source.country,
          source.industry, current_timestamp());
```

---

## 8. dim_campaign — SCD Type 1 <a name="dim-campaign"></a>

```sql
CREATE TABLE gold.dim_campaign (
    surrogate_key   BIGINT      GENERATED ALWAYS AS IDENTITY,
    campaign_id     STRING      NOT NULL,
    advertiser_id   STRING      NOT NULL,
    name            STRING,
    objective       STRING,     -- awareness | retargeting | conversion
    channel         STRING,     -- display | video | native
    budget_usd      DECIMAL(12,2),
    start_date      DATE,
    end_date        DATE,
    status          STRING,     -- active | paused | ended
    _updated_at     TIMESTAMP
)
USING DELTA;
```

Type 1 — if a campaign changes budget or status, you want the current value in all analyses. You don't need to know the budget was 50k before it became 80k.

---

## 9. dim_user — SCD Type 2 <a name="dim-user"></a>

The user's segment changes. You need to know **what segment they had at the moment of the impression** — not their segment today. Otherwise your performance analysis by segment is wrong.

```sql
CREATE TABLE gold.dim_user (
    surrogate_key   BIGINT    GENERATED ALWAYS AS IDENTITY,
    user_id         STRING    NOT NULL,   -- cookie ID / device ID / hashed email
    segment         STRING,               -- premium | standard | new_user
    country         STRING,
    device_type     STRING,               -- desktop | mobile | tablet
    browser         STRING,
    valid_from      TIMESTAMP NOT NULL,
    valid_to        TIMESTAMP,            -- NULL = still active
    is_current      BOOLEAN   NOT NULL
)
USING DELTA;
```

**Step 1 — expire old row:**

```sql
MERGE INTO gold.dim_user AS target
USING incoming_users AS source
  ON  target.user_id    = source.user_id
  AND target.is_current = true
WHEN MATCHED AND (
    target.segment     <> source.segment  OR
    target.country     <> source.country  OR
    target.device_type <> source.device_type
) THEN UPDATE SET
    target.valid_to    = source.valid_from,
    target.is_current  = false,
    target._updated_at = current_timestamp();
```

**Step 2 — insert new version:**

```sql
INSERT INTO gold.dim_user
  (user_id, segment, country, device_type, browser,
   valid_from, valid_to, is_current)
SELECT
    source.user_id,
    source.segment,
    source.country,
    source.device_type,
    source.browser,
    source.valid_from,
    NULL,
    true
FROM incoming_users AS source
WHERE NOT EXISTS (
    SELECT 1 FROM gold.dim_user existing
    WHERE existing.user_id    = source.user_id
      AND existing.valid_from = source.valid_from
      AND existing.is_current = true
);
```

---

## 10. The join that connects everything <a name="join"></a>

When recording an impression, store the `surrogate_key` of the user **at that moment**:

```sql
INSERT INTO gold.fact_impressions (user_key, campaign_key, impression_ts, ...)
SELECT
    u.surrogate_key,   -- key of the CURRENT version of the user at ingestion time
    c.surrogate_key,
    i.impression_ts,
    ...
FROM staging.impressions i
JOIN gold.dim_user u
  ON  u.user_id    = i.user_id
  AND u.is_current = true    -- join against version active at ingestion time
JOIN gold.dim_campaign c
  ON  c.campaign_id = i.campaign_id;
```

Six months later, when the user changed to 'enterprise' (surrogate_key = 3), historical impressions still point to surrogate_key = 1 → still show `segment = 'premium'`. History is correct.

### Querying historical performance by segment

```sql
-- CTR by user segment — historically accurate
SELECT
    u.segment,
    COUNT(*)                              AS impressions,
    SUM(CASE WHEN f.is_clicked THEN 1 END) AS clicks,
    ROUND(SUM(CASE WHEN f.is_clicked THEN 1 END) * 100.0 / COUNT(*), 4) AS ctr_pct
FROM gold.fact_impressions f
JOIN gold.dim_user u     ON f.user_key     = u.surrogate_key
JOIN gold.dim_campaign c ON f.campaign_key = c.surrogate_key
WHERE f.event_date BETWEEN '2024-01-01' AND '2024-03-31'
GROUP BY u.segment
ORDER BY ctr_pct DESC;
```

---

## 11. Why each dim has its SCD type <a name="why"></a>

| Dimension | SCD Type | Reason |
|---|---|---|
| dim_date | None | Never changes |
| dim_advertiser | Type 1 | Corrections only, history irrelevant |
| dim_campaign | Type 1 | Current state always relevant for operations |
| dim_user | **Type 2** | Segment at moment of impression matters for analysis |
| dim_creative | Type 1 | Banner metadata, corrections only |
| dim_placement | Type 1 | URL/position, corrections only |

**The answer to "why Type 2 for dim_user and Type 1 for dim_campaign?"**

Because performance analysis by user segment needs the segment the user had **at the moment of the impression**, not today's segment. If a user was `new_user` when they saw the ad and converted, that conversion must be attributed to `new_user`. If today they are `premium`, it's irrelevant for that historical event. For campaign, I don't need to know the budget was 50k — I need the current state to operate.

---

## 12. SCD Types — full reference <a name="scd-reference"></a>

The core question all types answer: **an attribute changes. What do you do with the previous value?**

| Type | What it does | History | When in Domain |
|---|---|---|---|
| 1 | Overwrite | None | Corrections, typos, name fixes |
| 2 | New row | Complete | User segment, customer tier |
| 3 | Extra column | Previous value only | Before/after migration analysis |
| 4 | Separate history table | In another table | Real-time + analytics decoupled |

### Type 3 — extra column, only remembers one change back

```sql
-- Table has an extra column: previous_segment
UPDATE silver.customers
SET    previous_segment = segment,
       segment          = 'enterprise',
       _updated_at      = current_timestamp()
WHERE  customer_id = 'C001';
```

Only remembers ONE change back. If the value changes three times, the first value is lost.

**When in Domain:** comparing "before vs now" for migration analysis. E.g. what percentage of users who were `standard` moved to `premium` this quarter, and how did their CTR change.

### Type 4 — separate history table

```sql
-- Main table: current state only, very fast
CREATE TABLE silver.customers_current (
    customer_id STRING, segment STRING, _updated_at TIMESTAMP
);

-- History table: all changes
CREATE TABLE silver.customers_history (
    customer_id STRING, segment STRING, valid_from TIMESTAMP, valid_to TIMESTAMP
);
```

**When in Domain:** when real-time queries need only current state (bidding, targeting) and analytical queries need full history (reporting, attribution). Decouples performance — current table is small and fast, history table can be enormous without affecting real-time.

---

*Next topics: dbt snapshots · PySpark transforms · Data Contracts with ODCS*

---

## 13. Late-arriving records in SCD Type 2

### What is a late-arriving record

Your pipeline runs every day at 2am. The source sends the previous day's changes. Then one day:

```
Pipeline of 2024-01-15 receives a record for U001
dated 2024-01-08 — one week late
```

Happens constantly in production: source bug, failed retry, migration.

### Current state of the dimension before it arrives

```
U001 | standard   | valid_from: 2024-01-10 | valid_to: 2024-01-13 | is_current: false
U001 | premium    | valid_from: 2024-01-13 | valid_to: null       | is_current: true
```

### What the late record says

```
U001 | enterprise | valid_from: 2024-01-08
```

U001 was `enterprise` on Jan 8 — before the `standard` record of Jan 10.

### What your naive MERGE breaks

Your MERGE joins on `is_current = true`. It finds `premium`, sees `enterprise <> premium`, expires `premium` and opens `enterprise` with today's date:

```
U001 | standard   | valid_from: 2024-01-10 | valid_to: 2024-01-13 | is_current: false
U001 | premium    | valid_from: 2024-01-13 | valid_to: 2024-01-15 | is_current: false  ← wrongly expired
U001 | enterprise | valid_from: 2024-01-15 | valid_to: null       | is_current: true   ← wrong date, wrong position
```

Three problems:
1. `enterprise` gets `valid_from: 2024-01-15` — today, not when it actually happened
2. Historical order is wrong — `enterprise` should be the first version
3. `premium` was expired unnecessarily — U001 was still `premium` after Jan 8

### The fix — classify before merging

**Step 1 — identify late-arriving records:**

```sql
CREATE OR REPLACE TEMP VIEW classified_incoming AS
SELECT
    src.*,
    tgt_current.valid_from    AS current_valid_from,
    tgt_current.surrogate_key AS current_key,
    CASE
        WHEN src.valid_from < tgt_current.valid_from THEN true
        ELSE false
    END AS is_late_arriving
FROM incoming_users src
LEFT JOIN gold.dim_user tgt_current
  ON  tgt_current.user_id    = src.user_id
  AND tgt_current.is_current = true;
```

**Step 2 — process normal records as usual (only non-late):**

```sql
MERGE INTO gold.dim_user AS target
USING (SELECT * FROM classified_incoming WHERE is_late_arriving = false) AS source
  ON  target.user_id    = source.user_id
  AND target.is_current = true
WHEN MATCHED AND (
    target.segment <> source.segment OR
    target.country <> source.country
) THEN UPDATE SET
    target.valid_to    = source.valid_from,
    target.is_current  = false,
    target._updated_at = current_timestamp();

INSERT INTO gold.dim_user
  (user_id, segment, country, valid_from, valid_to, is_current)
SELECT user_id, segment, country, valid_from, NULL, true
FROM classified_incoming
WHERE is_late_arriving = false
  AND NOT EXISTS (
      SELECT 1 FROM gold.dim_user e
      WHERE e.user_id    = user_id
        AND e.valid_from = valid_from
        AND e.is_current = true
  );
```

**Step 3 — find which row owns the late record's moment in time:**

```sql
CREATE OR REPLACE TEMP VIEW late_context AS
SELECT
    late.user_id,
    late.segment        AS late_segment,
    late.country        AS late_country,
    late.valid_from     AS late_valid_from,
    owner.surrogate_key AS owner_key,
    owner.valid_from    AS owner_valid_from,
    owner.valid_to      AS owner_valid_to
FROM classified_incoming late
JOIN gold.dim_user owner
  ON  owner.user_id    = late.user_id
  AND owner.valid_from <= late.valid_from
  AND (owner.valid_to  > late.valid_from OR owner.valid_to IS NULL)
WHERE late.is_late_arriving = true;
```

**Step 3a — trim the existing row to end just before the late record:**

```sql
MERGE INTO gold.dim_user AS target
USING late_context AS source
  ON target.surrogate_key = source.owner_key
WHEN MATCHED THEN UPDATE SET
    target.valid_to    = source.late_valid_from,
    target.is_current  = false,
    target._updated_at = current_timestamp();
```

**Step 3b — insert the late record in the correct position:**

```sql
INSERT INTO gold.dim_user
  (user_id, segment, country, valid_from, valid_to, is_current)
SELECT
    user_id,
    late_segment,
    late_country,
    late_valid_from,
    owner_valid_from,   -- ends where the trimmed row used to start
    false               -- not current — there are later versions
FROM late_context;
```

### The correct result

```
U001 | enterprise | valid_from: 2024-01-08 | valid_to: 2024-01-10 | is_current: false
U001 | standard   | valid_from: 2024-01-10 | valid_to: 2024-01-13 | is_current: false
U001 | premium    | valid_from: 2024-01-13 | valid_to: null       | is_current: true
```

Late record in the right place. History coherent. `premium` untouched.

### How to explain it in a meeting

> "The standard MERGE only looks at `is_current = true`, so a late record expires the active row with today's date — wrong. The fix is to classify first: if the record's `valid_from` is earlier than the active row's `valid_from`, it's late. For late records, find the row that owns that moment in time, trim it to end just before the late record, and insert the late record in the gap. Three operations instead of two."

---

## 14. Kimball hands-on — full model in SQL

### The scenario

Digital advertising company. Two active campaigns. Users that change segment over time. Impressions recorded across days.

Goal: **CTR by user segment and campaign, historically correct.**

### dim_date

```sql
CREATE OR REPLACE TABLE kimball.dim_date (
    date_key    INT     NOT NULL,
    full_date   DATE    NOT NULL,
    year        INT,
    month       INT,
    day         INT,
    is_weekend  BOOLEAN
) USING DELTA;

INSERT INTO kimball.dim_date VALUES
(20240110, '2024-01-10', 2024, 1, 10, false),
(20240111, '2024-01-11', 2024, 1, 11, false),
(20240112, '2024-01-12', 2024, 1, 12, false),
(20240113, '2024-01-13', 2024, 1, 13, true),
(20240114, '2024-01-14', 2024, 1, 14, true),
(20240115, '2024-01-15', 2024, 1, 15, false);
```

### dim_campaign — SCD Type 1

```sql
CREATE OR REPLACE TABLE kimball.dim_campaign (
    surrogate_key   BIGINT  GENERATED ALWAYS AS IDENTITY,
    campaign_id     STRING  NOT NULL,
    name            STRING,
    objective       STRING,
    channel         STRING,
    budget_usd      DECIMAL(12,2),
    status          STRING,
    _updated_at     TIMESTAMP
) USING DELTA;

-- Type 1 MERGE — overwrite, no history
MERGE INTO kimball.dim_campaign AS target
USING incoming AS source
  ON target.campaign_id = source.campaign_id
WHEN MATCHED THEN UPDATE SET
    target.budget_usd  = source.budget_usd,
    target.status      = source.status,
    target._updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT
    (campaign_id, name, objective, channel, budget_usd, status, _updated_at)
VALUES
    (source.campaign_id, source.name, source.objective,
     source.channel, source.budget_usd, source.status, current_timestamp());
```

### dim_user — SCD Type 2

```sql
CREATE OR REPLACE TABLE kimball.dim_user (
    surrogate_key   BIGINT    GENERATED ALWAYS AS IDENTITY,
    user_id         STRING    NOT NULL,
    segment         STRING,
    country         STRING,
    device_type     STRING,
    valid_from      TIMESTAMP NOT NULL,
    valid_to        TIMESTAMP,
    is_current      BOOLEAN   NOT NULL
) USING DELTA;

-- Step 1: expire old row
MERGE INTO kimball.dim_user AS target
USING incoming_users AS source
  ON  target.user_id    = source.user_id
  AND target.is_current = true
WHEN MATCHED AND (
    target.segment     <> source.segment OR
    target.country     <> source.country OR
    target.device_type <> source.device_type
) THEN UPDATE SET
    target.valid_to    = source.valid_from,
    target.is_current  = false,
    target._updated_at = current_timestamp();

-- Step 2: insert new version
INSERT INTO kimball.dim_user
  (user_id, segment, country, device_type, valid_from, valid_to, is_current)
SELECT user_id, segment, country, device_type, valid_from, NULL, true
FROM incoming_users
WHERE NOT EXISTS (
    SELECT 1 FROM kimball.dim_user e
    WHERE e.user_id    = incoming_users.user_id
      AND e.valid_from = incoming_users.valid_from
      AND e.is_current = true
);
```

### fact_impressions

```sql
CREATE OR REPLACE TABLE kimball.fact_impressions (
    impression_id   STRING    NOT NULL,
    date_key        INT       NOT NULL,
    campaign_key    BIGINT    NOT NULL,
    user_key        BIGINT    NOT NULL,
    impression_ts   TIMESTAMP NOT NULL,
    bid_price_usd   DECIMAL(10,6),
    is_clicked      BOOLEAN,
    is_converted    BOOLEAN,
    event_date      DATE      NOT NULL
)
USING DELTA
PARTITIONED BY (event_date)
CLUSTER BY (campaign_key, user_key);

-- Load fact — join to dims to resolve surrogate keys at ingestion time
INSERT INTO kimball.fact_impressions
SELECT
    s.impression_id,
    CAST(date_format(s.impression_ts, 'yyyyMMdd') AS INT) AS date_key,
    c.surrogate_key  AS campaign_key,
    u.surrogate_key  AS user_key,
    s.impression_ts,
    s.bid_price_usd,
    s.is_clicked,
    s.is_converted,
    CAST(s.impression_ts AS DATE) AS event_date
FROM staging_impressions s
JOIN kimball.dim_campaign c
  ON  c.campaign_id = s.campaign_id
JOIN kimball.dim_user u
  ON  u.user_id    = s.user_id
  AND u.valid_from <= s.impression_ts
  AND (u.valid_to  > s.impression_ts OR u.valid_to IS NULL);
```

### Key analytical queries

```sql
-- CTR by segment — historically correct
SELECT
    u.segment,
    COUNT(*)                                           AS impressions,
    SUM(CASE WHEN f.is_clicked   THEN 1 ELSE 0 END)   AS clicks,
    ROUND(SUM(CASE WHEN f.is_clicked THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS ctr_pct
FROM kimball.fact_impressions f
JOIN kimball.dim_user     u ON f.user_key     = u.surrogate_key
JOIN kimball.dim_campaign c ON f.campaign_key = c.surrogate_key
GROUP BY u.segment
ORDER BY ctr_pct DESC;

-- Point-in-time: what segment did U001 have at each impression?
SELECT f.impression_id, f.impression_ts, u.segment, u.valid_from, u.valid_to
FROM kimball.fact_impressions f
JOIN kimball.dim_user u ON f.user_key = u.surrogate_key
WHERE u.user_id = 'U001'
ORDER BY f.impression_ts;
```

---

*Next topics: PySpark transforms · Data Contracts with ODCS*

{% snapshot snap_dim_user %}

{{
    config(
        target_schema = 'gold',
        target_database = 'dbt_sandbox',
        unique_key = 'user_id',          
        strategy = 'check',              
        check_cols = ['segment', 'country', 'device_type'],
        invalidate_hard_deletes = True  
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

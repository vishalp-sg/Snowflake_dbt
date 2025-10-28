{{ config(
    materialized='incremental',
    unique_key='device_id'
) }}

SELECT
    device_id,
    MAX(ts) AS last_seen,
    AVG(oil_temp_c) AS avg_temp_1h,  -- simplified, use time filter if needed
    COUNT_IF(alert_severity = 'CRITICAL') AS faults_count_24h
FROM {{ source('core', 'new_telemetry_events') }}
GROUP BY device_id

{% if is_incremental() %}
  AND ts > (SELECT COALESCE(MAX(last_seen), '1900-01-01') FROM {{ this }})
{% endif %}

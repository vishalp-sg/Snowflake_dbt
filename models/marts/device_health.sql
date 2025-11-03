{{ config(
    materialized = 'incremental'
) }}

WITH src AS (
    SELECT
        device_id,
        ts,
        oil_temp_c,
        alert_severity
    FROM {{ source('core', 'new_telemetry_events') }}
    {% if is_incremental() %}
        WHERE ts > (
            SELECT COALESCE(MAX(last_seen), '1900-01-01')
            FROM {{ this }}
        )
    {% endif %}
)

SELECT
    device_id,
    MAX(ts) AS last_seen,
    AVG(oil_temp_c) AS avg_temp_1h,
    COUNT_IF(alert_severity = 'CRITICAL') AS faults_count_24h
FROM src
GROUP BY device_id

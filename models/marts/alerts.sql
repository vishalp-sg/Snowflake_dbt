{{ config(
    materialized = 'incremental'
) }}

WITH src AS (
    SELECT
        e.device_id,
        e.ts,
        e.alert_code,
        e.alert_severity AS severity,
        FALSE AS resolved_flag
    FROM {{ source('core', 'new_telemetry_events') }} AS e
    WHERE e.alert_code IS NOT NULL
    {% if is_incremental() %}
      AND e.ts > (SELECT COALESCE(MAX(ts), '1900-01-01') FROM {{ this }})
    {% endif %}
)

SELECT
    device_id,
    ts,
    alert_code,
    severity,
    resolved_flag
FROM src

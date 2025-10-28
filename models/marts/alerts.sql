{{ config(
    materialized='incremental',
    unique_key='device_id || ts'
) }}

SELECT
    device_id,
    ts,
    alert_code,
    alert_severity AS severity,
    FALSE AS resolved_flag
FROM {{ source('core', 'new_telemetry_events') }}
WHERE alert_code IS NOT NULL

{% if is_incremental() %}
  AND ts > (SELECT COALESCE(MAX(ts), '1900-01-01') FROM {{ this }})
{% endif %}


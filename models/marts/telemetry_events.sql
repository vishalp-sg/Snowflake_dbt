{{ config(
    materialized='incremental',
    unique_key='device_id || ts'
) }}

SELECT
    device_id,
    ts,
    lat,
    lon,
    speed,
    rpm,
    oil_temp_c,
    fuel_level_pct,
    operational_hours,
    firmware_version AS firmware,
    event_type
FROM {{ source('core', 'new_telemetry_events') }}

WHERE ts IS NOT NULL

{% if is_incremental() %}
  AND ts > (SELECT COALESCE(MAX(ts), '1900-01-01') FROM {{ this }})
{% endif %}

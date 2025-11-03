{{ config(
    materialized = 'incremental'
) }}

WITH src AS (
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
    {% if is_incremental() %}
      WHERE ts > (SELECT COALESCE(MAX(ts), '1900-01-01') FROM {{ this }})
    {% endif %}
)

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
    firmware,
    event_type
FROM src

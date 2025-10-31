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
    FROM tractor_db.core.new_telemetry_events n
    WHERE n.ts IS NOT NULL
      AND n.ts > (
          SELECT COALESCE(MAX(t.ts), '1900-01-01') 
          FROM tractor_db.tractor_schema_core.telemetry_events AS t
      )
)
SELECT * FROM src

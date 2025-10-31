WITH src AS (
    SELECT
        e.device_id,
        e.ts,
        e.alert_code,
        e.alert_severity AS severity,
        FALSE AS resolved_flag
    FROM tractor_db.core.new_telemetry_events AS e
    WHERE e.alert_code IS NOT NULL
      AND e.ts > (
          SELECT COALESCE(MAX(t.ts), '1900-01-01')
          FROM tractor_db.tractor_schema_core.alerts AS t
      )
)

SELECT *
FROM src

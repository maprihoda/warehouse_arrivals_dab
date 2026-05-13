CREATE OR REPLACE MATERIALIZED VIEW warehouse_arrivals_gold AS
SELECT
  g.device_id,
  g.timestamp,
  w.warehouse_name
FROM raw_gps_silver AS g
JOIN warehouse_geofences_gold AS w
  ON ST_Contains(w.boundary_geom, g.point_geom);

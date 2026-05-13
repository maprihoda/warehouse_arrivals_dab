CREATE OR REPLACE MATERIALIZED VIEW warehouse_geofences_gold AS
SELECT
  warehouse_name,
  ST_GeomFromWKT(boundary_wkt) AS boundary_geom
FROM geofences_bronze;

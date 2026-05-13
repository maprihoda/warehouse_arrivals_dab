CREATE OR REFRESH STREAMING TABLE raw_gps_silver
COMMENT "GPS pings with native geometry points for geometry joins";

CREATE FLOW raw_gps_silver_flow AS
INSERT INTO raw_gps_silver BY NAME
SELECT
  device_id,
  timestamp,
  longitude,
  latitude,
  ST_Point(longitude, latitude) AS point_geom
FROM STREAM(gps_bronze);

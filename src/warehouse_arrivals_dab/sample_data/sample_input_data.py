# Databricks notebook source
"""Load sample raw warehouse-arrivals input data for the demo pipeline.

This Databricks Python source file is executed by the bundle's workflow job on
`dev` before the Lakeflow pipeline runs. It prepares the managed volume inputs
that the pipeline reads by:

- writing one fresh batch of synthetic GPS events under `/gps`
- creating the demo warehouse geofences under `/geofences` when missing
- optionally clearing existing GPS and/or geofence data via reset parameters

Even though this file is stored as a normal `.py` source file inside `src/`,
Databricks executes it as a notebook task from the bundle definition.
"""

from datetime import datetime, timezone
from uuid import uuid4

from databricks.sdk.runtime import dbutils, spark
from pyspark.sql import functions as F


def get_required_param(name: str) -> str:
    """Declare the Databricks widget, then read and validate its required value."""
    dbutils.widgets.text(name, "")
    value = dbutils.widgets.get(name).strip()
    if not value:
        raise ValueError(f"Missing required parameter: {name}")
    return value


def get_optional_bool_param(name: str, default: bool = False) -> bool:
    """Declare an optional widget, using default when no job value is passed, and parse it as a bool."""
    default_value = "true" if default else "false"
    dbutils.widgets.text(name, default_value)
    raw_value = dbutils.widgets.get(name).strip().lower()
    return raw_value in {"1", "true", "t", "yes", "y"}


def path_has_entries(path: str) -> bool:
    """Return True when the volume path exists and contains at least one entry."""
    try:
        return len(dbutils.fs.ls(path)) > 0
    except Exception:
        return False


def delete_path_if_requested(path: str, should_delete: bool, label: str) -> None:
    """Delete a path recursively when the corresponding reset flag is enabled."""
    if not should_delete:
        return

    if path_has_entries(path):
        dbutils.fs.rm(path, True)
        print(f"Reset {label} data at {path}")
    else:
        print(f"No existing {label} data to reset at {path}")


def new_gps_batch_path(gps_root: str) -> str:
    """Create a unique batch directory so each sample-data run produces a new drop."""
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{gps_root}/run_id={run_id}_{uuid4().hex[:8]}"


catalog = get_required_param("catalog")
schema = get_required_param("schema")
volume = get_required_param("volume")
reset_all = get_optional_bool_param("reset_all")
reset_gps = reset_all or get_optional_bool_param("reset_gps")
reset_geofences = reset_all or get_optional_bool_param("reset_geofences")

volume_base = f"/Volumes/{catalog}/{schema}/{volume}"
gps_root = f"{volume_base}/gps"
geofences_path = f"{volume_base}/geofences"
gps_batch_path = new_gps_batch_path(gps_root)

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

delete_path_if_requested(gps_root, reset_gps, "GPS")
delete_path_if_requested(geofences_path, reset_geofences, "geofence")

# GPS: 5000 rows in a box that overlaps both warehouse geofences.
df_gps = (
    spark.range(0, 5000)
    .repartition(10)
    .select(
        F.format_string("device_%d", F.col("id").cast("long")).alias("device_id"),
        F.current_timestamp().alias("timestamp"),
        (-118.3 + F.rand() * 0.2).alias("longitude"),
        (34.0 + F.rand() * 0.2).alias("latitude"),
    )
)

df_gps.write.format("json").mode("overwrite").save(gps_batch_path)
print(f"Wrote 5000 GPS rows to new batch path {gps_batch_path}")

# Geofences: two warehouse polygons in WKT.
geofences_data = [
    (
        "Warehouse_A",
        "POLYGON ((-118.35 34.02, -118.25 34.02, -118.25 34.08, -118.35 34.08, -118.35 34.02))",
    ),
    (
        "Warehouse_B",
        "POLYGON ((-118.20 34.05, -118.12 34.05, -118.12 34.12, -118.20 34.12, -118.20 34.05))",
    ),
]

if path_has_entries(geofences_path):
    print(f"Preserved existing geofences at {geofences_path}")
else:
    df_geo = spark.createDataFrame(geofences_data, ["warehouse_name", "boundary_wkt"])
    df_geo.write.format("json").mode("overwrite").save(geofences_path)
    print(f"Wrote {len(geofences_data)} geofences to {geofences_path}")

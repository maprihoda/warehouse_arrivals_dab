from pyspark import pipelines as dp


CATALOG_CONF = "warehouse_arrivals.catalog"
SCHEMA_CONF = "warehouse_arrivals.schema"
VOLUME_CONF = "warehouse_arrivals.volume"


def _volume_path(child: str) -> str:
    catalog = spark.conf.get(CATALOG_CONF)
    schema = spark.conf.get(SCHEMA_CONF)
    volume = spark.conf.get(VOLUME_CONF)
    return f"/Volumes/{catalog}/{schema}/{volume}/{child}"


@dp.table(
    name="gps_bronze",
    comment="Raw GPS pings ingested from the managed raw_data volume using Auto Loader",
)
def gps_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(_volume_path("gps"))
    )


@dp.materialized_view(
    name="geofences_bronze",
    comment="Raw warehouse geofences loaded from the managed raw_data volume",
)
def geofences_bronze():
    return spark.read.format("json").load(_volume_path("geofences"))

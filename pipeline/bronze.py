"""Bronze: three source-faithful Parquet streams discovered by Auto Loader."""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


CATALOG = spark.conf.get("martech.catalog", "nethra")
BRONZE_SCHEMA = spark.conf.get("martech.bronze_schema", "martech_bronze")
LANDING_PATH = spark.conf.get(
    "martech.landing_path", f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/landing"
)


def _name(table: str) -> str:
    return f"{CATALOG}.{BRONZE_SCHEMA}.{table}"


PROFILE_SCHEMA = T.StructType(
    [
        T.StructField("client_id", T.StringType()),
        T.StructField("source_system", T.StringType()),
        T.StructField("operation", T.StringType()),
        T.StructField("operation_timestamp", T.StringType()),
        T.StructField("profile_change_type", T.StringType()),
        T.StructField("lifecycle_stage", T.StringType()),
        T.StructField("account_type", T.StringType()),
        T.StructField("aum_tier", T.StringType()),
        T.StructField("retirement_horizon", T.StringType()),
        T.StructField("advice_relationship", T.StringType()),
        T.StructField("email_consent", T.StringType()),
        T.StructField("personalization_consent", T.StringType()),
        T.StructField("adobe_segment", T.StringType()),
        T.StructField("synthetic_record", T.BooleanType()),
    ]
)

CAMPAIGN_SCHEMA = T.StructType(
    [
        T.StructField("campaign_id", T.StringType()),
        T.StructField("campaign_name", T.StringType()),
        T.StructField("source_system", T.StringType()),
        T.StructField("theme", T.StringType()),
        T.StructField("objective", T.StringType()),
        T.StructField("primary_channel", T.StringType()),
        T.StructField("start_date", T.StringType()),
        T.StructField("end_date", T.StringType()),
        T.StructField("target_audience", T.StringType()),
        T.StructField("content_topic", T.StringType()),
        T.StructField("content_format", T.StringType()),
        T.StructField("status", T.StringType()),
        T.StructField("synthetic_record", T.BooleanType()),
    ]
)

ENGAGEMENT_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType()),
        T.StructField("interaction_id", T.StringType()),
        T.StructField("sequence_number", T.LongType()),
        T.StructField("event_timestamp", T.StringType()),
        T.StructField("source_system", T.StringType()),
        T.StructField("channel", T.StringType()),
        T.StructField("event_type", T.StringType()),
        T.StructField("campaign_id", T.StringType()),
        T.StructField("client_id", T.StringType()),
        T.StructField("engagement_seconds", T.LongType()),
        T.StructField("synthetic_record", T.BooleanType()),
    ]
)


def _autoload(folder: str, source_label: str, schema: T.StructType) -> DataFrame:
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.includeExistingFiles", "true")
        .schema(schema)
        .load(f"{LANDING_PATH}/{folder}")
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_source_system", F.lit(source_label))
    )


BRONZE_PROPERTIES = {
    "quality": "bronze",
    "demo.synthetic": "true",
    "pipelines.autoOptimize.managed": "true",
}


@dp.table(
    name=_name("client_profiles_raw"),
    comment="Raw synthetic M360-style investor profile changes from Parquet files.",
    table_properties=BRONZE_PROPERTIES,
)
def client_profiles_raw() -> DataFrame:
    return _autoload("client_profiles", "Synthetic M360 profile export", PROFILE_SCHEMA)


@dp.table(
    name=_name("campaigns_raw"),
    comment="Raw synthetic campaign-planning records from Parquet files.",
    table_properties=BRONZE_PROPERTIES,
)
def campaigns_raw() -> DataFrame:
    return _autoload("campaigns", "Synthetic campaign planning export", CAMPAIGN_SCHEMA)


@dp.table(
    name=_name("engagement_events_raw"),
    comment="Raw synthetic email, web, and webinar engagement from Parquet files.",
    table_properties=BRONZE_PROPERTIES,
)
def engagement_events_raw() -> DataFrame:
    return _autoload(
        "engagement_events", "Synthetic owned-channel engagement export", ENGAGEMENT_SCHEMA
    )

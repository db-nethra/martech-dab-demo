"""Silver: typed clean streams, source-specific quarantines, and profile SCD2."""

from __future__ import annotations

from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


CATALOG = spark.conf.get("martech.catalog", "nethra")
BRONZE_SCHEMA = spark.conf.get("martech.bronze_schema", "martech_bronze")
SILVER_SCHEMA = spark.conf.get("martech.silver_schema", "martech_silver")


def _name(schema: str, table: str) -> str:
    return f"{CATALOG}.{schema}.{table}"


# One rule dictionary per source is reused by its clean stream and exact
# null-safe inverse quarantine. A record cannot disappear between the paths.
PROFILE_EXPECTATIONS = {
    "profile_has_client_id": "client_id RLIKE '^CL-[0-9]{6}$'",
    "profile_has_valid_operation": "operation IN ('INSERT', 'UPDATE', 'DELETE')",
    "profile_has_valid_sequence": "operation_timestamp IS NOT NULL",
    "profile_has_valid_lifecycle": (
        "operation = 'DELETE' OR lifecycle_stage IN ('Prospect', 'Active', 'At Risk', 'Inactive')"
    ),
    "profile_has_valid_consent": (
        "operation = 'DELETE' OR (email_consent IN ('granted', 'denied') "
        "AND personalization_consent IN ('granted', 'denied'))"
    ),
    "profile_has_no_rescued_data": (
        "coalesce(cast(_rescued_data AS STRING), '') IN ('', '{}')"
    ),
}

CAMPAIGN_EXPECTATIONS = {
    "campaign_has_id": "campaign_id RLIKE '^CMP-[0-9]{4}$'",
    "campaign_has_name": "campaign_name IS NOT NULL",
    "campaign_has_valid_dates": "start_date IS NOT NULL AND end_date >= start_date",
    "campaign_has_valid_channel": "primary_channel IN ('EMAIL', 'WEB', 'WEBINAR')",
    "campaign_has_valid_status": "status IN ('PLANNED', 'ACTIVE', 'COMPLETED')",
    "campaign_has_no_rescued_data": (
        "coalesce(cast(_rescued_data AS STRING), '') IN ('', '{}')"
    ),
}

ENGAGEMENT_EXPECTATIONS = {
    "engagement_has_event_id": "event_id RLIKE '^EVT-[0-9]{8}$'",
    "engagement_has_timestamp": "event_timestamp IS NOT NULL",
    "engagement_has_client": "client_id RLIKE '^CL-[0-9]{6}$'",
    "engagement_has_campaign": "campaign_id RLIKE '^CMP-[0-9]{4}$'",
    "engagement_has_valid_channel_event": """
        (channel = 'EMAIL' AND event_type IN
          ('SENT', 'DELIVERED', 'OPENED', 'CLICKED', 'BOUNCED', 'UNSUBSCRIBED'))
        OR (channel = 'WEB' AND event_type IN
          ('PAGE_VIEW', 'CONTENT_VIEW', 'CTA_CLICK'))
        OR (channel = 'WEBINAR' AND event_type IN
          ('REGISTERED', 'ATTENDED', 'WATCH_COMPLETED'))
    """,
    "engagement_has_nonnegative_duration": "coalesce(engagement_seconds, 0) >= 0",
    "engagement_has_no_rescued_data": (
        "coalesce(cast(_rescued_data AS STRING), '') IN ('', '{}')"
    ),
}


def _profile_candidates(streaming: bool) -> DataFrame:
    reader = spark.readStream if streaming else spark.read
    return reader.table(_name(BRONZE_SCHEMA, "client_profiles_raw")).withColumn(
        "operation_timestamp", F.to_timestamp("operation_timestamp")
    )


def _campaign_candidates(streaming: bool) -> DataFrame:
    reader = spark.readStream if streaming else spark.read
    return (
        reader.table(_name(BRONZE_SCHEMA, "campaigns_raw"))
        .withColumn("start_date", F.to_date("start_date"))
        .withColumn("end_date", F.to_date("end_date"))
    )


def _engagement_candidates(streaming: bool) -> DataFrame:
    reader = spark.readStream if streaming else spark.read
    return reader.table(_name(BRONZE_SCHEMA, "engagement_events_raw")).withColumn(
        "event_timestamp", F.to_timestamp("event_timestamp")
    )


def _rule_passes(expression: str) -> F.Column:
    return F.coalesce(F.expr(expression), F.lit(False))


def _all_rules_pass(expectations: dict[str, str]) -> F.Column:
    return reduce(lambda left, right: left & right, map(_rule_passes, expectations.values()))


def _failed_rules(expectations: dict[str, str]) -> F.Column:
    failures = [
        F.when(~_rule_passes(expression), F.lit(name))
        for name, expression in expectations.items()
    ]
    return F.concat_ws(", ", F.array(*failures))


def _raw_payload(candidates: DataFrame) -> F.Column:
    source_columns = [column for column in candidates.columns if not column.startswith("_")]
    return F.to_json(F.struct(*[F.col(column) for column in source_columns]))


SILVER_PROPERTIES = {"quality": "silver", "demo.synthetic": "true"}
QUARANTINE_PROPERTIES = {
    **SILVER_PROPERTIES,
    "quality.status": "quarantine",
}


@dp.table(
    name=_name(SILVER_SCHEMA, "client_profile_changes"),
    comment="Valid profile changes; retains source operation for the SCD2 demo.",
    table_properties=SILVER_PROPERTIES,
)
@dp.expect_all_or_drop(PROFILE_EXPECTATIONS)
def client_profile_changes() -> DataFrame:
    return _profile_candidates(streaming=True).dropDuplicates(
        ["client_id", "operation_timestamp", "operation"]
    )


@dp.table(
    name=_name(SILVER_SCHEMA, "client_profiles_quarantine"),
    comment="Invalid profile changes with source-specific failed expectations.",
    table_properties=QUARANTINE_PROPERTIES,
)
def client_profiles_quarantine() -> DataFrame:
    candidates = _profile_candidates(streaming=False)
    return candidates.filter(~_all_rules_pass(PROFILE_EXPECTATIONS)).select(
        "client_id",
        "operation",
        "operation_timestamp",
        "profile_change_type",
        "lifecycle_stage",
        "email_consent",
        "personalization_consent",
        _failed_rules(PROFILE_EXPECTATIONS).alias("failed_expectations"),
        _raw_payload(candidates).alias("raw_payload"),
        "_source_file",
        "_ingest_timestamp",
    )


dp.create_streaming_table(
    name=_name(SILVER_SCHEMA, "client_profile_history"),
    comment="Investor profile history maintained with AUTO CDC SCD Type 2.",
    table_properties={**SILVER_PROPERTIES, "delta.enableChangeDataFeed": "true"},
)

dp.create_auto_cdc_flow(
    target=_name(SILVER_SCHEMA, "client_profile_history"),
    source=_name(SILVER_SCHEMA, "client_profile_changes"),
    keys=["client_id"],
    sequence_by=F.col("operation_timestamp"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=[
        "operation",
        "_rescued_data",
        "_ingest_timestamp",
        "_source_file",
        "_source_system",
    ],
    stored_as_scd_type=2,
)


@dp.table(
    name=_name(SILVER_SCHEMA, "campaigns_clean"),
    comment="Valid, typed, deduplicated campaign planning records.",
    table_properties=SILVER_PROPERTIES,
)
@dp.expect_all_or_drop(CAMPAIGN_EXPECTATIONS)
def campaigns_clean() -> DataFrame:
    return _campaign_candidates(streaming=True).dropDuplicates(["campaign_id"])


@dp.table(
    name=_name(SILVER_SCHEMA, "campaigns_quarantine"),
    comment="Invalid campaign planning records with failed expectations.",
    table_properties=QUARANTINE_PROPERTIES,
)
def campaigns_quarantine() -> DataFrame:
    candidates = _campaign_candidates(streaming=False)
    return candidates.filter(~_all_rules_pass(CAMPAIGN_EXPECTATIONS)).select(
        "campaign_id",
        "campaign_name",
        "start_date",
        "end_date",
        "primary_channel",
        "status",
        _failed_rules(CAMPAIGN_EXPECTATIONS).alias("failed_expectations"),
        _raw_payload(candidates).alias("raw_payload"),
        "_source_file",
        "_ingest_timestamp",
    )


@dp.table(
    name=_name(SILVER_SCHEMA, "engagement_events_clean"),
    comment="Valid, typed, deduplicated owned-channel engagement events.",
    table_properties=SILVER_PROPERTIES,
)
@dp.expect_all_or_drop(ENGAGEMENT_EXPECTATIONS)
def engagement_events_clean() -> DataFrame:
    return _engagement_candidates(streaming=True).dropDuplicates(["event_id"])


@dp.table(
    name=_name(SILVER_SCHEMA, "engagement_events_quarantine"),
    comment="Invalid engagement events with source-specific failed expectations.",
    table_properties=QUARANTINE_PROPERTIES,
)
def engagement_events_quarantine() -> DataFrame:
    candidates = _engagement_candidates(streaming=False)
    return candidates.filter(~_all_rules_pass(ENGAGEMENT_EXPECTATIONS)).select(
        "event_id",
        "interaction_id",
        "event_timestamp",
        "source_system",
        "channel",
        "event_type",
        "campaign_id",
        "client_id",
        "engagement_seconds",
        _failed_rules(ENGAGEMENT_EXPECTATIONS).alias("failed_expectations"),
        _raw_payload(candidates).alias("raw_payload"),
        "_source_file",
        "_ingest_timestamp",
    )

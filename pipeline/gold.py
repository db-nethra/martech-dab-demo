"""Gold: a minimal marketing star at daily client-version-campaign-channel grain."""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


CATALOG = spark.conf.get("martech.catalog", "nethra")
SILVER_SCHEMA = spark.conf.get("martech.silver_schema", "martech_silver")
GOLD_SCHEMA = spark.conf.get("martech.gold_schema", "martech_gold")


def _name(schema: str, table: str) -> str:
    return f"{CATALOG}.{schema}.{table}"


GOLD_PROPERTIES = {
    "quality": "gold",
    "demo.synthetic": "true",
    "contains_direct_identifiers": "false",
}


def _client_key(client_id: Column, valid_from: Column) -> Column:
    """Create a privacy-safe surrogate for one effective-dated profile version."""
    return F.sha2(
        F.concat_ws(
            "||",
            client_id,
            F.date_format(valid_from, "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"),
        ),
        256,
    )


@dp.table(
    name=_name(GOLD_SCHEMA, "dim_client"),
    comment="Privacy-safe SCD2 investor profile versions used for historical attribution.",
    table_properties=GOLD_PROPERTIES,
)
def dim_client() -> DataFrame:
    history = spark.read.table(_name(SILVER_SCHEMA, "client_profile_history"))
    return history.select(
        _client_key(F.col("client_id"), F.col("__START_AT")).alias("client_key"),
        F.col("__START_AT").alias("valid_from_timestamp"),
        F.col("__END_AT").alias("valid_to_timestamp"),
        F.col("__END_AT").isNull().alias("is_current_profile"),
        "profile_change_type",
        "lifecycle_stage",
        "account_type",
        "aum_tier",
        "retirement_horizon",
        "advice_relationship",
        "email_consent",
        "personalization_consent",
        "adobe_segment",
        F.lit(True).alias("synthetic_record"),
    )


@dp.table(
    name=_name(GOLD_SCHEMA, "dim_campaign"),
    comment="Campaign taxonomy, audience, objective, dates, and primary content attributes.",
    table_properties=GOLD_PROPERTIES,
)
def dim_campaign() -> DataFrame:
    return spark.read.table(_name(SILVER_SCHEMA, "campaigns_clean")).select(
        F.sha2("campaign_id", 256).alias("campaign_key"),
        "campaign_id",
        "campaign_name",
        "theme",
        "objective",
        "primary_channel",
        "start_date",
        "end_date",
        "target_audience",
        "content_topic",
        "content_format",
        "status",
        F.lit(True).alias("synthetic_record"),
    )


@dp.table(
    name=_name(GOLD_SCHEMA, "fact_engagement_daily"),
    comment=(
        "Daily owned-channel engagement at activity date, client, campaign, and channel grain."
    ),
    table_properties=GOLD_PROPERTIES,
)
def fact_engagement_daily() -> DataFrame:
    events = spark.read.table(_name(SILVER_SCHEMA, "engagement_events_clean")).alias("event")
    client_versions = spark.read.table(
        _name(SILVER_SCHEMA, "client_profile_history")
    ).alias("profile")
    clean_campaigns = (
        spark.read.table(_name(SILVER_SCHEMA, "campaigns_clean"))
        .select("campaign_id")
        .dropDuplicates()
        .alias("campaign")
    )
    effective_profile = (
        (F.col("event.client_id") == F.col("profile.client_id"))
        & (F.col("event.event_timestamp") >= F.col("profile.__START_AT"))
        & (
            F.col("profile.__END_AT").isNull()
            | (F.col("event.event_timestamp") < F.col("profile.__END_AT"))
        )
    )
    resolved = events.join(client_versions, effective_profile, "inner").join(
        clean_campaigns,
        F.col("event.campaign_id") == F.col("campaign.campaign_id"),
        "inner",
    )
    return (
        resolved.groupBy(
            F.to_date("event.event_timestamp").alias("activity_date"),
            _client_key(
                F.col("profile.client_id"), F.col("profile.__START_AT")
            ).alias("client_key"),
            F.sha2(F.col("event.campaign_id"), 256).alias("campaign_key"),
            F.col("event.channel").alias("channel"),
        )
        .agg(
            F.countDistinct("event.event_id").alias("event_count"),
            F.sum(F.when(F.col("event.event_type") == "SENT", 1).otherwise(0)).alias(
                "sent_count"
            ),
            F.sum(F.when(F.col("event.event_type") == "DELIVERED", 1).otherwise(0)).alias(
                "delivered_count"
            ),
            F.sum(F.when(F.col("event.event_type") == "OPENED", 1).otherwise(0)).alias(
                "opened_count"
            ),
            F.sum(F.when(F.col("event.event_type") == "CLICKED", 1).otherwise(0)).alias(
                "clicked_count"
            ),
            F.sum(F.when(F.col("event.event_type") == "BOUNCED", 1).otherwise(0)).alias(
                "bounced_count"
            ),
            F.sum(
                F.when(F.col("event.event_type") == "UNSUBSCRIBED", 1).otherwise(0)
            ).alias(
                "unsubscribed_count"
            ),
            F.sum(
                F.when(
                    F.col("event.event_type").isin("PAGE_VIEW", "CONTENT_VIEW"), 1
                ).otherwise(0)
            ).alias("web_view_count"),
            F.sum(F.when(F.col("event.event_type") == "CTA_CLICK", 1).otherwise(0)).alias(
                "cta_click_count"
            ),
            F.sum(F.when(F.col("event.event_type") == "REGISTERED", 1).otherwise(0)).alias(
                "webinar_registered_count"
            ),
            F.sum(F.when(F.col("event.event_type") == "ATTENDED", 1).otherwise(0)).alias(
                "webinar_attended_count"
            ),
            F.sum(
                F.when(F.col("event.event_type") == "WATCH_COMPLETED", 1).otherwise(0)
            ).alias(
                "webinar_completed_count"
            ),
            F.sum(F.coalesce(F.col("event.engagement_seconds"), F.lit(0))).alias(
                "engagement_seconds"
            ),
        )
        .withColumn("synthetic_record", F.lit(True))
    )

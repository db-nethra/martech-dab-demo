# Databricks notebook source
# MAGIC %md
# MAGIC # Validate the promoted MarTech data product
# MAGIC
# MAGIC This notebook is deployed by the bundle and runs inside each target.
# MAGIC It proves that the same code produced the expected PII-free, synthetic
# MAGIC Bronze/Silver/Gold contract without copying development records to UAT.

# COMMAND ----------

import json

dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("catalog", "nethra")
dbutils.widgets.text("bronze_schema", "martech_cicd_bronze")
dbutils.widgets.text("silver_schema", "martech_cicd_silver")
dbutils.widgets.text("gold_schema", "martech_cicd_gold")

environment = dbutils.widgets.get("environment")
catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")


def table_count(schema: str, table: str) -> int:
    return spark.table(f"`{catalog}`.`{schema}`.`{table}`").count()


actual = {
    "client_profiles_raw": table_count(bronze_schema, "client_profiles_raw"),
    "campaigns_raw": table_count(bronze_schema, "campaigns_raw"),
    "engagement_events_raw": table_count(bronze_schema, "engagement_events_raw"),
    "client_profiles_quarantine": table_count(silver_schema, "client_profiles_quarantine"),
    "campaigns_quarantine": table_count(silver_schema, "campaigns_quarantine"),
    "engagement_events_quarantine": table_count(silver_schema, "engagement_events_quarantine"),
    "dim_client": table_count(gold_schema, "dim_client"),
    "dim_campaign": table_count(gold_schema, "dim_campaign"),
    "fact_engagement_daily": table_count(gold_schema, "fact_engagement_daily"),
}

expected = {
    "client_profiles_quarantine": 11,
    "campaigns_quarantine": 6,
    "engagement_events_quarantine": 25,
    "dim_client": 620,
    "dim_campaign": 15,
    "fact_engagement_daily": 2966,
}

failures = {
    name: {"expected": expected_count, "actual": actual[name]}
    for name, expected_count in expected.items()
    if actual[name] != expected_count
}
for bronze_table in ("client_profiles_raw", "campaigns_raw", "engagement_events_raw"):
    if actual[bronze_table] <= 0:
        failures[bronze_table] = {"expected": "> 0", "actual": actual[bronze_table]}

gold_identifier_leaks = {}
for table in ("dim_client", "fact_engagement_daily"):
    columns = {field.name.lower() for field in spark.table(f"`{catalog}`.`{gold_schema}`.`{table}`").schema}
    leaked = sorted(columns & {"client_id", "email", "email_address", "phone", "account_number"})
    if leaked:
        gold_identifier_leaks[table] = leaked

if failures or gold_identifier_leaks:
    raise AssertionError(
        json.dumps(
            {"environment": environment, "count_failures": failures, "identifier_leaks": gold_identifier_leaks},
            indent=2,
            sort_keys=True,
        )
    )

evidence = {
    "environment": environment,
    "catalog": catalog,
    "schemas": {"bronze": bronze_schema, "silver": silver_schema, "gold": gold_schema},
    "counts": actual,
    "gold_direct_identifier_check": "PASS",
    "synthetic_data_only": True,
    "status": "PASS",
}
print(json.dumps(evidence, indent=2, sort_keys=True))

# COMMAND ----------

display(spark.createDataFrame([(name, count) for name, count in actual.items()], ["asset", "row_count"]))

dbutils.notebook.exit(json.dumps(evidence, sort_keys=True))

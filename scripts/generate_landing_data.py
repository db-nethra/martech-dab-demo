#!/usr/bin/env python3
"""Generate three deterministic synthetic MarTech Parquet feeds.

The same files support two workshop stories:

1. query existing S3 Parquet in place through an external Volume; and
2. incrementally materialize the feeds with Auto Loader.

No names, email addresses, phone numbers, account numbers, or real customer
records are generated.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl


START = datetime(2026, 1, 1, tzinfo=UTC)
END = datetime(2026, 6, 30, 23, 59, tzinfo=UTC)


@dataclass(frozen=True)
class Counts:
    clients: int = 500
    profile_updates: int = 120
    campaigns: int = 15
    email_interactions: int = 900
    web_interactions: int = 1_200
    webinar_interactions: int = 500


THEMES = [
    "Retirement Readiness",
    "IRA Rollover",
    "Advice",
    "529 College Savings",
    "Market Volatility Education",
    "Estate Planning",
]

AUDIENCES = [
    "Pre-Retiree",
    "Rollover Explorer",
    "Advice Seeker",
    "College Saver",
    "Active Investor",
    "Retiree",
]


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _is_uc_volume_path(path: Path) -> bool:
    return len(path.parts) > 1 and path.parts[0] == "/" and path.parts[1] == "Volumes"


def _clear_output_directory(output: Path) -> None:
    if not output.exists():
        return
    if not _is_uc_volume_path(output):
        shutil.rmtree(output)
        return
    for child in output.iterdir():
        if child.is_dir() and not child.is_symlink():
            shutil.rmtree(child)
        else:
            child.unlink()


def _write_parquet_batches(df: pl.DataFrame, directory: Path, batches: int) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    indexes = np.array_split(np.arange(df.height), batches)
    for number, batch_indexes in enumerate(indexes, 1):
        df[batch_indexes.tolist()].write_parquet(directory / f"batch_{number:03d}.parquet")


def _profile_rows(
    rng: np.random.Generator, counts: Counts
) -> tuple[pl.DataFrame, list[str], dict[str, dict[str, str]]]:
    client_ids = [f"CL-{number:06d}" for number in range(1, counts.clients + 1)]
    current: dict[str, dict[str, str]] = {}
    rows: list[dict[str, Any]] = []
    for index, client_id in enumerate(client_ids):
        audience_index = index % len(AUDIENCES)
        lifecycle = "Prospect" if audience_index in {1, 2} else "Active"
        profile = {
            "lifecycle_stage": lifecycle,
            "account_type": str(rng.choice(["IRA", "Brokerage", "Workplace retirement", "529"])),
            "aum_tier": str(rng.choice(["Emerging", "Established", "High Value"])),
            "retirement_horizon": str(
                rng.choice(["0-5 years", "6-10 years", "11-20 years", "20+ years"])
            ),
            "advice_relationship": str(
                rng.choice(["Self-Directed", "Digital Advice", "Human Advice"])
            ),
            "email_consent": "granted" if rng.random() < 0.88 else "denied",
            "personalization_consent": "granted" if rng.random() < 0.80 else "denied",
            "adobe_segment": AUDIENCES[audience_index],
        }
        current[client_id] = profile.copy()
        rows.append(
            {
                "client_id": client_id,
                "source_system": "Synthetic M360 profile export",
                "operation": "INSERT",
                "operation_timestamp": _iso(START + timedelta(minutes=index)),
                "profile_change_type": "INITIAL_PROFILE",
                **profile,
                "synthetic_record": True,
            }
        )

    update_ids = rng.choice(client_ids, counts.profile_updates, replace=False)
    for offset, raw_client_id in enumerate(update_ids):
        client_id = str(raw_client_id)
        profile = current[client_id].copy()
        if offset % 2 == 0:
            profile["lifecycle_stage"] = {
                "Prospect": "Active",
                "Active": "At Risk",
                "At Risk": "Active",
            }[profile["lifecycle_stage"]]
            change_type = "LIFECYCLE_CHANGE"
        else:
            profile["advice_relationship"] = {
                "Self-Directed": "Digital Advice",
                "Digital Advice": "Human Advice",
                "Human Advice": "Self-Directed",
            }[profile["advice_relationship"]]
            change_type = "ADVICE_RELATIONSHIP_CHANGE"
        current[client_id] = profile.copy()
        rows.append(
            {
                "client_id": client_id,
                "source_system": "Synthetic M360 profile export",
                "operation": "UPDATE",
                "operation_timestamp": _iso(
                    datetime(2026, 4, 1, tzinfo=UTC) + timedelta(minutes=offset)
                ),
                "profile_change_type": change_type,
                **profile,
                "synthetic_record": True,
            }
        )

    defects = [
        {**rows[0], "client_id": None, "profile_change_type": "DQ_NULL_KEY"},
        {
            **rows[1],
            "client_id": "CL-900002",
            "operation": "UPSERT",
            "profile_change_type": "DQ_BAD_OPERATION",
        },
        {
            **rows[2],
            "client_id": "CL-900003",
            "operation_timestamp": "not-a-timestamp",
            "profile_change_type": "DQ_BAD_TIMESTAMP",
        },
        {
            **rows[3],
            "client_id": "CL-900004",
            "lifecycle_stage": "Unknown",
            "profile_change_type": "DQ_BAD_LIFECYCLE",
        },
        {
            **rows[4],
            "client_id": "CL-900005",
            "email_consent": "pending",
            "profile_change_type": "DQ_BAD_CONSENT",
        },
        {
            **rows[5],
            "client_id": "CL-900006",
            "personalization_consent": "unknown",
            "profile_change_type": "DQ_BAD_PERSONALIZATION_CONSENT",
        },
        {**rows[6], "client_id": "CLIENT-7", "profile_change_type": "DQ_MALFORMED_KEY"},
        {
            **rows[7],
            "client_id": "CL-900008",
            "operation_timestamp": None,
            "profile_change_type": "DQ_NULL_TIMESTAMP",
        },
        {
            **rows[8],
            "client_id": "CL-900009",
            "lifecycle_stage": "Dormant",
            "profile_change_type": "DQ_BAD_STAGE_2",
        },
        {
            **rows[9],
            "client_id": "CL-900010",
            "email_consent": "revoked",
            "personalization_consent": "pending",
            "profile_change_type": "DQ_BAD_CONSENT_COMBINATION",
        },
    ]
    return pl.DataFrame(rows + defects), client_ids, current


def _campaign_rows(counts: Counts) -> tuple[pl.DataFrame, list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    for index in range(counts.campaigns):
        start = date(2026, 1, 6) + timedelta(days=index * 10)
        end = start + timedelta(days=45)
        theme = THEMES[index % len(THEMES)]
        channel = ["EMAIL", "WEB", "WEBINAR"][index % 3]
        rows.append(
            {
                "campaign_id": f"CMP-{index + 1:04d}",
                "campaign_name": f"{theme} {['Education', 'Next Step'][index % 2]}",
                "source_system": "Synthetic Workfront and campaign taxonomy export",
                "theme": theme,
                "objective": ["Educate", "Deepen engagement", "Prompt next step"][index % 3],
                "primary_channel": channel,
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "target_audience": AUDIENCES[index % len(AUDIENCES)],
                "content_topic": theme,
                "content_format": ["Article", "Calculator", "Checklist", "Webinar"][index % 4],
                "status": "COMPLETED" if end < date(2026, 5, 1) else "ACTIVE",
                "synthetic_record": True,
            }
        )
    defects = [
        {**rows[0], "campaign_id": None, "campaign_name": "DQ null campaign"},
        {
            **rows[1],
            "campaign_id": "CMP-9002",
            "end_date": "2025-01-01",
            "campaign_name": "DQ invalid dates",
        },
        {
            **rows[2],
            "campaign_id": "CMP-9003",
            "primary_channel": "FAX",
            "campaign_name": "DQ invalid channel",
        },
        {**rows[3], "campaign_id": "CMP-9004", "campaign_name": None},
        {
            **rows[4],
            "campaign_id": "CMP-9005",
            "status": "ARCHIVED",
            "campaign_name": "DQ invalid status",
        },
    ]
    return pl.DataFrame(rows + defects), rows


def _engagement_rows(
    rng: np.random.Generator,
    counts: Counts,
    client_ids: list[str],
    campaigns: list[dict[str, Any]],
) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []

    def add(
        interaction_id: str,
        sequence_number: int,
        timestamp: datetime,
        source_system: str,
        channel: str,
        event_type: str,
        campaign_id: str,
        client_id: str,
        engagement_seconds: int = 0,
    ) -> None:
        rows.append(
            {
                "event_id": f"EVT-{len(rows) + 1:08d}",
                "interaction_id": interaction_id,
                "sequence_number": sequence_number,
                "event_timestamp": _iso(timestamp),
                "source_system": source_system,
                "channel": channel,
                "event_type": event_type,
                "campaign_id": campaign_id,
                "client_id": client_id,
                "engagement_seconds": engagement_seconds,
                "synthetic_record": True,
            }
        )

    for number in range(counts.email_interactions):
        client_id = str(rng.choice(client_ids))
        campaign = campaigns[number % len(campaigns)]
        base = START + timedelta(days=int(rng.integers(1, 176)), minutes=number)
        interaction_id = f"MSG-{number + 1:06d}"
        source = "Marketo" if number % 2 == 0 else "Adobe Journey Optimizer"
        add(interaction_id, 1, base, source, "EMAIL", "SENT", campaign["campaign_id"], client_id)
        if rng.random() < 0.96:
            add(interaction_id, 2, base + timedelta(minutes=1), source, "EMAIL", "DELIVERED", campaign["campaign_id"], client_id)
            if rng.random() < 0.62:
                add(interaction_id, 3, base + timedelta(hours=2), source, "EMAIL", "OPENED", campaign["campaign_id"], client_id, 20)
                if rng.random() < 0.31:
                    add(interaction_id, 4, base + timedelta(hours=3), source, "EMAIL", "CLICKED", campaign["campaign_id"], client_id, 45)
        else:
            add(interaction_id, 2, base + timedelta(minutes=2), source, "EMAIL", "BOUNCED", campaign["campaign_id"], client_id)
        if rng.random() < 0.015:
            add(interaction_id, 5, base + timedelta(days=1), source, "EMAIL", "UNSUBSCRIBED", campaign["campaign_id"], client_id)

    for number in range(counts.web_interactions):
        client_id = str(rng.choice(client_ids))
        campaign = campaigns[number % len(campaigns)]
        timestamp = START + timedelta(days=int(rng.integers(1, 181)), seconds=number * 11)
        event_type = str(rng.choice(["PAGE_VIEW", "CONTENT_VIEW", "CTA_CLICK"], p=[0.55, 0.35, 0.10]))
        add(
            f"WEB-{number // 3 + 1:06d}",
            number % 3 + 1,
            timestamp,
            "Adobe Analytics",
            "WEB",
            event_type,
            campaign["campaign_id"],
            client_id,
            int(rng.integers(5, 180)),
        )

    for number in range(counts.webinar_interactions):
        client_id = str(rng.choice(client_ids))
        campaign = campaigns[number % len(campaigns)]
        timestamp = START + timedelta(days=int(rng.integers(1, 171)), minutes=number * 2)
        interaction_id = f"WEBINAR-{number + 1:06d}"
        source = ["ON24", "Kaltura", "Cvent"][number % 3]
        add(interaction_id, 1, timestamp, source, "WEBINAR", "REGISTERED", campaign["campaign_id"], client_id)
        if rng.random() < 0.72:
            add(interaction_id, 2, timestamp + timedelta(days=3), source, "WEBINAR", "ATTENDED", campaign["campaign_id"], client_id, 1_800)
            if rng.random() < 0.55:
                add(interaction_id, 3, timestamp + timedelta(days=3, minutes=40), source, "WEBINAR", "WATCH_COMPLETED", campaign["campaign_id"], client_id, 2_400)

    defects: list[dict[str, Any]] = []
    for repeat in range(4):
        offset = repeat * 6
        defects.extend(
            [
                {
                    **rows[offset],
                    "event_id": None,
                    "interaction_id": f"DQ-NULL-EVENT-{repeat + 1}",
                },
                {
                    **rows[offset + 1],
                    "event_id": f"EVT-{99_000_000 + offset + 2:08d}",
                    "event_timestamp": "not-a-timestamp",
                    "interaction_id": f"DQ-BAD-TIME-{repeat + 1}",
                },
                {
                    **rows[offset + 2],
                    "event_id": f"EVT-{99_000_000 + offset + 3:08d}",
                    "client_id": "UNKNOWN",
                    "interaction_id": f"DQ-BAD-CLIENT-{repeat + 1}",
                },
                {
                    **rows[offset + 3],
                    "event_id": f"EVT-{99_000_000 + offset + 4:08d}",
                    "campaign_id": "MISSING",
                    "interaction_id": f"DQ-BAD-CAMPAIGN-{repeat + 1}",
                },
                {
                    **rows[offset + 4],
                    "event_id": f"EVT-{99_000_000 + offset + 5:08d}",
                    "channel": "SOCIAL",
                    "interaction_id": f"DQ-BAD-CHANNEL-{repeat + 1}",
                },
                {
                    **rows[offset + 5],
                    "event_id": f"EVT-{99_000_000 + offset + 6:08d}",
                    "engagement_seconds": -30,
                    "interaction_id": f"DQ-BAD-DURATION-{repeat + 1}",
                },
            ]
        )
    return pl.DataFrame(rows + defects)


def _write_drift_batch(df: pl.DataFrame, directory: Path, prefix: str) -> None:
    drift = df.head(1).with_columns(
        pl.lit(f"synthetic schema drift from {prefix}").alias("unexpected_source_field")
    )
    # Give the drift sample its own business key so it cannot compete with a
    # legitimate row during Silver deduplication before the rescue rule fires.
    if prefix == "client_profiles":
        drift = drift.with_columns(
            pl.lit("CL-999991").alias("client_id"),
            pl.lit("DQ_SCHEMA_DRIFT").alias("profile_change_type"),
        )
    elif prefix == "campaigns":
        drift = drift.with_columns(pl.lit("CMP-9991").alias("campaign_id"))
    elif prefix == "engagement_events":
        drift = drift.with_columns(pl.lit("EVT-99999991").alias("event_id"))
    drift.write_parquet(directory / "batch_999_schema_drift.parquet")


def generate(output: str | Path, seed: int = 42, clean: bool = False) -> dict[str, int]:
    output = Path(output)
    if clean:
        _clear_output_directory(output)
    output.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(seed)
    counts = Counts()

    profiles, client_ids, _ = _profile_rows(rng, counts)
    campaigns, campaign_rows = _campaign_rows(counts)
    engagement = _engagement_rows(rng, counts, client_ids, campaign_rows)

    datasets = {
        "client_profiles": profiles,
        "campaigns": campaigns,
        "engagement_events": engagement,
    }
    batch_counts = {"client_profiles": 4, "campaigns": 3, "engagement_events": 6}
    for name, frame in datasets.items():
        directory = output / name
        _write_parquet_batches(frame, directory, batch_counts[name])
        _write_drift_batch(frame, directory, name)

    manifest_counts = {name: frame.height + 1 for name, frame in datasets.items()}
    manifest = {
        "seed": seed,
        "synthetic": True,
        "format": "parquet",
        "counts": manifest_counts,
        "expected_quarantine_counts": {
            "client_profiles": 11,
            "campaigns": 6,
            "engagement_events": 25,
        },
        "sha256_contract": hashlib.sha256(
            json.dumps(manifest_counts, sort_keys=True).encode()
        ).hexdigest(),
        "note": "Synthetic educational data; not actual customer data or performance.",
    }
    (output / "_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest_counts


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--clean", action="store_true")
    args = parser.parse_args()
    counts = generate(args.output, seed=args.seed, clean=args.clean)
    print(json.dumps(counts, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

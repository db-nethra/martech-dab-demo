from __future__ import annotations

import hashlib
import json
from pathlib import Path

import polars as pl

from generate_landing_data import _clear_output_directory, generate


def _digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(candidate for candidate in root.rglob("*") if candidate.is_file()):
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _read(root: Path, source: str) -> pl.DataFrame:
    return pl.read_parquet(root / source / "batch_0*.parquet", glob=True)


def test_generation_is_deterministic_and_has_only_three_parquet_sources(tmp_path: Path) -> None:
    first, second = tmp_path / "first", tmp_path / "second"
    first_counts = generate(first)
    second_counts = generate(second)

    assert first_counts == second_counts
    assert set(first_counts) == {"client_profiles", "campaigns", "engagement_events"}
    assert _digest(first) == _digest(second)
    assert all(len(list((first / source).glob("*.parquet"))) >= 4 for source in first_counts)

    manifest = json.loads((first / "_manifest.json").read_text())
    assert manifest["seed"] == 42
    assert manifest["synthetic"] is True
    assert manifest["format"] == "parquet"
    assert manifest["counts"] == first_counts
    assert manifest["expected_quarantine_counts"] == {
        "client_profiles": 11,
        "campaigns": 6,
        "engagement_events": 25,
    }


def test_profiles_are_mutable_entities_with_controlled_bad_records(tmp_path: Path) -> None:
    generate(tmp_path)
    profiles = _read(tmp_path, "client_profiles")

    assert {"INSERT", "UPDATE", "UPSERT"} == set(profiles["operation"].drop_nulls())
    valid_inserts = profiles.filter(
        pl.col("client_id").str.contains(r"^CL-[0-9]{6}$")
        & (pl.col("operation") == "INSERT")
        & (pl.col("profile_change_type") == "INITIAL_PROFILE")
    )
    assert valid_inserts["client_id"].n_unique() == 500
    assert profiles.filter(pl.col("operation") == "UPDATE").height >= 120
    assert profiles.filter(pl.col("operation") == "DELETE").is_empty()
    assert profiles["client_id"].null_count() > 0
    assert "not-a-timestamp" in set(profiles["operation_timestamp"])
    assert profiles.filter(pl.col("profile_change_type").str.starts_with("DQ_")).height == 10
    drift = pl.read_parquet(tmp_path / "client_profiles" / "batch_999_schema_drift.parquet")
    assert "unexpected_source_field" in drift.columns


def test_campaign_and_engagement_sources_are_coherent_and_pii_free(tmp_path: Path) -> None:
    generate(tmp_path)
    campaigns = _read(tmp_path, "campaigns")
    events = _read(tmp_path, "engagement_events")

    valid_campaigns = set(
        campaigns.filter(pl.col("campaign_id").str.contains(r"^CMP-[0-9]{4}$"))["campaign_id"]
    )
    valid_events = events.filter(
        pl.col("event_id").str.contains(r"^EVT-[0-9]{8}$")
        & pl.col("client_id").str.contains(r"^CL-[0-9]{6}$")
        & pl.col("campaign_id").str.contains(r"^CMP-[0-9]{4}$")
        & pl.col("channel").is_in(["EMAIL", "WEB", "WEBINAR"])
        & (pl.col("engagement_seconds") >= 0)
    )
    assert set(valid_events["campaign_id"]) <= valid_campaigns
    assert {"EMAIL", "WEB", "WEBINAR"} <= set(valid_events["channel"])
    assert {"Marketo", "Adobe Journey Optimizer", "Adobe Analytics", "ON24", "Kaltura", "Cvent"} <= set(
        valid_events["source_system"]
    )
    assert events.filter(pl.col("engagement_seconds") < 0).height > 0
    assert events.filter(pl.col("campaign_id") == "MISSING").height > 0
    assert campaigns.height - 15 == 5
    assert events.filter(pl.col("event_id").is_null()).height == 4
    assert events.filter(pl.col("event_timestamp") == "not-a-timestamp").height == 4
    assert events.filter(pl.col("client_id") == "UNKNOWN").height == 4
    assert events.filter(pl.col("campaign_id") == "MISSING").height == 4
    assert events.filter(pl.col("channel") == "SOCIAL").height == 4
    assert events.filter(pl.col("engagement_seconds") < 0).height == 4

    forbidden = {"name", "email", "email_address", "phone", "account_number"}
    for frame in (campaigns, events, _read(tmp_path, "client_profiles")):
        assert forbidden.isdisjoint(frame.columns)


def test_clean_replaces_output_and_preserves_volume_root(tmp_path: Path, monkeypatch) -> None:
    output = tmp_path / "landing"
    generate(output)
    (output / "stale.txt").write_text("stale")
    generate(output, clean=True)
    assert not (output / "stale.txt").exists()

    monkeypatch.setattr(
        "generate_landing_data._is_uc_volume_path", lambda candidate: candidate == output
    )
    _clear_output_directory(output)
    assert output.is_dir()
    assert list(output.iterdir()) == []

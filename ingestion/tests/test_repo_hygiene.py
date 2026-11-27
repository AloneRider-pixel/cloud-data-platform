"""Repo hygiene: dbt models must not be swallowed by the model-weights ignore rule."""
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _ignored(path: str) -> bool:
    return subprocess.run(
        ["git", "check-ignore", "-q", path], cwd=ROOT
    ).returncode == 0


def test_dbt_models_are_not_gitignored():
    assert not _ignored("dbt/models/staging/stg_orders.sql")
    assert not _ignored("dbt/models/marts/mart_revenue.sql")


def test_root_model_weights_are_still_ignored():
    assert _ignored("models/weights.bin")

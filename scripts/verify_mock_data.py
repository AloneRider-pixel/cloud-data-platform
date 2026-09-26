"""Verify that the synthetic data benchmark is deterministic and traceable."""
from __future__ import annotations

import hashlib
import importlib
import json
import random

SEED = 20260927


def snapshot() -> tuple[str, dict[str, int]]:
    random.seed(SEED)
    module = importlib.import_module("scripts.generate_mock_data")
    payload = json.dumps(module.DATA_STORE, sort_keys=True, default=str).encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    counts = {name: len(rows) for name, rows in module.DATA_STORE.items()}
    return digest, counts


first_digest, first_counts = snapshot()
module = importlib.import_module("scripts.generate_mock_data")
importlib.reload(module)
second_digest, second_counts = snapshot()

if first_digest != second_digest or first_counts != second_counts:
    raise SystemExit("Synthetic data generator is not deterministic for the fixed benchmark seed.")

expected = {"/orders": 5000, "/products": 100, "/customers": 500, "/events": 2000}
if first_counts != expected:
    raise SystemExit(f"Unexpected fixture counts: {first_counts!r}; expected {expected!r}")

print(
    "synthetic dataset verified: "
    f"seed={SEED}, sha256={first_digest}, counts={first_counts}"
)

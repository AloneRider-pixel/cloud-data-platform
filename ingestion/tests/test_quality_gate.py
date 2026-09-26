import pytest

from src.main import enforce_quality_gate


def test_quality_gate_allows_valid_results() -> None:
    enforce_quality_gate([
        ("orders row count", True),
        ("orders primary-key uniqueness", True),
    ])


def test_quality_gate_blocks_invalid_results() -> None:
    with pytest.raises(RuntimeError, match="primary-key uniqueness"):
        enforce_quality_gate([
            ("orders row count", True),
            ("orders primary-key uniqueness", False),
        ])

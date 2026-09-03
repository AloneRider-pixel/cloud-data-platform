"""Tests for the ingestion layer."""
import pytest
import pandas as pd
from src.validators.schema_validator import SchemaValidator, DataQualityChecker
from src.extractors.csv_extractor import CSVExtractor
from src.config import Config


class TestSchemaValidator:
    def test_valid_schema(self):
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        validator = SchemaValidator()
        is_valid, errors = validator.validate(df, {"id": "integer", "name": "string"})
        assert is_valid
        assert len(errors) == 0

    def test_missing_columns(self):
        df = pd.DataFrame({"id": [1]})
        validator = SchemaValidator()
        is_valid, errors = validator.validate(df, {"id": "integer", "name": "string"})
        assert not is_valid
        assert any("Missing" in e for e in errors)


class TestDataQualityChecker:
    def test_not_null_pass(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        checker = DataQualityChecker()
        assert checker.check_not_null(df, ["a", "b"])

    def test_not_null_fail(self):
        df = pd.DataFrame({"a": [1, None, 3]})
        checker = DataQualityChecker()
        assert not checker.check_not_null(df, ["a"])

    def test_unique_pass(self):
        df = pd.DataFrame({"id": [1, 2, 3]})
        checker = DataQualityChecker()
        assert checker.check_unique(df, ["id"])

    def test_unique_fail(self):
        df = pd.DataFrame({"id": [1, 1, 3]})
        checker = DataQualityChecker()
        assert not checker.check_unique(df, ["id"])

    def test_accepted_values_pass(self):
        df = pd.DataFrame({"status": ["active", "inactive"]})
        checker = DataQualityChecker()
        assert checker.check_accepted_values(df, "status", ["active", "inactive", "pending"])

    def test_accepted_values_fail(self):
        df = pd.DataFrame({"status": ["active", "unknown"]})
        checker = DataQualityChecker()
        assert not checker.check_accepted_values(df, "status", ["active", "inactive"])

    def test_row_count(self):
        df = pd.DataFrame({"a": range(100)})
        checker = DataQualityChecker()
        assert checker.check_row_count(df, min_rows=50)
        assert not checker.check_row_count(df, min_rows=200)

    def test_range_check(self):
        df = pd.DataFrame({"amount": [10, 50, 100]})
        checker = DataQualityChecker()
        assert checker.check_range(df, "amount", min_value=0, max_value=200)
        assert not checker.check_range(df, "amount", min_value=50)


class TestConfig:
    def test_config_defaults(self):
        config = Config()
        assert config.batch_size == 10000
        assert config.max_retries == 3

    def test_database_url(self):
        config = Config()
        assert "postgresql://" in config.database_url

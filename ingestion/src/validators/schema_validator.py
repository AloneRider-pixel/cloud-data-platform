"""
Data Validation Framework.
Schema validation, data quality checks, and anomaly detection.
"""
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


class SchemaValidator:
    """Validates data against expected schemas."""

    def validate(
        self,
        df: pd.DataFrame,
        expected_schema: Dict[str, str],
        required_columns: Optional[List[str]] = None,
    ) -> Tuple[bool, List[str]]:
        """
        Validate DataFrame against expected schema.
        
        Returns:
            (is_valid, list_of_errors)
        """
        errors = []
        required_columns = required_columns or list(expected_schema.keys())

        # Check required columns exist
        missing = set(required_columns) - set(df.columns)
        if missing:
            errors.append(f"Missing required columns: {missing}")

        # Check column types
        for col, expected_type in expected_schema.items():
            if col not in df.columns:
                continue

            actual_type = str(df[col].dtype)
            if not self._types_compatible(actual_type, expected_type):
                errors.append(
                    f"Column '{col}': expected type '{expected_type}', "
                    f"got '{actual_type}'"
                )

        is_valid = len(errors) == 0
        if not is_valid:
            logger.warning(f"Schema validation failed: {errors}")
        else:
            logger.info(f"Schema validation passed for {len(df.columns)} columns")

        return is_valid, errors

    def _types_compatible(self, actual: str, expected: str) -> bool:
        """Check if actual type is compatible with expected."""
        type_groups = {
            "string": ["object", "string", "str", "category"],
            "integer": ["int64", "int32", "Int64", "Int32"],
            "float": ["float64", "float32", "Float64"],
            "numeric": ["int64", "int32", "float64", "float32", "Int64", "Float64"],
            "boolean": ["bool", "boolean"],
            "datetime": ["datetime64[ns]", "datetime64"],
        }

        if expected in type_groups:
            return actual in type_groups[expected]
        return actual == expected


class DataQualityChecker:
    """Runs data quality checks on DataFrames."""

    def __init__(self):
        self.results = []

    def check_not_null(
        self,
        df: pd.DataFrame,
        columns: List[str],
    ) -> bool:
        """Check that specified columns have no null values."""
        null_counts = df[columns].isnull().sum()
        has_nulls = null_counts[null_counts > 0]

        passed = len(has_nulls) == 0
        self.results.append({
            "check": "not_null",
            "columns": columns,
            "passed": passed,
            "null_counts": has_nulls.to_dict() if not passed else {},
        })

        if not passed:
            logger.warning(f"NOT NULL check failed: {has_nulls.to_dict()}")
        return passed

    def check_unique(
        self,
        df: pd.DataFrame,
        columns: List[str],
    ) -> bool:
        """Check that specified columns have unique values."""
        passed = True
        duplicates = {}

        for col in columns:
            dup_count = df[col].duplicated().sum()
            if dup_count > 0:
                passed = False
                duplicates[col] = int(dup_count)

        self.results.append({
            "check": "unique",
            "columns": columns,
            "passed": passed,
            "duplicates": duplicates,
        })

        if not passed:
            logger.warning(f"UNIQUE check failed: {duplicates}")
        return passed

    def check_accepted_values(
        self,
        df: pd.DataFrame,
        column: str,
        accepted_values: List[Any],
    ) -> bool:
        """Check that a column only contains accepted values."""
        actual_values = set(df[column].unique())
        invalid = actual_values - set(accepted_values)

        passed = len(invalid) == 0
        self.results.append({
            "check": "accepted_values",
            "column": column,
            "passed": passed,
            "invalid_values": list(invalid),
            "accepted": accepted_values,
        })

        if not passed:
            logger.warning(
                f"ACCEPTED VALUES check failed for {column}: "
                f"invalid={invalid}"
            )
        return passed

    def check_range(
        self,
        df: pd.DataFrame,
        column: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
    ) -> bool:
        """Check that numeric column values are within range."""
        col_data = pd.to_numeric(df[column], errors="coerce")
        
        passed = True
        violations = {}

        if min_value is not None:
            below_min = (col_data < min_value).sum()
            if below_min > 0:
                passed = False
                violations["below_min"] = int(below_min)

        if max_value is not None:
            above_max = (col_data > max_value).sum()
            if above_max > 0:
                passed = False
                violations["above_max"] = int(above_max)

        self.results.append({
            "check": "range",
            "column": column,
            "passed": passed,
            "min_value": min_value,
            "max_value": max_value,
            "violations": violations,
        })

        return passed

    def check_row_count(
        self,
        df: pd.DataFrame,
        min_rows: int = 0,
        max_rows: Optional[int] = None,
    ) -> bool:
        """Check that row count is within expected bounds."""
        row_count = len(df)
        passed = row_count >= min_rows

        if max_rows is not None:
            passed = passed and row_count <= max_rows

        self.results.append({
            "check": "row_count",
            "passed": passed,
            "row_count": row_count,
            "min_expected": min_rows,
            "max_expected": max_rows,
        })

        return passed

    def check_referential_integrity(
        self,
        df: pd.DataFrame,
        foreign_key_column: str,
        reference_df: pd.DataFrame,
        reference_key_column: str,
    ) -> bool:
        """Check that all foreign key values exist in the reference table."""
        fk_values = set(df[foreign_key_column].unique())
        ref_values = set(reference_df[reference_key_column].unique())

        orphans = fk_values - ref_values
        passed = len(orphans) == 0

        self.results.append({
            "check": "referential_integrity",
            "passed": passed,
            "foreign_key": foreign_key_column,
            "reference_key": reference_key_column,
            "orphan_count": len(orphans),
        })

        if not passed:
            logger.warning(
                f"Referential integrity failed: {len(orphans)} orphan values"
            )
        return passed

    def get_summary(self) -> Dict:
        """Get summary of all quality checks."""
        total = len(self.results)
        passed = sum(1 for r in self.results if r["passed"])
        failed = total - passed

        return {
            "total_checks": total,
            "passed": passed,
            "failed": failed,
            "pass_rate": passed / total if total > 0 else 1.0,
            "results": self.results,
            "timestamp": datetime.utcnow().isoformat(),
        }

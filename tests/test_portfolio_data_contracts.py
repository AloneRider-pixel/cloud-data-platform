from portfolio_hardening.data_contracts import compare_contracts, fingerprint


def test_additive_change_is_compatible() -> None:
    previous = {"id": {"type": "integer", "required": True, "nullable": False}}
    current = {
        **previous,
        "email": {"type": "string", "required": False, "nullable": True},
    }
    diff = compare_contracts(previous, current)
    assert diff.breaking == ()
    assert [change.field for change in diff.additive] == ["email"]


def test_required_field_removal_is_breaking() -> None:
    previous = {"id": {"type": "integer", "required": True, "nullable": False}}
    diff = compare_contracts(previous, {})
    assert diff.breaking[0].kind == "removed_required"


def test_type_and_nullability_drift_are_breaking() -> None:
    previous = {"email": {"type": "string", "required": False, "nullable": True}}
    current = {"email": {"type": "integer", "required": False, "nullable": False}}
    diff = compare_contracts(previous, current)
    assert {change.kind for change in diff.breaking} == {
        "type_change",
        "nullability_tightened",
    }


def test_fingerprint_is_deterministic() -> None:
    schema_a = {"b": {"type": "string"}, "a": {"type": "integer"}}
    schema_b = {"a": {"type": "integer"}, "b": {"type": "string"}}
    assert fingerprint(schema_a) == fingerprint(schema_b)

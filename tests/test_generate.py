from anomaly_lab import generate


def test_same_seed_same_rows():
    a = generate.generate_rows(devices=3, days=3, seed=42)
    assert a == generate.generate_rows(devices=3, days=3, seed=42)
    assert a != generate.generate_rows(devices=3, days=3, seed=43)


def test_all_anomaly_types_present():
    rows = generate.generate_rows(devices=3, days=3, seed=42)
    types = {r[7] for r in rows}
    assert set(generate.TYPES) <= types
    # is_injected is 1 exactly when the type is not "normal"
    assert all((r[6] == 1) == (r[7] != "normal") for r in rows)


def test_reference_sensor_is_constant():
    rows = generate.generate_rows(devices=3, days=3, seed=42)
    ref = [r for r in rows if r[0] == "dev03"]
    assert len(ref) == 72
    assert {r[4] for r in ref} == {60.0}
    assert {r[5] for r in ref} == {1.2}
    assert all(r[6] == 0 for r in ref)


def test_bad_arguments_explain_the_fix():
    import pytest

    with pytest.raises(ValueError, match="devices must be >= 2"):
        generate.generate_rows(devices=1)
    with pytest.raises(ValueError, match="days must be >= 3"):
        generate.generate_rows(days=2)


def test_dataframe_matches_rows(spark):
    df = generate.generate(spark, devices=3, days=3, seed=42)
    assert df.count() == 3 * 72
    assert df.filter("is_injected = 1").count() > 0

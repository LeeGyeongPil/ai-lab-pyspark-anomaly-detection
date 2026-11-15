import pytest
from pyspark.sql import functions as F

from anomaly_lab import evaluate


def test_metrics_hand_counted(spark):
    # (is_injected, anomaly_type, event_id, t, score, flag)
    rows = [
        (0, "normal", -1, 0, 0.1, 0),
        (0, "normal", -1, 1, 0.2, 0),
        (0, "normal", -1, 2, 0.3, 0),
        (0, "normal", -1, 3, 4.0, 1),  # false alarm
        (0, "normal", -1, 4, 0.1, 0),
        (0, "normal", -1, 5, None, 0),  # undecidable: no score
        (1, "point_strong", 1, 6, 5.0, 1),
        (1, "point_strong", 2, 7, 1.0, 0),
        (1, "sustained", 3, 8, 1.0, 0),
        (1, "sustained", 3, 9, 4.5, 1),  # one alarm inside the event: event detected
        (1, "combo", 4, 10, 1.0, 0),
    ]
    df = spark.createDataFrame(
        rows,
        "is_injected int, anomaly_type string, event_id int, t int, score double, flag int",
    )
    m = evaluate.evaluate_flags(df, {"x": (F.col("flag") == 1, F.col("score"))})["x"]
    assert m["undecidable"] == 1
    assert m["negatives"] == 5  # 6 normal rows minus the undecidable one
    assert (m["tp"], m["fp"], m["positives"]) == (2, 1, 5)
    assert m["recall"] == pytest.approx(2 / 5)
    assert m["fpr"] == pytest.approx(1 / 5)
    assert m["precision"] == pytest.approx(2 / 3)
    assert m["by_type"]["point_strong"] == pytest.approx(1 / 2)
    assert m["by_type"]["sustained"] == pytest.approx(1.0)  # graded per event
    assert m["by_type"]["combo"] == 0.0
    assert m["mean_delay"] == pytest.approx(1.0)  # event starts at t=8, first alarm at t=9


def test_threshold_curve_monotonic(scored):
    curve = evaluate.threshold_curve(scored, days=3)
    for prev, cur in zip(curve, curve[1:]):
        assert cur["threshold"] > prev["threshold"]
        assert cur["alerts"] <= prev["alerts"]
        assert cur["recall"] <= prev["recall"] + 1e-12
        assert cur["recall_point"] <= prev["recall_point"] + 1e-12


def test_weak_anomalies_bend_curve(spark):
    # default data (6 devices x 14 days): even at the highest threshold weak points are missed
    from anomaly_lab import generate, residual

    df = residual.add_residual(generate.generate(spark, 6, 14, 42))
    curve = evaluate.threshold_curve(df, days=14)
    assert curve[-1]["recall_point"] < 1.0
    assert curve[0]["recall_point"] > curve[-1]["recall_point"]


def test_curve_csv_roundtrip(tmp_path):
    curve = [{"threshold": 2.0, "alerts": 3, "recall": None}]
    path = tmp_path / "sub" / "c.csv"
    evaluate.write_curve_csv(curve, path)
    assert path.read_text().splitlines() == ["threshold,alerts,recall", "2.0,3,"]

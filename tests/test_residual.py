from anomaly_lab import evaluate
from pyspark.sql import functions as F


def test_residual_beats_raw_iqr(scored):
    graded = evaluate.evaluate_flags(
        scored,
        {
            "raw_iqr": (F.col("iqr_flag") == 1, None),
            "robust_z": (F.col("rz") > 3.5, F.col("rz")),
        },
    )
    point_iqr = graded["raw_iqr"]["by_type"]["point"]
    point_rz = graded["robust_z"]["by_type"]["point"]
    assert point_rz > point_iqr


def test_reference_sensor_rz_is_null(scored):
    ref = scored.filter("device = 'dev03'")
    assert ref.filter(F.col("rz").isNotNull()).count() == 0

import pytest
from pyspark.sql import functions as F

from anomaly_lab import baseline, evaluate


def _tiny(spark, values, device="a"):
    rows = [(device, i, float(v)) for i, v in enumerate(values)]
    return spark.createDataFrame(rows, "device string, t int, temp double")


def test_plain_divide_raises_under_ansi(spark):
    assert spark.conf.get("spark.sql.ansi.enabled") == "true"  # Spark 4 default
    df = _tiny(spark, [5, 5, 5]).withColumn("sd", F.lit(0.0))
    with pytest.raises(Exception, match="DIVIDE_BY_ZERO"):
        df.withColumn("z", F.col("temp") / F.col("sd")).collect()


def test_try_divide_yields_null_and_undecidable_count(spark):
    # one constant sensor (spread 0) and one normal sensor
    rows = [("flat", i, 7.0) for i in range(30)] + [("wavy", i, float(i % 5)) for i in range(30)]
    df = spark.createDataFrame(rows, "device string, t int, temp double")
    out = baseline.add_moving_z(df)
    flat = out.filter("device = 'flat'").collect()
    assert all(r["z"] is None for r in flat)  # NULL, not a crash
    # undecidable = 30 flat rows + 2 warm-up rows of the wavy device (no history, 1 value)
    assert evaluate.count_undecidable(out, "z") == 32


def test_window_excludes_current_row(spark):
    out = baseline.add_moving_z(_tiny(spark, [10, 12, 11, 13, 100])).orderBy("t").collect()
    # row 4 (the spike) is compared with the mean of rows 0-3 only
    assert out[4]["mu"] == pytest.approx(11.5)
    assert out[0]["mu"] is None  # no history yet


def test_iqr_flag_marks_values_outside_the_fences(spark):
    values = [10] * 8 + [11] * 8 + [100]
    out = baseline.add_iqr(_tiny(spark, values)).orderBy("t").collect()
    assert out[-1]["iqr_flag"] == 1
    assert sum(r["iqr_flag"] for r in out) == 1

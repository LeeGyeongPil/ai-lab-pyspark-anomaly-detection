"""Step 2: two simple baselines on the raw temperature.

1. Moving z-score: how far is this hour from the previous 24 hours of the same device?
2. IQR fences (Tukey): how far outside the middle 50% of the device's values?

Both look at the raw value, so the daily temperature cycle confuses them.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

BASELINE_COLS = ["mu", "sd", "z", "q1", "q3", "iqr_flag"]


def add_moving_z(df: DataFrame, window: int = 24) -> DataFrame:
    """Add mu, sd, z using the previous `window` rows of the same device.

    rowsBetween(-window, -1) leaves the current row out. If it were included,
    a big spike would pull its own average up and hide itself.
    """
    w = Window.partitionBy("device").orderBy("t").rowsBetween(-window, -1)
    return (
        df.withColumn("mu", F.avg("temp").over(w))
        .withColumn("sd", F.stddev("temp").over(w))
        # Spark 4 runs in ANSI mode: a plain "/" by zero fails the whole job.
        # try_divide gives NULL instead; we later count NULL as "undecidable".
        .withColumn("z", F.try_divide(F.col("temp") - F.col("mu"), F.col("sd")))
    )


def add_iqr(df: DataFrame, k: float = 1.5) -> DataFrame:
    """Add q1, q3 and iqr_flag (1 if outside the Tukey fences) per device."""
    quartiles = df.groupBy("device").agg(
        F.percentile_approx("temp", [0.25, 0.75], 10000).alias("q")
    )
    quartiles = quartiles.select(
        "device", F.col("q")[0].alias("q1"), F.col("q")[1].alias("q3")
    )
    out = df.join(F.broadcast(quartiles), "device")
    spread = F.col("q3") - F.col("q1")
    outside = (F.col("temp") < F.col("q1") - k * spread) | (F.col("temp") > F.col("q3") + k * spread)
    return out.withColumn("iqr_flag", outside.cast("int"))


def add_baseline(df: DataFrame, window: int = 24) -> DataFrame:
    return add_iqr(add_moving_z(df, window))

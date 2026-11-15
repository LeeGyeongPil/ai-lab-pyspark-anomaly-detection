"""Step 3: remove the "usual" for each device and hour, then score what is left.

residual = value - (median of the same device at the same hour of day)
robust z = |residual| / (1.4826 * MAD)   (MAD = median absolute deviation)
Median and MAD are not pulled around by the anomalies themselves.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

RESIDUAL_COLS = ["temp_med", "vib_med", "resid", "vib_resid", "mad", "rz"]


def add_residual(df: DataFrame) -> DataFrame:
    """Add resid, vib_resid and rz (robust z of the temperature residual)."""
    usual = df.groupBy("device", "hour").agg(
        F.percentile_approx("temp", 0.5).alias("temp_med"),
        F.percentile_approx("vib", 0.5).alias("vib_med"),
    )
    out = (
        df.join(F.broadcast(usual), ["device", "hour"])
        .withColumn("resid", F.col("temp") - F.col("temp_med"))
        .withColumn("vib_resid", F.col("vib") - F.col("vib_med"))
    )
    mad = out.groupBy("device").agg(F.percentile_approx(F.abs("resid"), 0.5).alias("mad"))
    return out.join(F.broadcast(mad), "device").withColumn(
        # 1.4826 makes MAD comparable to a standard deviation.
        # A constant sensor has MAD 0 -> NULL ("undecidable") instead of a crash.
        "rz", F.try_divide(F.abs("resid"), F.lit(1.4826) * F.col("mad"))
    )

import math

from pyspark.sql import functions as F

from anomaly_lab import cluster, evaluate


def test_distance_uses_scaled_features(residual_df):
    df = cluster.add_kmeans(residual_df, keep_debug=True)
    rows = df.select("resid", "vib_resid", "scaled", "center", "km_dist").collect()
    for r in rows[:50]:
        expected = math.dist(r["scaled"], r["center"])
        assert math.isclose(r["km_dist"], expected, rel_tol=1e-9)
        # the raw (unscaled) distance would be very different: vibration is tiny, temperature is not
        raw = math.dist([r["resid"], r["vib_resid"]], r["center"])
        assert not math.isclose(r["km_dist"], raw, rel_tol=1e-3) or r["resid"] == 0
    # scaled columns have mean ~0 and std ~1
    stats = df.select(
        F.mean(F.col("scaled")[0]).alias("m"), F.stddev(F.col("scaled")[0]).alias("s")
    ).first()
    assert abs(stats["m"]) < 1e-6
    assert abs(stats["s"] - 1) < 1e-6


def test_cluster_catches_combo_missed_by_residual(scored):
    cut = evaluate.kmeans_cut(scored, top=0.05)
    caught = scored.filter(
        (F.col("anomaly_type") == "combo")
        & ~F.coalesce(F.col("rz") > 3.5, F.lit(False))
        & (F.col("km_dist") > cut)
    ).count()
    assert caught >= 1

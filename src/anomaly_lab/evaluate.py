"""Step 5: grade every detector with the planted answers.

recall    = planted anomalies that raised an alarm / planted anomalies
fpr       = normal rows that raised an alarm / normal rows
precision = planted anomalies among all alarms
Rows whose score is NULL are "undecidable": they never raise an alarm and are
left out of the fpr denominator, and we count them separately.
Sustained drifts are graded per event (one alarm anywhere in the event counts).
"""

from __future__ import annotations

import csv
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from anomaly_lab.baseline import BASELINE_COLS
from anomaly_lab.cluster import CLUSTER_COLS
from anomaly_lab.generate import COMBO, POINT_STRONG, POINT_WEAK, SUSTAINED
from anomaly_lab.residual import RESIDUAL_COLS

METHODS = ("moving_z", "raw_iqr", "robust_z", "kmeans")
ROW_TYPES = (POINT_STRONG, POINT_WEAK, COMBO)
THRESHOLDS = (2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0)


def _ratio(a: float, b: float) -> float | None:
    return None if not b else a / b


def infer_days(df: DataFrame) -> float:
    """Number of days covered by the data (one row per device-hour)."""
    return (df.agg(F.max("t")).first()[0] + 1) / 24


def evaluate_flags(df: DataFrame, flags: dict[str, tuple[Column, Column | None]]) -> dict[str, dict]:
    """Grade several alarm rules in one pass.

    flags: name -> (alarm condition, score column or None). A NULL condition counts as no alarm.
    """
    keys = {name: f"k{i}" for i, name in enumerate(flags)}  # plain column names (no dots)
    cols = [F.col(c) for c in ("is_injected", "anomaly_type", "event_id", "t")]
    for name, (cond, score) in flags.items():
        k = keys[name]
        cols.append(F.coalesce(cond, F.lit(False)).cast("int").alias(f"f_{k}"))
        null = score.isNull() if score is not None else F.lit(False)
        cols.append(null.cast("int").alias(f"u_{k}"))
    base = df.select(*cols)

    inj = F.col("is_injected")
    ty = F.col("anomaly_type")
    aggs = [F.sum(inj).alias("pos")]
    aggs += [F.sum((ty == x).cast("int")).alias(f"n_{x}") for x in ROW_TYPES]
    for k in keys.values():
        f, u = F.col(f"f_{k}"), F.col(f"u_{k}")
        aggs += [
            F.sum(f).alias(f"alerts_{k}"),
            F.sum(f * inj).alias(f"tp_{k}"),
            F.sum(f * (1 - inj)).alias(f"fp_{k}"),
            F.sum(u).alias(f"und_{k}"),
            F.sum((1 - inj) * (1 - u)).alias(f"neg_{k}"),
        ]
        aggs += [F.sum(f * (ty == x).cast("int")).alias(f"h_{k}_{x}") for x in ROW_TYPES]
    r = base.agg(*aggs).first().asDict()
    g = lambda key: r[key] or 0  # noqa: E731  (sum over empty data is NULL)

    ev_aggs = [F.min("t").alias("start")]
    ev_aggs += [F.max(f"f_{k}").alias(f"d_{k}") for k in keys.values()]
    ev_aggs += [F.min(F.when(F.col(f"f_{k}") == 1, F.col("t"))).alias(f"first_{k}") for k in keys.values()]
    events = base.filter(ty == SUSTAINED).groupBy("event_id").agg(*ev_aggs).collect()

    out: dict[str, dict] = {}
    for name, k in keys.items():
        n_pt = g(f"n_{POINT_STRONG}") + g(f"n_{POINT_WEAK}")
        h_pt = g(f"h_{k}_{POINT_STRONG}") + g(f"h_{k}_{POINT_WEAK}")
        detected = [e for e in events if e[f"d_{k}"]]
        delays = [e[f"first_{k}"] - e["start"] for e in detected]
        tp, fp = g(f"tp_{k}"), g(f"fp_{k}")
        out[name] = {
            "alerts": g(f"alerts_{k}"),
            "tp": tp,
            "fp": fp,
            "positives": g("pos"),
            "negatives": g(f"neg_{k}"),
            "undecidable": g(f"und_{k}"),
            "recall": _ratio(tp, g("pos")),
            "fpr": _ratio(fp, g(f"neg_{k}")),
            "precision": _ratio(tp, tp + fp),
            "events_total": len(events),
            "events_detected": len(detected),
            "mean_delay": sum(delays) / len(delays) if delays else None,
            "by_type": {
                "point": _ratio(h_pt, n_pt),
                POINT_STRONG: _ratio(g(f"h_{k}_{POINT_STRONG}"), g(f"n_{POINT_STRONG}")),
                POINT_WEAK: _ratio(g(f"h_{k}_{POINT_WEAK}"), g(f"n_{POINT_WEAK}")),
                SUSTAINED: _ratio(len(detected), len(events)),
                COMBO: _ratio(g(f"h_{k}_{COMBO}"), g(f"n_{COMBO}")),
            },
        }
    return out


def count_undecidable(df: DataFrame, score_col: str) -> int:
    return df.filter(F.col(score_col).isNull()).count()


def kmeans_cut(df: DataFrame, top: float = 0.03) -> float:
    """Distance above which a row is in the top `top` share of all distances."""
    return df.approxQuantile("km_dist", [1 - top], 0.001)[0]


def method_flags(
    z_threshold: float = 3.0, rz_threshold: float = 3.5, cut: float = 0.0
) -> dict[str, tuple[Column, Column | None]]:
    """The four detectors as alarm conditions."""
    return {
        "moving_z": (F.abs("z") > z_threshold, F.col("z")),
        "raw_iqr": (F.col("iqr_flag") == 1, None),
        "robust_z": (F.col("rz") > rz_threshold, F.col("rz")),
        "kmeans": (F.col("km_dist") > cut, F.col("km_dist")),
    }


def compare_methods(
    df: DataFrame, z_threshold: float = 3.0, rz_threshold: float = 3.5, kmeans_top: float = 0.03
) -> dict[str, dict]:
    cut = kmeans_cut(df, kmeans_top)
    return evaluate_flags(df, method_flags(z_threshold, rz_threshold, cut))


def threshold_curve(
    df: DataFrame, thresholds=THRESHOLDS, days: float = 1.0, score_col: str = "rz"
) -> list[dict]:
    """Alarm count and grades for each threshold on one score column."""
    flags = {str(t): (F.col(score_col) > t, F.col(score_col)) for t in thresholds}
    graded = evaluate_flags(df, flags)
    rows = []
    for t in thresholds:
        m = graded[str(t)]
        rows.append(
            {
                "threshold": t,
                "alerts": m["alerts"],
                "alerts_per_day": m["alerts"] / days,
                "recall": m["recall"],
                "fpr": m["fpr"],
                "precision": m["precision"],
                "recall_point": m["by_type"]["point"],
                "recall_sustained": m["by_type"][SUSTAINED],
                "recall_combo": m["by_type"][COMBO],
            }
        )
    return rows


def write_curve_csv(curve: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(curve[0]))
        writer.writeheader()
        for row in curve:
            writer.writerow({k: ("" if v is None else v) for k, v in row.items()})


def load_scored(spark: SparkSession, out: Path) -> DataFrame:
    """Join the saved outputs of steps 1-4 into one table keyed by (device, t)."""
    keys = ["device", "t"]
    df = spark.read.parquet(str(out / "data"))
    for folder, cols in (
        ("baseline", BASELINE_COLS),
        ("residual", RESIDUAL_COLS),
        ("cluster", CLUSTER_COLS),
    ):
        part = spark.read.parquet(str(out / folder)).select(*keys, *cols)
        df = df.join(part, keys)
    return df

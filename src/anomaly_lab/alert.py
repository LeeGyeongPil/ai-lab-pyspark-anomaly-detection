"""Step 6: turn scores into alerts people can live with.

1. Alert budget: "I can look at B alerts a day" -> lowest threshold that fits.
2. m-of-n rule: alert only if at least m of the last n hours are anomalous.
   It silences one-hour blips but lets a slow drift through at a *lower* threshold.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from anomaly_lab.evaluate import THRESHOLDS, evaluate_flags, threshold_curve


def pick_threshold(rows: list[dict], budget: float) -> dict:
    """Lowest threshold whose alerts_per_day <= budget.

    If none fits, return the highest threshold with within_budget False.
    """
    ordered = sorted(rows, key=lambda r: r["threshold"])
    for row in ordered:
        if row["alerts_per_day"] <= budget:
            return {**row, "within_budget": True}
    return {**ordered[-1], "within_budget": False}


def _window(n: int):
    return Window.partitionBy("device").orderBy("t").rowsBetween(-(n - 1), 0)


def rule_alert(score_col: str, threshold: float, n: int, m: int):
    """Alarm condition: at least m of the last n hours scored above threshold."""
    anomalous = F.coalesce(F.col(score_col) > threshold, F.lit(False)).cast("int")
    return F.sum(anomalous).over(_window(n)) >= m


def rule_alert_counts(df: DataFrame, thresholds, n: int, m: int, score_col: str = "rz") -> dict:
    """Alert count of the m-of-n rule for every threshold, in one pass."""
    cols = [rule_alert(score_col, t, n, m).cast("int").alias(f"a{i}") for i, t in enumerate(thresholds)]
    row = df.select(*cols).agg(*[F.sum(f"a{i}").alias(f"a{i}") for i in range(len(thresholds))]).first()
    return {t: row[f"a{i}"] or 0 for i, t in enumerate(thresholds)}


def evaluate_rule(
    df: DataFrame, threshold: float, n: int, m: int, days: float, score_col: str = "rz"
) -> dict:
    """Grade one threshold under an m-of-n rule (n=1, m=1 means no rule)."""
    cond = rule_alert(score_col, threshold, n, m)
    metrics = evaluate_flags(df, {"x": (cond, F.col(score_col))})["x"]
    return {
        "threshold": threshold,
        "alerts": metrics["alerts"],
        "alerts_per_day": metrics["alerts"] / days,
        "recall_point": metrics["by_type"]["point"],
        "recall_sustained": metrics["by_type"]["sustained"],
        "mean_delay": metrics["mean_delay"],
        "precision": metrics["precision"],
    }


def compare_rules(
    df: DataFrame,
    days: float,
    budget: float,
    n: int = 3,
    m: int = 2,
    thresholds=THRESHOLDS,
    score_col: str = "rz",
) -> dict:
    """Same alert budget, with and without the m-of-n rule.

    plain:              no rule, lowest threshold within budget
    rule:               m-of-n rule, lowest threshold within budget
    plain_at_rule_thr:  no rule at the rule's (lower) threshold -> what the rule saves you from
    """
    curve = threshold_curve(df, thresholds, days, score_col)
    plain_pick = pick_threshold(curve, budget)
    counts = rule_alert_counts(df, thresholds, n, m, score_col)
    rule_pick = pick_threshold(
        [{"threshold": t, "alerts_per_day": c / days} for t, c in counts.items()], budget
    )
    return {
        "budget": budget,
        "n": n,
        "m": m,
        "plain": {**evaluate_rule(df, plain_pick["threshold"], 1, 1, days, score_col),
                  "within_budget": plain_pick["within_budget"]},
        "rule": {**evaluate_rule(df, rule_pick["threshold"], n, m, days, score_col),
                 "within_budget": rule_pick["within_budget"]},
        "plain_at_rule_thr": evaluate_rule(df, rule_pick["threshold"], 1, 1, days, score_col),
        "curve": curve,
    }


def write_alerts(
    df: DataFrame, threshold: float, n: int, m: int, out: Path, score_col: str = "rz"
) -> None:
    """Save the alert table, one folder per device (partitionBy)."""
    alerts = df.withColumn("alert", rule_alert(score_col, threshold, n, m).cast("int"))
    alerts.select(
        "device", "t", "temp", "vib", score_col, "km_dist", "is_injected", "anomaly_type", "alert"
    ).write.mode("overwrite").partitionBy("device").parquet(str(out / "alerts"))

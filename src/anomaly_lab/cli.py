"""Command line: one command per step, plus run-all.

Each step reads the saved output of the earlier steps (folders under --out),
prints a table and a "What this means" paragraph, and saves its own result.
"""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Sequence
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from anomaly_lab import alert, baseline, cluster, evaluate, generate, report, residual
from anomaly_lab import spark as spark_env
from anomaly_lab.baseline import BASELINE_COLS
from anomaly_lab.cluster import CLUSTER_COLS
from anomaly_lab.i18n import LANGS, resolve_lang, tr
from anomaly_lab.residual import RESIDUAL_COLS

STEPS = ("generate", "baseline", "residual", "cluster", "evaluate", "alert", "run-all")
KEYS = ["device", "t"]


class CliError(Exception):
    """A problem the user can fix; printed without a traceback, exit code 2."""


class _Formatter(argparse.HelpFormatter):
    """Help formatter whose 'usage:' prefix comes from the message table."""

    lang = "en"

    def add_usage(self, usage, actions, groups, prefix=None):  # noqa: ANN001
        return super().add_usage(usage, actions, groups, tr(self.lang, "help_usage_prefix"))


def _arg_detail(lang: str, message: str) -> str:
    """Name the offending argument, using only names/values from argparse's message."""
    if m := re.match(r"argument (\S+): invalid choice: '([^']*)'", message):
        return tr(lang, "err_arg_choice", name=m[1], value=m[2])
    if m := re.match(r"argument (\S+): invalid \w+ value: '([^']*)'", message):
        return tr(lang, "err_arg_value", name=m[1], value=m[2])
    if m := re.match(r"argument (\S+): expected", message):
        return tr(lang, "err_arg_missing_value", name=m[1])
    if m := re.match(r"unrecognized arguments: (.*)", message):
        return tr(lang, "err_arg_unknown", value=m[1])
    if m := re.match(r"the following arguments are required: (.*)", message):
        return tr(lang, "err_arg_required", name=m[1])
    return tr(lang, "err_arg_other", value=message)


class _Parser(argparse.ArgumentParser):
    """ArgumentParser that reports its own errors in the chosen language."""

    lang = "en"

    def error(self, message: str):  # noqa: ANN201
        self.print_usage(sys.stderr)
        # argparse's own wording (the message) is English, so it is not shown
        print(tr(self.lang, "err_arg"), file=sys.stderr)
        print(_arg_detail(self.lang, message), file=sys.stderr)
        raise SystemExit(2)


def build_parser(lang: str) -> argparse.ArgumentParser:
    formatter = type("LangFormatter", (_Formatter,), {"lang": lang})
    p = _Parser(prog="pyspark-anomaly", description=tr(lang, "help_description"),
                add_help=False, formatter_class=formatter)
    p.lang = lang
    # Section titles and the -h flag are set here so argparse's English defaults never show.
    p._positionals.title = tr(lang, "help_positionals")
    p._optionals.title = tr(lang, "help_options")
    p.add_argument("-h", "--help", action="help", help=tr(lang, "help_help"))
    p.add_argument("step", choices=STEPS, metavar="step", help=tr(lang, "help_step"))
    p.add_argument("--devices", type=int, default=6, help=tr(lang, "help_devices"))
    p.add_argument("--days", type=int, default=14, help=tr(lang, "help_days"))
    p.add_argument("--seed", type=int, default=42, help=tr(lang, "help_seed"))
    p.add_argument("--out", default="out", help=tr(lang, "help_out"))
    p.add_argument("--lang", choices=LANGS, default=None, help=tr(lang, "help_lang"))
    p.add_argument("--threshold", type=float, default=3.5, help=tr(lang, "help_threshold"))
    p.add_argument("--z-threshold", type=float, default=3.0, help=tr(lang, "help_zthreshold"))
    p.add_argument("--kmeans-top", type=float, default=0.03, help=tr(lang, "help_kmeans_top"))
    p.add_argument("--n", type=int, default=3, help=tr(lang, "help_n"))
    p.add_argument("--m", type=int, default=2, help=tr(lang, "help_m"))
    p.add_argument("--budget", type=float, default=1.5, help=tr(lang, "help_budget"))
    return p


def _require(path: Path, step: str) -> None:
    if not path.exists():
        raise CliError(("err_no_input", {"path": path, "step": step}))


def _show(title: str, body: str) -> None:
    print(f"\n== {title} ==\n{body}")


def _meaning(lang: str, key: str, **values: object) -> None:
    print(f"\n{tr(lang, 'meaning_title')}: {tr(lang, key, **values)}")


def _method_table(lang: str, graded: dict[str, dict]) -> str:
    headers = [tr(lang, k) for k in (
        "col_method", "col_alerts", "col_recall", "col_fpr", "col_precision", "col_undecidable")]
    rows = [[n, m["alerts"], m["recall"], m["fpr"], m["precision"], m["undecidable"]]
            for n, m in graded.items()]
    return report.table(headers, rows)


def _read_data(spark: SparkSession, out: Path, step: str) -> DataFrame:
    _require(out / "data", step)
    return spark.read.parquet(str(out / "data"))


# --------------------------------------------------------------------------- steps


def step_generate(spark: SparkSession, a: argparse.Namespace, lang: str, out: Path) -> None:
    df = generate.generate(spark, a.devices, a.days, a.seed)
    df.write.mode("overwrite").parquet(str(out / "data"))
    counts = {r["anomaly_type"]: r["count"] for r in df.groupBy("anomaly_type").count().collect()}
    total = sum(counts.values())
    injected = total - counts.get("normal", 0)
    _show(tr(lang, "generate_title", rows=total, devices=a.devices, days=a.days, seed=a.seed),
          report.table([tr(lang, "col_type"), tr(lang, "col_rows")],
                       [["normal", counts.get("normal", 0)]]
                       + [[t, counts.get(t, 0)] for t in generate.TYPES]))
    _meaning(lang, "generate_meaning", injected=injected, ref=f"dev{a.devices:02d}")
    print(f"\n{tr(lang, 'synthetic_notice')}")


def step_baseline(spark: SparkSession, a: argparse.Namespace, lang: str, out: Path) -> None:
    df = baseline.add_baseline(_read_data(spark, out, "generate")).cache()
    df.select(*KEYS, *BASELINE_COLS).write.mode("overwrite").parquet(str(out / "baseline"))
    graded = evaluate.evaluate_flags(df, {
        "moving_z": (F.abs("z") > a.z_threshold, F.col("z")),
        "raw_iqr": (F.col("iqr_flag") == 1, None),
    })
    _show(tr(lang, "baseline_title"), _method_table(lang, graded))
    _meaning(lang, "baseline_meaning", iqr=graded["raw_iqr"]["alerts"],
             z=graded["moving_z"]["tp"], und=graded["moving_z"]["undecidable"])
    df.unpersist()


def step_residual(spark: SparkSession, a: argparse.Namespace, lang: str, out: Path) -> None:
    df = residual.add_residual(_read_data(spark, out, "generate"))
    df = baseline.add_iqr(df).cache()
    df.select(*KEYS, *RESIDUAL_COLS).write.mode("overwrite").parquet(str(out / "residual"))
    graded = evaluate.evaluate_flags(df, {
        "raw_iqr": (F.col("iqr_flag") == 1, None),
        "robust_z": (F.col("rz") > a.threshold, F.col("rz")),
    })
    _show(tr(lang, "residual_title"), _method_table(lang, graded))
    _meaning(lang, "residual_meaning", rz=report.pct(graded["robust_z"]["by_type"]["point"]),
             iqr=report.pct(graded["raw_iqr"]["by_type"]["point"]))
    df.unpersist()


def step_cluster(spark: SparkSession, a: argparse.Namespace, lang: str, out: Path) -> None:
    _require(out / "residual", "residual")
    df = _read_data(spark, out, "generate").join(
        spark.read.parquet(str(out / "residual")).select(*KEYS, *RESIDUAL_COLS), KEYS)
    df = cluster.add_kmeans(df).cache()
    df.select(*KEYS, *CLUSTER_COLS).write.mode("overwrite").parquet(str(out / "cluster"))
    cut = evaluate.kmeans_cut(df, a.kmeans_top)
    combos = df.filter(F.col("anomaly_type") == generate.COMBO)
    missed = combos.filter(~F.coalesce(F.col("rz") > a.threshold, F.lit(False)))
    caught = missed.filter(F.col("km_dist") > cut)
    n_combo, n_missed, n_caught = combos.count(), missed.count(), caught.count()
    _show(tr(lang, "cluster_title"), report.table(
        [tr(lang, "col_type"), tr(lang, "col_rows")],
        [[tr(lang, "cluster_row_total"), n_combo], [tr(lang, "cluster_row_missed"), n_missed],
         [tr(lang, "cluster_row_caught"), n_caught]]))
    _meaning(lang, "cluster_meaning", combo=n_combo, missed=n_missed, caught=n_caught)
    df.unpersist()


def step_evaluate(spark: SparkSession, a: argparse.Namespace, lang: str, out: Path) -> None:
    for folder, step in (("data", "generate"), ("baseline", "baseline"),
                         ("residual", "residual"), ("cluster", "cluster")):
        _require(out / folder, step)
    df = evaluate.load_scored(spark, out).cache()
    days = evaluate.infer_days(df)
    graded = evaluate.compare_methods(df, a.z_threshold, a.threshold, a.kmeans_top)
    _show(tr(lang, "evaluate_title"), _method_table(lang, graded))
    print()
    print(report.table(
        [tr(lang, k) for k in ("col_method", "col_point", "col_sustained", "col_combo")],
        [[n, m["by_type"]["point"], m["by_type"]["sustained"], m["by_type"]["combo"]]
         for n, m in graded.items()]))
    curve = evaluate.threshold_curve(df, days=days)
    _show(tr(lang, "evaluate_curve_title"), report.table(
        [tr(lang, k) for k in ("col_threshold", "col_alerts", "col_per_day", "col_recall",
                               "col_fpr", "col_precision", "col_point")],
        [[r["threshold"], r["alerts"], r["alerts_per_day"], r["recall"], r["fpr"],
          r["precision"], r["recall_point"]] for r in curve]))
    path = out / "threshold_curve.csv"
    evaluate.write_curve_csv(curve, path)
    _meaning(lang, "evaluate_meaning", top=curve[-1]["threshold"], rec=report.pct(curve[-1]["recall_point"]))
    print(f"\n{tr(lang, 'evaluate_saved', path=path)}")
    df.unpersist()


def step_alert(spark: SparkSession, a: argparse.Namespace, lang: str, out: Path) -> None:
    for folder, step in (("data", "generate"), ("baseline", "baseline"),
                         ("residual", "residual"), ("cluster", "cluster")):
        _require(out / folder, step)
    df = evaluate.load_scored(spark, out).cache()
    days = evaluate.infer_days(df)
    cmp = alert.compare_rules(df, days, a.budget, a.n, a.m)
    plain, rule, flood = cmp["plain"], cmp["rule"], cmp["plain_at_rule_thr"]
    labels = [
        (tr(lang, "alert_label_plain"), plain),
        (tr(lang, "alert_label_rule", n=a.n, m=a.m), rule),
        (tr(lang, "alert_label_flood"), flood),
    ]
    _show(tr(lang, "alert_title", budget=a.budget, n=a.n, m=a.m), report.table(
        [tr(lang, k) for k in ("col_rule", "col_threshold", "col_per_day", "col_point",
                               "col_sustained", "col_delay", "col_precision")],
        [[name, r["threshold"], r["alerts_per_day"], r["recall_point"],
          r["recall_sustained"], r["mean_delay"], r["precision"]] for name, r in labels]))
    key = "alert_budget_line" if plain["within_budget"] else "alert_budget_none"
    print("\n" + tr(lang, key, thr=plain["threshold"], per_day=f"{plain['alerts_per_day']:.2f}"))
    if plain["within_budget"] and rule["within_budget"]:
        _meaning(lang, "alert_meaning", budget=a.budget, t_plain=plain["threshold"],
                 s_plain=report.pct(plain["recall_sustained"]), t_rule=rule["threshold"],
                 s_rule=report.pct(rule["recall_sustained"]), n=a.n, m=a.m,
                 p_rule=report.pct(rule["recall_point"]), flood=f"{flood['alerts_per_day']:.1f}")
    else:  # no threshold keeps the alerts inside the budget: do not pretend one does
        _meaning(lang, "alert_meaning_over", budget=a.budget, t_plain=plain["threshold"],
                 per_day=f"{plain['alerts_per_day']:.1f}")
    alert.write_alerts(df, rule["threshold"], a.n, a.m, out)
    print(f"\n{tr(lang, 'alert_saved', path=out / 'alerts')}")
    df.unpersist()


RUNNERS = {
    "generate": step_generate,
    "baseline": step_baseline,
    "residual": step_residual,
    "cluster": step_cluster,
    "evaluate": step_evaluate,
    "alert": step_alert,
}


# --------------------------------------------------------------------------- main


def main(argv: Sequence[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    # Find --lang first so that --help can already be shown in that language.
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--lang", choices=LANGS, default=None)
    known, _ = pre.parse_known_args(argv)
    lang = resolve_lang(known.lang)

    args = build_parser(lang).parse_args(argv)
    lang = resolve_lang(args.lang)

    try:
        if args.devices < 2:
            raise CliError(("err_devices_min", {}))
        if args.days < 3:
            raise CliError(("err_days_min", {}))
        if not 0 < args.kmeans_top < 1:
            raise CliError(("err_kmeans_top_range", {}))
        if not 1 <= args.m <= args.n:
            raise CliError(("err_m_range", {}))
        if not args.budget > 0:
            raise CliError(("err_budget_min", {}))
        try:
            spark_env.ensure_java()
        except spark_env.JavaError as err:
            key = "err_java_missing" if err.kind == "missing" else "err_java_old"
            raise CliError((key, {"found": err.found})) from err

        out = Path(args.out)
        steps = list(RUNNERS) if args.step == "run-all" else [args.step]
        with spark_env.spark_session() as spark:
            for name in steps:
                RUNNERS[name](spark, args, lang, out)
        if args.step == "run-all":
            print(f"\n{tr(lang, 'run_all_done', out=out)}")
    except CliError as err:
        key, values = err.args[0]
        print(tr(lang, key, **values), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

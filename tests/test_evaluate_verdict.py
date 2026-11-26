"""Evaluate commentary is chosen from the graded numbers (no Spark needed)."""
import pytest

from anomaly_lab.cli import evaluate_verdict
from anomaly_lab.i18n import MESSAGES, tr


def graded(rz_prec, km_prec, km_combo):
    return {
        "robust_z": {"precision": rz_prec, "by_type": {"combo": 0.0}},
        "kmeans": {"precision": km_prec, "by_type": {"combo": km_combo}},
    }


def test_default_run_keeps_original_sentence():
    key, values = evaluate_verdict(graded(0.971, 0.855, 1.0))
    assert key == "evaluate_verdict_both"
    assert "kmeans sees combos but also raises more false alarms" in tr("en", key, **values)
    assert "오탐도 더 냅니다" in tr("ko", key, **values)


@pytest.mark.parametrize("lang", ["en", "ko"])
def test_kmeans_top_001_does_not_claim_more_false_alarms(lang):
    # --kmeans-top 0.01: kmeans precision 1.000 > robust_z 0.971, combo rate 0.067
    key, values = evaluate_verdict(graded(0.971, 1.0, 0.067))
    assert key == "evaluate_verdict_neither"
    text = tr(lang, key, **values)
    assert "7%" in text and "1.000" in text and "0.971" in text
    assert "more false alarms" not in text and "오탐도 더 냅니다" not in text
    assert "sees combos" not in text


def test_seed_7_precision_higher_but_combos_seen():
    # --seed 7: kmeans precision 0.968 > robust_z 0.950, combos still found
    key, _ = evaluate_verdict(graded(0.950, 0.968, 1.0))
    assert key == "evaluate_verdict_combo"


def test_more_alarms_but_few_combos():
    key, values = evaluate_verdict(graded(0.97, 0.6, 0.1))
    assert key == "evaluate_verdict_alarms"
    assert "does not reliably see" in tr("en", key, **values)


def test_missing_precision_is_not_worse():
    key, values = evaluate_verdict(graded(0.9, None, None))
    assert key == "evaluate_verdict_neither"
    assert "n/a" in tr("en", key, **values)


def test_verdict_keys_exist_in_both_languages():
    for key in ("evaluate_verdict_both", "evaluate_verdict_combo",
                "evaluate_verdict_alarms", "evaluate_verdict_neither"):
        assert key in MESSAGES["en"] and key in MESSAGES["ko"]

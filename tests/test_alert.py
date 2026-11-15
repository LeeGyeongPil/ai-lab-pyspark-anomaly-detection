from anomaly_lab import alert


def test_budget_picks_lowest_threshold_within_budget():
    curve = [
        {"threshold": 2.0, "alerts_per_day": 10.0},
        {"threshold": 3.0, "alerts_per_day": 3.0},
        {"threshold": 4.0, "alerts_per_day": 1.0},
        {"threshold": 5.0, "alerts_per_day": 0.5},
    ]
    pick = alert.pick_threshold(curve, budget=3.0)
    assert pick["threshold"] == 3.0 and pick["within_budget"] is True
    assert alert.pick_threshold(curve, budget=0.8)["threshold"] == 5.0
    none = alert.pick_threshold(curve, budget=0.1)  # nothing fits: highest threshold, flagged
    assert none["threshold"] == 5.0 and none["within_budget"] is False


def _series(spark):
    """20 hours: two isolated spikes (t=3, 10) and a weak drift (t=15-17)."""
    scores = {3: 5.0, 10: 5.0, 15: 2.6, 16: 2.8, 17: 3.0}
    rows = []
    for t in range(20):
        if t in (3, 10):
            kind, ev = "point_strong", t + 100
        elif t in (15, 16, 17):
            kind, ev = "sustained", 1
        else:
            kind, ev = "normal", -1
        rows.append(("a", t, scores.get(t, 0.1), int(kind != "normal"), kind, ev))
    return spark.createDataFrame(
        rows, "device string, t int, rz double, is_injected int, anomaly_type string, event_id int"
    )


def test_m_of_n_tradeoff(spark):
    df = _series(spark)
    grid = [2.0, 2.5, 3.0, 3.5]
    # same threshold: the 2-of-3 rule silences isolated spikes
    plain_2 = alert.evaluate_rule(df, 2.0, 1, 1, days=1)
    rule_2 = alert.evaluate_rule(df, 2.0, 3, 2, days=1)
    assert rule_2["alerts"] < plain_2["alerts"]
    assert rule_2["recall_point"] == 0.0 and plain_2["recall_point"] == 1.0
    # same budget: the rule can use a lower threshold and still finds the drift
    cmp = alert.compare_rules(df, days=1, budget=3, thresholds=grid)
    assert cmp["plain"]["threshold"] == 3.0 and cmp["plain"]["recall_sustained"] == 0.0
    assert cmp["rule"]["threshold"] == 2.0
    assert cmp["rule"]["recall_sustained"] >= cmp["plain"]["recall_sustained"]
    assert cmp["rule"]["recall_sustained"] == 1.0
    assert cmp["plain_at_rule_thr"]["alerts"] > cmp["rule"]["alerts"]


def test_rule_alert_counts_match_single_runs(spark):
    df = _series(spark)
    counts = alert.rule_alert_counts(df, [2.0, 4.0], 3, 2)
    assert counts[2.0] == alert.evaluate_rule(df, 2.0, 3, 2, days=1)["alerts"] == 3
    assert counts[4.0] == 0


def test_write_alerts_partitions_by_device(scored, tmp_path):
    alert.write_alerts(scored, 3.5, 3, 2, tmp_path)
    folders = sorted(p.name for p in (tmp_path / "alerts").glob("device=*"))
    assert folders == ["device=dev01", "device=dev02", "device=dev03"]

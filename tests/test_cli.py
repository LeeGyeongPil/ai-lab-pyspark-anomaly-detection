from pathlib import Path

from anomaly_lab import cli


def test_run_all_small(spark, tmp_path, capsys, monkeypatch):
    monkeypatch.setenv("LANG", "C")
    out = tmp_path / "out"
    code = cli.main(["--lang", "en", "run-all", "--devices", "3", "--days", "3", "--out", str(out)])
    text = capsys.readouterr().out
    assert code == 0
    for needle in ("robust_z", "kmeans", "recall", "What this means"):
        assert needle in text
    assert (out / "threshold_curve.csv").is_file()
    assert list((out / "alerts").glob("device=*"))


def test_korean_output(spark, tmp_path, capsys):
    out = tmp_path / "out"
    assert cli.main(["--lang", "ko", "generate", "--devices", "3", "--days", "3", "--out", str(out)]) == 0
    text = capsys.readouterr().out
    assert "그래서 무슨 뜻일까요:" in text and "연습용 데이터" in text


def test_step_without_input_explains_fix(spark, tmp_path, capsys):
    code = cli.main(["--lang", "en", "baseline", "--out", str(tmp_path / "nothing")])
    assert code == 2
    err = capsys.readouterr().err
    assert "pyspark-anomaly generate" in err


def test_bad_option_exits_2(tmp_path, capsys):
    assert cli.main(["--lang", "en", "generate", "--days", "1", "--out", str(tmp_path)]) == 2
    assert "--days must be >= 3" in capsys.readouterr().err


def test_korean_help_has_no_english_boilerplate(capsys):
    try:
        cli.main(["--lang", "ko", "--help"])
    except SystemExit as exit_:
        assert exit_.code == 0
    text = capsys.readouterr().out
    assert "사용법:" in text and "옵션" in text
    for english in ("usage:", "options:", "positional arguments", "show this help"):
        assert english not in text


def test_korean_option_errors_are_korean(tmp_path, capsys):
    assert cli.main(["--lang", "ko", "generate", "--devices", "1", "--out", str(tmp_path)]) == 2
    err = capsys.readouterr().err
    assert "--devices 는 2 이상" in err and "must be" not in err


def test_korean_argparse_error_is_korean(capsys):
    try:
        cli.main(["--lang", "ko", "bogus"])
    except SystemExit as exit_:
        assert exit_.code == 2
    err = capsys.readouterr().err
    assert "사용법:" in err and "인자가 올바르지 않습니다" in err
    assert "usage:" not in err


def test_budget_must_be_positive(tmp_path, capsys):
    for bad in ("-1", "0"):
        assert cli.main(["--lang", "en", "alert", "--budget", bad, "--out", str(tmp_path)]) == 2
        assert "--budget must be greater than 0" in capsys.readouterr().err


def test_over_budget_meaning_does_not_claim_a_fit(spark, tmp_path, capsys):
    out = tmp_path / "out"
    assert cli.main(["--lang", "en", "run-all", "--devices", "2", "--days", "3",
                     "--budget", "0.01", "--out", str(out)]) == 0
    text = capsys.readouterr().out
    assert "No threshold fits the budget" in text
    assert "Even the highest threshold" in text
    assert "no rule needs threshold" not in text


def _err(argv, capsys):
    try:
        cli.main(argv)
    except SystemExit as exit_:
        assert exit_.code == 2
    return capsys.readouterr().err


def test_arg_error_names_bad_value_en(capsys):
    err = _err(["--lang", "en", "generate", "--devices", "abc"], capsys)
    assert "--devices" in err and "abc" in err


def test_arg_error_names_unknown_option_en(capsys):
    err = _err(["--lang", "en", "generate", "--colour", "1"], capsys)
    assert "--colour" in err


def test_arg_error_names_bad_values_ko(capsys):
    err = _err(["--lang", "ko", "generate", "--devices", "abc"], capsys)
    assert "--devices" in err and "abc" in err and "값이 올바르지 않습니다" in err
    err = _err(["--lang", "ko", "bogus"], capsys)
    assert "bogus" in err and "usage:" not in err
    err = _err(["--lang", "ko", "generate", "--colour", "1"], capsys)
    assert "--colour" in err

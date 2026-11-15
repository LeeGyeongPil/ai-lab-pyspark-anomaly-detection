import pytest

from anomaly_lab import cli
from anomaly_lab.i18n import MESSAGES, resolve_lang, tr


def test_message_keys_match():
    assert set(MESSAGES["en"]) == set(MESSAGES["ko"])
    for key in MESSAGES["en"]:  # same placeholders in both languages
        import string

        names = lambda lang: {f for _, f, _, _ in string.Formatter().parse(MESSAGES[lang][key]) if f}  # noqa: E731
        assert names("en") == names("ko"), key


def test_lang_resolution_order():
    env = {"PYSPARK_ANOMALY_LANG": "ko", "LC_ALL": "en_US.UTF-8", "LANG": "en_US.UTF-8"}
    assert resolve_lang("en", env) == "en"  # --lang wins
    assert resolve_lang(None, env) == "ko"  # then PYSPARK_ANOMALY_LANG
    assert resolve_lang(None, {"LC_ALL": "ko_KR.UTF-8", "LANG": "en_US"}) == "ko"  # then locale
    assert resolve_lang(None, {"LC_MESSAGES": "en_US", "LANG": "ko_KR"}) == "en"
    assert resolve_lang(None, {"LANG": "ko_KR.UTF-8"}) == "ko"
    assert resolve_lang(None, {}) == "en"  # default
    assert resolve_lang(None, {"PYSPARK_ANOMALY_LANG": "xx", "LANG": "ko_KR"}) == "ko"  # invalid ignored


def test_tr_fills_placeholders():
    assert "17" in tr("en", "err_java_old", found=11)
    assert "11" in tr("ko", "err_java_old", found=11)


@pytest.mark.parametrize("lang, needle", [("ko", "실행할 단계"), ("en", "step to run")])
def test_help_in_each_language(lang, needle, capsys, monkeypatch):
    monkeypatch.setenv("LANG", "C")
    with pytest.raises(SystemExit) as exc:
        cli.main(["--lang", lang, "--help"])
    assert exc.value.code == 0
    assert needle in capsys.readouterr().out

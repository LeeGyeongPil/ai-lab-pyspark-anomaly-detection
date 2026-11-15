import pytest

from anomaly_lab import cli, spark as spark_env


def test_missing_java_exits_2(monkeypatch, capsys, tmp_path):
    monkeypatch.setattr(spark_env, "find_java", lambda environ=None: None)
    assert cli.main(["--lang", "en", "generate", "--out", str(tmp_path)]) == 2
    assert "Java 17" in capsys.readouterr().err
    assert cli.main(["--lang", "ko", "generate", "--out", str(tmp_path)]) == 2
    assert "Java를 찾지 못했습니다" in capsys.readouterr().err


def test_old_java_exits_2(monkeypatch, capsys, tmp_path):
    monkeypatch.setattr(spark_env, "find_java", lambda environ=None: ("/x/java", 11))
    assert cli.main(["--lang", "en", "generate", "--out", str(tmp_path)]) == 2
    assert "Java 11" in capsys.readouterr().err


def test_tmpdir_points_to_project(tmp_path):
    env: dict[str, str] = {"JAVA_TOOL_OPTIONS": "-Djava.net.preferIPv4Stack=true"}
    used = spark_env.configure_env(tmp_path / ".spark-tmp", env)
    assert used == (tmp_path / ".spark-tmp").resolve() and used.is_dir()
    assert f"-Djava.io.tmpdir={used}" in env["JAVA_TOOL_OPTIONS"]
    assert "preferIPv4Stack" in env["JAVA_TOOL_OPTIONS"]  # existing options kept
    assert env["SPARK_LOCAL_DIRS"] == str(used)


def test_existing_env_respected(tmp_path):
    env = {"JAVA_TOOL_OPTIONS": "-Djava.io.tmpdir=/mine", "SPARK_LOCAL_DIRS": "/mine"}
    assert spark_env.configure_env(tmp_path / ".spark-tmp", env) is None
    assert env == {"JAVA_TOOL_OPTIONS": "-Djava.io.tmpdir=/mine", "SPARK_LOCAL_DIRS": "/mine"}
    assert not (tmp_path / ".spark-tmp").exists()


def test_find_java_ignores_broken_java_home(tmp_path):
    # a wrong JAVA_HOME must not crash: later candidates (or None) are used
    result = spark_env.find_java({"JAVA_HOME": str(tmp_path), "PATH": ""})
    assert result is None or result[1] >= 8


def test_ensure_java_real_version():
    try:
        assert spark_env.ensure_java()
    except spark_env.JavaError as err:  # fine on a machine without Java 17+
        pytest.skip(f"no Java 17+ here: {err}")

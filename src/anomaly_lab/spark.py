"""Java check, scratch folder and Spark session setup.

Three things go wrong most often when a first PySpark program starts:
1. Java is missing or too old  -> JAVA_GATEWAY_EXITED
2. The system temp folder is not writable (sandbox, CI) -> same error
3. Too many shuffle partitions for a tiny local data set -> slow
This module handles all three in one place.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from collections.abc import Iterator, MutableMapping
from contextlib import contextmanager
from pathlib import Path

MIN_JAVA = 17


class JavaError(Exception):
    """Java is missing (kind="missing") or older than MIN_JAVA (kind="too_old")."""

    def __init__(self, kind: str, found: int | None = None) -> None:
        super().__init__(f"{kind} (found={found})")
        self.kind = kind
        self.found = found


def _java_major(java_exec: str) -> int | None:
    """Run `java -version` and return the major version (8 for "1.8.0")."""
    try:
        proc = subprocess.run(
            [java_exec, "-version"], capture_output=True, text=True, timeout=30
        )
    except (OSError, subprocess.SubprocessError):
        return None
    match = re.search(r'version "(\d+)(?:\.(\d+))?', proc.stderr + proc.stdout)
    if not match:
        return None
    major = int(match.group(1))
    return int(match.group(2) or 0) if major == 1 else major


def find_java(environ: MutableMapping[str, str] | None = None) -> tuple[str, int] | None:
    """Look for Java in JAVA_HOME, then /usr/libexec/java_home (macOS), then PATH.

    Returns (java executable, major version) or None.
    """
    environ = os.environ if environ is None else environ
    candidates: list[str] = []
    home = environ.get("JAVA_HOME")
    if home:
        candidates.append(str(Path(home) / "bin" / "java"))
    if sys.platform == "darwin" and Path("/usr/libexec/java_home").exists():
        try:
            out = subprocess.run(
                ["/usr/libexec/java_home"], capture_output=True, text=True, timeout=10
            ).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            out = ""
        if out:
            candidates.append(str(Path(out) / "bin" / "java"))
    on_path = shutil.which("java", path=environ.get("PATH"))
    if on_path:
        candidates.append(on_path)
    for java_exec in candidates:
        if Path(java_exec).exists():
            major = _java_major(java_exec)
            if major is not None:
                return java_exec, major
    return None


def ensure_java(environ: MutableMapping[str, str] | None = None) -> str:
    """Return the Java executable or raise JavaError. Never starts Spark."""
    found = find_java(environ)
    if found is None:
        raise JavaError("missing")
    java_exec, major = found
    if major < MIN_JAVA:
        raise JavaError("too_old", major)
    return java_exec


def configure_env(
    tmp_root: Path | None = None, environ: MutableMapping[str, str] | None = None
) -> Path | None:
    """Point Java and Spark scratch files at ./.spark-tmp (project folder).

    Values the user already set are respected. Must run before the first
    SparkSession is created, because the JVM reads these once at start.
    Returns the folder that was used, or None if both values were already set.
    """
    environ = os.environ if environ is None else environ
    tmp_dir = (tmp_root or Path.cwd() / ".spark-tmp").resolve()
    used = None
    options = environ.get("JAVA_TOOL_OPTIONS", "")
    need_tmpdir = "-Djava.io.tmpdir" not in options
    need_local = "SPARK_LOCAL_DIRS" not in environ
    if need_tmpdir or need_local:
        tmp_dir.mkdir(parents=True, exist_ok=True)
        used = tmp_dir
    if need_tmpdir:
        environ["JAVA_TOOL_OPTIONS"] = f"{options} -Djava.io.tmpdir={tmp_dir}".strip()
    if need_local:
        environ["SPARK_LOCAL_DIRS"] = str(tmp_dir)
    return used


@contextmanager
def spark_session(app_name: str = "anomaly-lab", tmp_root: Path | None = None) -> Iterator:
    """Yield a small local SparkSession; stop it afterwards if we created it."""
    from pyspark.sql import SparkSession

    existing = SparkSession.getActiveSession()
    if existing is not None:  # e.g. a test session: reuse and leave it running
        yield existing
        return
    configure_env(tmp_root)
    spark = (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")  # default 200 is slow for tiny data
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        yield spark
    finally:
        spark.stop()

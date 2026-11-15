"""Shared Spark session and small data (3 devices x 3 days) for all tests."""

from pathlib import Path

import pytest

from anomaly_lab import baseline, cluster, generate, residual
from anomaly_lab import spark as spark_env

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def spark():
    # Scratch files go to <project>/.spark-tmp (the system temp folder may be read-only).
    with spark_env.spark_session("anomaly-tests", tmp_root=ROOT / ".spark-tmp") as session:
        yield session


@pytest.fixture(scope="session")
def residual_df(spark):
    """Small data with baseline and residual columns (no KMeans yet)."""
    df = generate.generate(spark, devices=3, days=3, seed=42)
    df = residual.add_residual(baseline.add_baseline(df)).cache()
    df.count()
    return df


@pytest.fixture(scope="session")
def scored(residual_df):
    """Small data with every score column: baseline, residual and KMeans."""
    df = cluster.add_kmeans(residual_df).cache()
    df.count()
    return df

"""Step 4: look at temperature and vibration together with KMeans.

Pipeline: VectorAssembler -> StandardScaler -> KMeans.
Score = distance from a row to its nearest cluster center, measured in the
*scaled* space (the space the centers live in). Without scaling, the feature
with the bigger numbers would decide the distance alone.
"""

from __future__ import annotations

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

CLUSTER_COLS = ["cluster", "km_dist"]
FEATURES = ["resid", "vib_resid"]


def add_kmeans(df: DataFrame, k: int = 3, seed: int = 7, keep_debug: bool = False) -> DataFrame:
    """Add `cluster` and `km_dist`. With keep_debug also `scaled` and `center` arrays."""
    spark = df.sparkSession
    pipeline = Pipeline(
        stages=[
            VectorAssembler(inputCols=FEATURES, outputCol="raw"),
            StandardScaler(inputCol="raw", outputCol="features", withMean=True, withStd=True),
            KMeans(k=k, seed=seed, featuresCol="features", predictionCol="cluster"),
        ]
    )
    model = pipeline.fit(df)
    centers = model.stages[-1].clusterCenters()  # numpy arrays, in scaled space
    center_df = spark.createDataFrame(
        [(i, [float(x) for x in c]) for i, c in enumerate(centers)],
        "cluster int, center array<double>",
    )
    scored = (
        model.transform(df)
        .join(F.broadcast(center_df), "cluster")
        .withColumn("scaled", vector_to_array("features"))
    )
    squares = F.zip_with("scaled", "center", lambda a, b: (a - b) * (a - b))
    scored = scored.withColumn(
        "km_dist", F.sqrt(F.aggregate(squares, F.lit(0.0), lambda acc, x: acc + x))
    ).drop("raw", "features")
    return scored if keep_debug else scored.drop("scaled", "center")

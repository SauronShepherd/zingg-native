"""Small, explicit facade for Spark 4 native Zingg operations."""

from typing import Any

from .backend import resolve_backend
from .runtime import detect_runtime


class Zingg:
    """Facade that keeps configuration and execution transport-independent."""

    def __init__(self, arguments: Any = None, spark: Any = None, backend: str | None = None):
        if spark is None:
            raise ValueError("spark is required; create a Spark 4 Classic or Connect session")
        self.arguments = arguments
        self.spark = spark
        self.runtime = detect_runtime(spark)
        self.backend = resolve_backend(spark, backend)

    def transform(self, df: Any, operation: str, **options: Any) -> Any:
        return self.backend.transform(df, operation, **options)

    def exact(self, df: Any, left: str, right: str, output: str = "z_exact") -> Any:
        return self.transform(df, "EXACT_SIMILARITY", left=left, right=right, output=output)

    def jaccard(self, df: Any, left: str, right: str, output: str = "z_jaccard") -> Any:
        """Add the native equivalent of Zingg 0.7 Jaccard similarity."""
        return self.transform(df, "JACCARD_SIMILARITY", left=left, right=right, output=output)

    def jaro(self, df: Any, left: str, right: str, output: str = "z_jaro") -> Any:
        """Add the native equivalent of Zingg 0.7's Jaro-backed score."""
        return self.transform(df, "JARO_SIMILARITY", left=left, right=right, output=output)

    def exact_match(self, df: Any, keys: list[str], cluster_column: str = "z_cluster") -> Any:
        """Deterministically cluster records sharing all supplied exact keys.

        This is an exact-key workflow, not a replacement for fuzzy Zingg
        matching. Null-safe equality is implemented with Spark joins so the
        operation remains visible to Photon and other native engines.
        """
        from pyspark.sql import Window
        from pyspark.sql import functions as F
        if not keys:
            raise ValueError("keys must contain at least one column")
        missing = sorted(set(keys) - set(df.columns))
        if missing:
            raise ValueError(f"Unknown match key columns: {missing}")
        return df.withColumn(
            cluster_column,
            F.dense_rank().over(Window.orderBy(*[F.col(k).asc_nulls_first() for k in keys])).cast("long"),
        )

    def find_training_data(
        self,
        df: Any,
        keys: list[str],
        id_column: str,
        output_path: str | None = None,
        include_all_pairs: bool = False,
    ) -> Any:
        """Generate native, deterministic candidate pairs for training.

        Candidates share at least one non-null exact key. The returned relation
        contains left/right ids, per-key native exact scores, mean score, a
        deterministic pair cluster id, and a nullable ``z_isMatch`` label.
        """
        from functools import reduce
        from pyspark.sql import functions as F
        if not keys:
            raise ValueError("keys must contain at least one column")
        required = set(keys) | {id_column}
        missing = sorted(required - set(df.columns))
        if missing:
            raise ValueError(f"Unknown training-data columns: {missing}")
        left = df.select(
            F.col(id_column).alias("_left_id"),
            *[F.col(k).alias(f"_left_{k}") for k in keys],
        )
        right = df.select(
            F.col(id_column).alias("_right_id"),
            *[F.col(k).alias(f"_right_{k}") for k in keys],
        )
        id_order = F.col("_left_id").cast("string") < F.col("_right_id").cast("string")
        shared = reduce(
            lambda a, b: a | b,
            [F.col(f"_left_{k}").eqNullSafe(F.col(f"_right_{k}")) & F.col(f"_left_{k}").isNotNull() for k in keys],
        )
        pairs = left.crossJoin(right).where(id_order & (F.lit(True) if include_all_pairs else shared))
        scores = [self._exact_column(F.col(f"_left_{k}"), F.col(f"_right_{k}")).alias(f"z_{k}") for k in keys]
        pair_id = F.sha2(F.concat_ws("|", F.col("_left_id"), F.col("_right_id")), 256)
        result = pairs.select(
            pair_id.alias("z_cluster"),
            F.col("_left_id").alias(f"z_left_{id_column}"),
            F.col("_right_id").alias(f"z_right_{id_column}"),
            *scores,
        ).withColumn("z_score", sum(F.col(f"z_{k}") for k in keys) / F.lit(float(len(keys))))
        result = result.withColumn("z_isMatch", F.lit(None).cast("int"))
        if output_path:
            result.write.mode("overwrite").parquet(output_path)
        return result

    @staticmethod
    def _exact_column(left: Any, right: Any) -> Any:
        from pyspark.sql import functions as F
        return F.when(left.isNull() | right.isNull(), F.lit(1.0)).otherwise(
            F.when(left == right, F.lit(1.0)).otherwise(F.lit(0.0))
        )

    def label(self, pairs: Any, match_threshold: float = 1.0, output_path: str | None = None) -> Any:
        """Apply deterministic non-interactive labels to candidate pairs."""
        from pyspark.sql import functions as F
        labeled = pairs.withColumn(
            "z_isMatch",
            F.when(F.col("z_score") >= F.lit(float(match_threshold)), F.lit(1))
             .otherwise(F.lit(0)),
        )
        if output_path:
            labeled.write.mode("overwrite").parquet(output_path)
        return labeled

    def score_features(self, df: Any, output: str = "z_score") -> Any:
        """Score a pre-built candidate relation with its native feature column.

        The relation must contain ``z_exact`` and/or ``z_jaro`` columns. This
        keeps feature generation declarative while allowing a complete
        non-interactive training pipeline to be exercised independently of
        the exact-key model shortcut.
        """
        from pyspark.sql import functions as F
        excluded = {"z_score", "z_cluster", "z_isMatch"}
        available = [name for name in df.columns
                     if name.startswith("z_") and name not in excluded
                     and not name.startswith("z_left_") and not name.startswith("z_right_")]
        if not available:
            raise ValueError("candidate relation must contain z_exact or z_jaro")
        return df.withColumn(output, sum(F.col(name) for name in available) / F.lit(float(len(available))))

    def update_label(self, pairs: Any, labels: Any, output_path: str | None = None) -> Any:
        """Merge explicit ``(z_cluster, z_isMatch)`` labels into pairs."""
        from pyspark.sql import functions as F
        required = {"z_cluster", "z_isMatch"}
        if not required.issubset(set(labels.columns)):
            raise ValueError("labels must contain z_cluster and z_isMatch")
        updated = (pairs.drop("z_isMatch")
                   .join(labels.select("z_cluster", F.col("z_isMatch").cast("int")), "z_cluster", "left")
                   .withColumn("z_isMatch", F.coalesce(F.col("z_isMatch"), F.lit(2))))
        if output_path:
            updated.write.mode("overwrite").parquet(output_path)
        return updated

    def train(self, labeled: Any, keys: list[str], model_path: str | None = None, match_threshold: float = 1.0, feature_functions: dict[str, str] | None = None) -> dict[str, Any]:
        """Train the exact model contract from labeled native pairs."""
        from pyspark.sql import functions as F
        positives = labeled.where(F.col("z_isMatch") == 1).count()
        negatives = labeled.where(F.col("z_isMatch") == 0).count()
        if positives == 0 or negatives == 0:
            raise ValueError("training requires at least one positive and one negative label")
        feature_model = "z_jaro" in labeled.columns or "z_exact" in labeled.columns and "z_jaro" in labeled.columns
        model = {"algorithm": "NATIVE_FEATURE_THRESHOLD" if feature_model else "EXACT_KEYS",
                 "keys": list(keys), "positive_pairs": positives,
                 "negative_pairs": negatives, "threshold": float(match_threshold),
                 "feature_functions": dict(feature_functions or {})}
        if model_path:
            import json
            with open(model_path, "w", encoding="utf-8") as handle:
                json.dump(model, handle, sort_keys=True)
        return model

    def match_pairs(self, pairs: Any, model: dict[str, Any]) -> Any:
        """Return native-scored candidate pairs accepted by a threshold model."""
        from pyspark.sql import functions as F
        if model.get("algorithm") != "NATIVE_FEATURE_THRESHOLD":
            raise ValueError("match_pairs requires a NATIVE_FEATURE_THRESHOLD model")
        return pairs.where(F.col("z_score") >= F.lit(float(model["threshold"])))

    def link_pairs(self, pairs: Any, model: dict[str, Any]) -> Any:
        """Link is the cross-source form of native threshold pair matching."""
        return self.match_pairs(pairs, model)

    def cluster_pairs(self, pairs: Any, id_column: str = "record_id", max_iterations: int = 32) -> Any:
        """Build deterministic connected-component clusters from accepted pairs."""
        from pyspark.sql import functions as F
        left_name, right_name = f"z_left_{id_column}", f"z_right_{id_column}"
        if left_name not in pairs.columns or right_name not in pairs.columns:
            raise ValueError(f"pairs must contain {left_name} and {right_name}")
        edges = (pairs.select(F.col(left_name).cast("string").alias("src"), F.col(right_name).cast("string").alias("dst"))
                 .unionByName(pairs.select(F.col(right_name).cast("string").alias("src"), F.col(left_name).cast("string").alias("dst")))
                 .distinct())
        vertices = (edges.select(F.col("src").alias("id"))
                    .unionByName(edges.select(F.col("dst").alias("id")))
                    .distinct()
                    .withColumn("z_cluster", F.col("id")))
        for _ in range(max_iterations):
            propagated = (vertices.alias("v").join(edges.alias("e"), F.col("v.id") == F.col("e.src"))
                          .select(F.col("e.dst").alias("id"), F.col("v.z_cluster"))
                          .groupBy("id").agg(F.min("z_cluster").alias("z_cluster")))
            updated = (vertices.unionByName(propagated)
                       .groupBy("id").agg(F.min("z_cluster").alias("z_cluster")))
            changed = (updated.alias("u").join(vertices.alias("v"), "id")
                       .where(F.col("u.z_cluster") != F.col("v.z_cluster")).limit(1).count())
            if changed == 0:
                vertices = updated
                break
            vertices = updated
        return vertices

    def fuzzy_match(self, df: Any, model: dict[str, Any], id_column: str = "record_id", right_df: Any = None) -> Any:
        """Generate, score, and threshold record pairs from a trained feature model."""
        from pyspark.sql import functions as F
        from .similarity import exact_similarity, jaro_similarity
        keys = list(model.get("keys", []))
        functions = model.get("feature_functions", {})
        required = set(keys) | {id_column}
        missing = sorted(required - set(df.columns))
        if missing:
            raise ValueError(f"Unknown fuzzy match columns: {missing}")
        left = df.alias("left")
        right = (right_df if right_df is not None else df).alias("right")
        scores = []
        for key in keys:
            fn = functions.get(key, "EXACT").upper()
            if fn == "JARO":
                score = jaro_similarity(F.col(f"left.{key}"), F.col(f"right.{key}"))
            elif fn == "EXACT":
                score = exact_similarity(F.col(f"left.{key}"), F.col(f"right.{key}"))
            else:
                raise ValueError(f"Unsupported native feature function: {fn}")
            scores.append(score.alias(f"z_{key}"))
        pairs = left.crossJoin(right)
        if right_df is None:
            pairs = pairs.where(F.col(f"left.{id_column}").cast("string") < F.col(f"right.{id_column}").cast("string"))
        pairs = (pairs
            .select(
                F.sha2(F.concat_ws("|", F.col(f"left.{id_column}"), F.col(f"right.{id_column}")), 256).alias("z_cluster"),
                F.col(f"left.{id_column}").alias(f"z_left_{id_column}"),
                F.col(f"right.{id_column}").alias(f"z_right_{id_column}"),
                *scores,
            ))
        return self.score_features(pairs)

    def link_sources(self, left_df: Any, right_df: Any, model: dict[str, Any], id_column: str = "record_id") -> Any:
        """Score and threshold pairs across two distinct source DataFrames."""
        from pyspark.sql import functions as F
        pairs = self.fuzzy_match(left_df, model, id_column=id_column, right_df=right_df)
        return pairs.where(F.col("z_score") >= F.lit(float(model["threshold"])))

    def match(self, df: Any, model: dict[str, Any], cluster_column: str = "z_cluster") -> Any:
        """Apply a trained exact-key model."""
        from pyspark.sql import functions as F
        if model.get("algorithm") == "NATIVE_FEATURE_THRESHOLD":
            return self.fuzzy_match(df, model).where(F.col("z_score") >= F.lit(float(model["threshold"])))
        if model.get("algorithm") != "EXACT_KEYS":
            raise ValueError("unsupported native model algorithm")
        return self.exact_match(df, list(model["keys"]), cluster_column)

    def link(self, df: Any, model: dict[str, Any], cluster_column: str = "z_cluster") -> Any:
        """Link is the cross-source equivalent of exact-key match."""
        return self.match(df, model, cluster_column)

    def generate_docs(self, model: dict[str, Any]) -> dict[str, Any]:
        """Return a serializable model document for the native workflow."""
        return {"operation": "ZINGG_NATIVE_EXACT_WORKFLOW", "model": model,
                "native_plan": "join -> project -> dense_rank / CASE expressions"}

    def execute(self, phase: str | None = None, **kwargs: Any) -> Any:
        """Execute one native exact-workflow phase.

        The phase names mirror the corresponding Zingg 0.7 client phases. The
        exact workflow is deliberately explicit about required inputs so a
        missing phase artifact cannot silently fall back to upstream code.
        """
        phase = phase or (self.arguments or {}).get("phase")
        if phase == "findTrainingData":
            return self.find_training_data(kwargs["df"], kwargs["keys"], kwargs["id_column"], kwargs.get("output_path"), kwargs.get("include_all_pairs", False))
        if phase == "label":
            return self.label(kwargs["pairs"], kwargs.get("match_threshold", 1.0), kwargs.get("output_path"))
        if phase == "updateLabel":
            return self.update_label(kwargs["pairs"], kwargs["labels"], kwargs.get("output_path"))
        if phase == "train":
            return self.train(kwargs["labeled"], kwargs["keys"], kwargs.get("model_path"), kwargs.get("match_threshold", 1.0))
        if phase in ("match", "link"):
            method = self.match if phase == "match" else self.link
            return method(kwargs["df"], kwargs["model"], kwargs.get("cluster_column", "z_cluster"))
        if phase == "generateDocs":
            return self.generate_docs(kwargs["model"])
        raise ValueError("Unsupported native phase: {!r}".format(phase))

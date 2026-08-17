"""Classic/Py4J transport for the shared Scala core."""

from typing import Any

from ..errors import BackendUnavailableError


class ClassicBackend:
    name = "classic-py4j"

    def __init__(self, spark: Any):
        self.spark = spark
        try:
            gateway = spark._jvm.ai.zingg.native.gateway.ClassicGateway()
            self._gateway = gateway
        except Exception as exc:
            raise BackendUnavailableError(
                "The shared zingg-native Scala core is not loaded in this Spark JVM; "
                "install the core JAR before using backend='classic'."
            ) from exc
        if gateway.protocolVersion() != "1":
            raise BackendUnavailableError(f"Unsupported zingg-native protocol: {gateway.protocolVersion()}")

    def transform(self, df: Any, operation: str, **options: Any) -> Any:
        if operation not in {"EXACT_SIMILARITY", "JACCARD_SIMILARITY", "JARO_SIMILARITY"}:
            raise NotImplementedError(f"Classic shared core does not certify operation {operation}")
        jdf = self._gateway.transform(
            df._jdf,
            operation,
            options["left"],
            options["right"],
            options.get("output", "z_score"),
        )
        from pyspark.sql import DataFrame
        return DataFrame(jdf, self.spark)

    def capabilities(self) -> dict[str, Any]:
        return {
            "protocol_version": str(self._gateway.protocolVersion()),
            "metadata": self._gateway.capabilityMetadata(),
            "operations": list(self._gateway.supportedOperations()),
            "phases": list(self._gateway.supportedPhases()),
            "model_artifact_schema_version": int(self._gateway.modelArtifactSchemaVersion()),
            "blocking_tree_artifact_schema_version": int(self._gateway.blockingTreeArtifactSchemaVersion()),
        }

    def find_training_data(self, df: Any, keys: list[str], id_column: str, output_path: str | None = None) -> Any:
        if not keys:
            raise ValueError("keys must contain at least one column")
        java_keys = self.spark._jvm.java.util.ArrayList()
        for key in keys:
            java_keys.add(key)
        jdf = self._gateway.findTrainingData(df._jdf, id_column, java_keys)
        if output_path:
            jdf = self._gateway.persist(jdf, output_path)
        from pyspark.sql import DataFrame
        return DataFrame(jdf, self.spark)

    def preprocess(self, df: Any, operation: str, columns: list[str]) -> Any:
        if not columns:
            raise ValueError("columns must contain at least one field")
        if operation not in {"CASE_NORMALIZE", "TRIM"}:
            raise NotImplementedError(
                f"Classic shared core does not certify preprocessing operation {operation}"
            )
        java_columns = self.spark._jvm.java.util.ArrayList()
        for column in columns:
            java_columns.add(column)
        jdf = self._gateway.preprocess(df._jdf, operation, java_columns)
        from pyspark.sql import DataFrame
        return DataFrame(jdf, self.spark)

    def label(self, df: Any, threshold: float, output_path: str | None = None) -> Any:
        jdf = self._gateway.label(df._jdf, float(threshold))
        if output_path:
            jdf = self._gateway.persist(jdf, output_path)
        from pyspark.sql import DataFrame
        return DataFrame(jdf, self.spark)

    def build_training_pairs(self, df: Any, id_column: str) -> Any:
        jdf = self._gateway.buildTrainingPairs(df._jdf, id_column)
        from pyspark.sql import DataFrame
        return DataFrame(jdf, self.spark)

    def update_label(self, pairs: Any, labels: Any, output_path: str | None = None) -> Any:
        jdf = self._gateway.updateLabel(pairs._jdf, labels._jdf)
        if output_path:
            jdf = self._gateway.persist(jdf, output_path)
        from pyspark.sql import DataFrame
        return DataFrame(jdf, self.spark)

    def inspect_training_evidence(self, df: Any) -> dict[str, Any]:
        counts = self._gateway.inspectTrainingEvidence(df._jdf)
        positive, negative = int(counts[0]), int(counts[1])
        return {
            "positive_pairs": positive,
            "negative_pairs": negative,
            "sufficient": positive >= 5 and negative >= 5,
        }

    def fit_experimental_model(
        self,
        df: Any,
        feature_columns: list[str],
        model_path: str,
        model_checksum: str,
        blocking_tree_path: str,
        blocking_tree_checksum: str,
    ) -> dict[str, Any]:
        java_columns = self.spark._jvm.java.util.ArrayList()
        for column in feature_columns:
            java_columns.add(column)
        import json
        return json.loads(self._gateway.fitExperimentalModel(
            df._jdf, java_columns, model_path, model_checksum,
            blocking_tree_path, blocking_tree_checksum,
        ))

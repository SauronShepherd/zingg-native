package ai.zingg.native

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.apache.spark.sql.functions.{array, array_intersect, array_union, col, concat, element_at, floor, greatest, least, length, lit, lower, regexp_extract_all, sequence, size, struct, transform, trim, when, zip_with, aggregate}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}

sealed trait NativeMode
object NativeMode { case object SAFE extends NativeMode; case object EXPERIMENTAL extends NativeMode }
final case class RuntimeDescriptor(sparkVersion: String, scalaVersion: String, photon: Option[Boolean] = None)
final case class NativeContext(spark: SparkSession, mode: NativeMode, runtime: RuntimeDescriptor)
final case class OperationMetadata(id: String, semanticStatus: String, nativeStatus: String)

trait NativeSimilarity { def id: String; def apply(left: Column, right: Column, context: NativeContext): Column }

object ExactSimilarity extends NativeSimilarity {
  val id = "EXACT_SIMILARITY"
  def apply(left: Column, right: Column, context: NativeContext): Column =
    when(left.isNull || right.isNull, lit(1.0)).when(left === right, lit(1.0)).otherwise(lit(0.0))
}

object JaccardSimilarity extends NativeSimilarity {
  val id = "JACCARD_SIMILARITY"
  def apply(left: Column, right: Column, context: NativeContext): Column = {
    val empty = left.isNull || right.isNull || left === lit("") || right === lit("")
    val l = regexp_extract_all(lower(left.cast("string")), lit("[\\p{L}\\p{N}]+"), lit(0))
    val r = regexp_extract_all(lower(right.cast("string")), lit("[\\p{L}\\p{N}]+"), lit(0))
    when(empty, lit(1.0)).otherwise(size(array_intersect(l, r)).cast("double") / size(array_union(l, r)))
  }
}

/** SecondString Jaro semantics used by Zingg 0.7 (not Jaro-Winkler). */
object JaroSimilarity extends NativeSimilarity {
  val id = "JARO_SIMILARITY"
  def apply(left: Column, right: Column, context: NativeContext): Column = {
    val first = lower(left.cast("string"))
    val second = lower(right.cast("string"))
    val n = length(first)
    val m = length(second)
    val chars1 = org.apache.spark.sql.functions.split(first, "")
    val chars2 = org.apache.spark.sql.functions.split(second, "")
    val half = floor(least(n, m) / lit(2)).cast("int") + lit(1)

    def common(source: Column, target: Column, sourceLen: Column): Column = {
      val indexes = sequence(lit(0), greatest(sourceLen - lit(1), lit(0)))
      val initial = struct(array().cast("array<string>").alias("common"), target.alias("remaining"))
      val result = aggregate(indexes, initial, (state: Column, i: Column) => {
        val start = greatest(lit(0), i - half)
        val stop = least(size(state.getField("remaining")) - lit(1), i + half - lit(1))
        val candidates = sequence(start, greatest(start, stop))
        val found = aggregate(candidates, struct(lit(false).alias("matched"), lit(-1).alias("index")),
          (hit: Column, j: Column) => when(hit.getField("matched"), hit).otherwise(
            when(element_at(source, i + lit(1)) === element_at(state.getField("remaining"), j + lit(1)),
              struct(lit(true).alias("matched"), j.alias("index"))).otherwise(hit)))
        val marked = transform(state.getField("remaining"), (value: Column, j: Column) =>
          when(j === found.getField("index"), lit("*")).otherwise(value))
        struct(
          when(found.getField("matched"), concat(state.getField("common"), array(element_at(source, i + lit(1)))))
            .otherwise(state.getField("common")).alias("common"),
          when(found.getField("matched"), marked).otherwise(state.getField("remaining")).alias("remaining"))
      })
      result.getField("common")
    }
    val common1 = common(chars1, chars2, n)
    val common2 = common(chars2, chars1, m)
    val commonCount = size(common1)
    val mismatches = aggregate(zip_with(common1, common2, (a: Column, b: Column) => (a =!= b).cast("int")), lit(0),
      (total: Column, mismatch: Column) => total + org.apache.spark.sql.functions.coalesce(mismatch, lit(0)))
    val score = ((commonCount / n) + (commonCount / m) + ((commonCount - floor(mismatches / lit(2))) / commonCount)) / lit(3.0)
    when(left.isNull || right.isNull || length(left) === lit(0) || length(right) === lit(0), lit(1.0))
      .otherwise(when(commonCount === lit(0) || n === lit(0) || m === lit(0), lit(0.0)).otherwise(score))
  }
}

object SimilarityRegistry {
  private val all = Map[String, NativeSimilarity](ExactSimilarity.id -> ExactSimilarity, JaccardSimilarity.id -> JaccardSimilarity, JaroSimilarity.id -> JaroSimilarity)
  def metadata: Seq[OperationMetadata] = Seq(OperationMetadata(ExactSimilarity.id, "certified", "standard-expression"), OperationMetadata(JaccardSimilarity.id, "certified", "standard-expression"), OperationMetadata(JaroSimilarity.id, "certified", "standard-expression-fallback"))
  def resolve(id: String, mode: NativeMode): NativeSimilarity = all.getOrElse(id, throw new IllegalArgumentException(s"Unknown or unavailable operation: $id"))
}

object Core {
  val libraryVersion = "0.2.0-SNAPSHOT"
  val protocolVersion = "1"
  def transform(df: DataFrame, operationId: String, left: String, right: String, output: String): DataFrame = {
    val ctx = NativeContext(df.sparkSession, NativeMode.SAFE, RuntimeDescriptor(df.sparkSession.version, "2.13"))
    df.withColumn(output, SimilarityRegistry.resolve(operationId, NativeMode.SAFE)(col(left), col(right), ctx))
  }

  /** Execute a registered public-expression rewrite with an explicit policy. */
  def rewrite(
      df: DataFrame,
      operationId: String,
      left: String,
      right: Option[String],
      output: String,
      context: RewriteContext,
      registry: RewriteRegistry = NativeRewriteRegistry.default): DataFrame = {
    val operation = NativeOperation.resolve(operationId)
    val rule = registry.resolve(operation)
    val rightColumn = right.map(name => col(name))
    val value = rule(col(left), rightColumn, context)
    if (context.mode == NativeExecutionMode.OFF) df else df.withColumn(output, value)
  }

  /** Apply upstream-compatible declarative string preprocessing to selected fields. */
  def preprocess(df: DataFrame, operationId: String, columns: Seq[String]): DataFrame = {
    require(columns.nonEmpty, "columns must contain at least one field")
    val missing = columns.distinct.filterNot(df.columns.contains)
    require(missing.isEmpty, s"Unknown preprocessing columns: ${missing.mkString(", ")}")
    operationId match {
      case "TRIM" => columns.foldLeft(df)((current, field) => current.withColumn(field, trim(col(field))))
      case "CASE_NORMALIZE" => columns.foldLeft(df)((current, field) => current.withColumn(field, lower(col(field).cast(StringType))))
      case other => throw new IllegalArgumentException(s"Unknown preprocessing operation: $other")
    }
  }

  /** Declarative exact-key candidate generation for the first native phase. */
  def findTrainingData(df: DataFrame, idColumn: String, keys: Seq[String]): DataFrame = {
    require(keys.nonEmpty, "keys must contain at least one column")
    val missing = (keys :+ idColumn).distinct.filterNot(df.columns.contains)
    require(missing.isEmpty, s"Unknown training-data columns: ${missing.mkString(", ")}")
    val left = df.select((Seq(col(idColumn).alias("_left_id")) ++ keys.map(k => col(k).alias(s"_left_$k"))): _*).alias("left")
    val right = df.select((Seq(col(idColumn).alias("_right_id")) ++ keys.map(k => col(k).alias(s"_right_$k"))): _*).alias("right")
    val shared = keys.map { k =>
      val l = col(s"left._left_$k")
      val r = col(s"right._right_$k")
      l <=> r && l.isNotNull
    }.reduce(_ || _)
    val ordered = col("left._left_id").cast("string") < col("right._right_id").cast("string")
    val ctx = NativeContext(df.sparkSession, NativeMode.SAFE, RuntimeDescriptor(df.sparkSession.version, "2.13"))
    val scores = keys.map(k => ExactSimilarity(col(s"left._left_$k"), col(s"right._right_$k"), ctx).alias(s"z_$k"))
    val pairId = org.apache.spark.sql.functions.sha2(org.apache.spark.sql.functions.concat_ws("|", col("left._left_id"), col("right._right_id")), 256)
    left.join(right, ordered && shared)
      .select((Seq(pairId.alias("z_cluster"), col("left._left_id").alias(s"z_left_$idColumn"), col("right._right_id").alias(s"z_right_$idColumn")) ++ scores): _*)
      .withColumn("z_score", keys.map(k => col(s"z_$k")).reduce(_ + _) / lit(keys.size.toDouble))
      .withColumn("z_isMatch", lit(null).cast("int"))
  }

  /**
    * Expand record-level training labels into labeled pairs by cluster.
    * This is the declarative self-join prerequisite used by the upstream
    * trainer; model fitting and blocking-tree learning remain separate gates.
    */
  def buildTrainingPairs(df: DataFrame, idColumn: String): DataFrame = {
    val required = Set(idColumn, "z_cluster", "z_isMatch")
    val missing = required.filterNot(df.columns.contains)
    require(missing.isEmpty, s"training relation is missing columns: ${missing.mkString(", ")}")
    val left = df.select(
      col(idColumn).cast("string").alias("_left_id"),
      col("z_cluster").alias("_left_cluster"),
      col("z_isMatch").cast("int").alias("z_isMatch")
    ).alias("left")
    val right = df.select(
      col(idColumn).cast("string").alias("_right_id"),
      col("z_cluster").alias("_right_cluster"),
      col("z_isMatch").cast("int").alias("z_isMatch")
    ).alias("right")
    left.join(right, col("left._left_cluster") <=> col("right._right_cluster") &&
      col("left._left_id") < col("right._right_id"))
      .filter(col("left.z_isMatch").isin(0, 1) && col("right.z_isMatch").isin(0, 1))
      .select(
        org.apache.spark.sql.functions.sha2(org.apache.spark.sql.functions.concat_ws("|", col("left._left_id"), col("right._right_id")), 256).alias("z_cluster"),
        col("left._left_id").alias(s"z_left_$idColumn"),
        col("right._right_id").alias(s"z_right_$idColumn"),
        col("left.z_isMatch").alias("z_isMatch")
      )
  }

  /** Apply a deterministic non-interactive label to a candidate relation. */
  def label(df: DataFrame, threshold: Double): DataFrame = {
    require(df.columns.contains("z_score"), "candidate relation must contain z_score")
    df.withColumn("z_isMatch", org.apache.spark.sql.functions.when(col("z_score") >= lit(threshold), lit(1)).otherwise(lit(0)))
  }

  /** Merge explicit labels into a candidate relation without collecting rows. */
  def updateLabel(pairs: DataFrame, labels: DataFrame): DataFrame = {
    require(pairs.columns.contains("z_cluster"), "candidate relation must contain z_cluster")
    require(Set("z_cluster", "z_isMatch").subsetOf(labels.columns.toSet),
      "labels must contain z_cluster and z_isMatch")
    pairs.drop("z_isMatch")
      .join(labels.select(col("z_cluster"), col("z_isMatch").cast("int")), Seq("z_cluster"), "left")
      .withColumn("z_isMatch", org.apache.spark.sql.functions.coalesce(col("z_isMatch"), lit(2)))
  }

  /** Inspect persisted labels before a future native model fit. */
  def inspectTrainingEvidence(df: DataFrame): TrainingEvidence = {
    require(df.columns.contains("z_isMatch"), "training relation must contain z_isMatch")
    TrainingEvidence(
      df.filter(col("z_isMatch") === lit(1)).count(),
      df.filter(col("z_isMatch") === lit(0)).count()
    )
  }

  /** Fit the upstream-compatible binary model through public Spark ML APIs. */
  def trainModel(df: DataFrame, featureColumns: Seq[String], labelColumn: String = "z_isMatch",
                 modelPath: Option[String] = None): LogisticRegressionModel = {
    require(featureColumns.nonEmpty, "featureColumns must not be empty")
    val missing = (featureColumns :+ labelColumn).distinct.filterNot(df.columns.contains)
    require(missing.isEmpty, s"model training relation is missing columns: ${missing.mkString(", ")}")
    val assembler = new VectorAssembler().setInputCols(featureColumns.toArray).setOutputCol("z_features")
    val assembled = assembler.transform(df).withColumn("z_label", col(labelColumn).cast("double"))
    val model = new LogisticRegression().setFeaturesCol("z_features").setLabelCol("z_label").fit(assembled)
    modelPath.foreach(model.write.overwrite().save)
    model
  }

  /** Load a persisted Spark ML model without requiring driver-side Python state. */
  def loadModel(modelPath: String): LogisticRegressionModel = {
    require(modelPath != null && modelPath.nonEmpty, "modelPath must not be empty")
    LogisticRegressionModel.load(modelPath)
  }

  /** Score candidate rows with a persisted or in-memory public Spark ML model. */
  def matchModel(df: DataFrame, model: LogisticRegressionModel, featureColumns: Seq[String]): DataFrame = {
    require(featureColumns.nonEmpty, "featureColumns must not be empty")
    val assembler = new VectorAssembler().setInputCols(featureColumns.toArray).setOutputCol("z_features")
    model.transform(assembler.transform(df))
      .withColumn("z_score", col("probability").getItem(1))
  }

  /** Native DataFrame connected components for the final link phase. */
  def linkComponents(edges: DataFrame, leftColumn: String, rightColumn: String,
                     maxIterations: Int = 32): DataFrame = {
    require(maxIterations > 0, "maxIterations must be positive")
    val missing = Seq(leftColumn, rightColumn).distinct.filterNot(edges.columns.contains)
    require(missing.isEmpty, s"link relation is missing columns: ${missing.mkString(", ")}")
    val undirected = edges.select(col(leftColumn).cast("string").alias("src"), col(rightColumn).cast("string").alias("dst"))
      .unionByName(edges.select(col(rightColumn).cast("string").alias("src"), col(leftColumn).cast("string").alias("dst")))
      .distinct()
    var vertices = undirected.select(col("src").alias("id")).unionByName(undirected.select(col("dst").alias("id")))
      .distinct().withColumn("z_cluster", col("id"))
    var iteration = 0
    var changed = true
    while (iteration < maxIterations && changed) {
      val propagated = vertices.join(undirected, vertices("id") === undirected("src"))
        .select(undirected("dst").alias("id"), vertices("z_cluster"))
        .groupBy("id").agg(org.apache.spark.sql.functions.min("z_cluster").alias("z_cluster"))
      val updated = vertices.unionByName(propagated).groupBy("id").agg(org.apache.spark.sql.functions.min("z_cluster").alias("z_cluster"))
      changed = updated.join(vertices, Seq("id")).where(updated("z_cluster") =!= vertices("z_cluster")).limit(1).count() > 0
      vertices = updated
      iteration += 1
    }
    vertices
  }

  /** Persist a phase relation without collecting it on the driver. */
  def persist(df: DataFrame, outputPath: String): DataFrame = {
    df.write.mode("overwrite").parquet(ArtifactSchema.validatePath(outputPath))
    df
  }
}

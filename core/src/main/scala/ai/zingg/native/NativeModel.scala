package ai.zingg.native

import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Public-DataFrame replacement for the Spark ML portion of Zingg 0.7 SparkModel.
 *
 * Zingg itself still owns feature creation and phase orchestration.  This class
 * replaces only the VectorAssembler -> PolynomialExpansion(3) ->
 * LogisticRegression -> CrossValidator boundary, which cannot be used as the
 * production Serverless Scala-Connect training path.
 *
 * The optimizer deliberately uses only ordinary Spark SQL expressions and
 * aggregate actions.  No Estimator, RDD, Dataset.map, UDF, SparkContext or
 * Catalyst API is required.  The same code therefore works through Classic
 * Spark and Spark Connect and leaves the resulting relational plans eligible
 * for Databricks native execution.
 */
final case class NativeTrainedModel(
    schemaVersion: Int,
    featureColumns: Vector[String],
    polynomialDegree: Int,
    polynomialOrdering: String,
    coefficients: Vector[Double],
    intercept: Double,
    regParam: Double,
    threshold: Double,
    maxIter: Int,
    numFolds: Int,
    seed: Long,
    optimizer: String)

object NativeModelEngine {
  val SchemaVersion = 1
  val PolynomialDegree = 3
  val PolynomialOrdering = "spark-polynomial-expansion-order-v1"
  val MaxIter = 100
  val NumFolds = 2
  val Seed = 13L
  val Optimizer = "lbfgs-public-sql-v1"
  private val RegGrid = Vector(0.0001d, 0.001d, 0.01d, 0.1d, 1.0d)
  private val ThresholdGrid = Vector(0.40d, 0.45d, 0.50d, 0.55d)
  /** Optional bounded Databricks probe override; production default remains 100. */
  private def effectiveMaxIter: Int = sys.props.get("zingg.native.model.maxIter").flatMap(v => scala.util.Try(v.toInt).toOption).filter(_ > 0).getOrElse(MaxIter)
  private def quickProbe: Boolean = sys.props.get("zingg.native.model.quickProbe").exists(_.equalsIgnoreCase("true"))
  // Retain degree-3/full-term shape while bounding CV work for a diagnostic
  // persistence probe. This flag is never set by ordinary Zingg execution.
  private def boundedProbe: Boolean = sys.props.get("zingg.native.model.boundedProbe").exists(_.equalsIgnoreCase("true"))
  // Production-iteration parity probes retain the real optimizer iteration
  // count while reducing CV fan-out to one fold and the first reg value. This
  // isolates optimizer convergence against the Spark ML oracle.
  private def parityProbe: Boolean = sys.props.get("zingg.native.model.parityProbe").exists(_.equalsIgnoreCase("true"))
  // Quick probes are explicitly diagnostic and may use a linear model to
  // exercise the complete feature-column boundary without constructing the
  // production 1,770-term degree-3 Connect plan. Normal STRICT training keeps
  // the upstream-compatible degree-3 contract.
  private def effectivePolynomialDegree: Int = if (quickProbe) 1 else PolynomialDegree
  private def effectiveNumFolds: Int = if (quickProbe || boundedProbe || parityProbe) 1 else NumFolds
  private def effectiveRegGrid: Vector[Double] = if (quickProbe || boundedProbe || parityProbe) Vector(RegGrid.head) else RegGrid
  val NativeArtifactDirectory = "_zingg_native_model_v1"

  private def nativePath(path: String): String =
    Option(path).map(_.stripSuffix("/")).getOrElse("") + "/" + NativeArtifactDirectory

  /**
   * Deterministic degree-3 polynomial expansion.  Ordering is versioned in the
   * artifact because coefficient interpretation depends on it.
   */
  private[native] def polynomialTerms(base: Seq[Column], degree: Int = PolynomialDegree): Vector[Column] = {
    require(degree >= 1 && degree <= 3, s"Native Zingg model supports polynomial degree 1..3, got $degree")

    // Mirror Spark PolynomialExpansion's feature-prefix ordering. For two
    // features (x,y), degree three is:
    // (x, x*x, x*x*x, y, x*y, x*x*y, y*y, x*y*y, y*y*y).
    // Every term is represented by a nondecreasing index path; paths are
    // ordered by their maximum feature index and then by degree.
    polynomialIndexPaths(base.size, degree).map { path =>
      path.foldLeft(lit(1.0d)) { case (product, index) => product * base(index) }
    }
  }

  private def baseFeatures(df: DataFrame, names: Seq[String]): Vector[Column] =
    names.toVector.map(n => coalesce(df.col(n).cast("double"), lit(0.0d)))

  private def expanded(df: DataFrame, names: Seq[String], degree: Int = PolynomialDegree): Vector[Column] =
    polynomialTerms(baseFeatures(df, names), degree)

  /**
   * The same Spark PolynomialExpansion ordering expressed as index paths.
   * Keeping the paths as data lets the materialized Serverless path evaluate
   * the expansion with one public higher-order SQL expression instead of
   * serializing a 1,770-node arithmetic tree through Spark Connect.
   */
  private[native] def polynomialIndexPaths(size: Int, degree: Int): Vector[Vector[Int]] = {
    require(size > 0 && degree >= 1 && degree <= 3)
    def pathsForFeatures(featureCount: Int, maximumDegree: Int): Vector[Vector[Int]] =
      if (featureCount == 0 || maximumDegree == 0) Vector.empty
      else (0 until featureCount).toVector.flatMap { current =>
        (1 to maximumDegree).toVector.flatMap { power =>
          val currentPower = Vector.fill(power)(current)
          Vector(currentPower) ++
            pathsForFeatures(current, maximumDegree - power).map(_ ++ currentPower)
        }
      }

    pathsForFeatures(size, degree)
  }

  private def expandedArray(base: Seq[Column], degree: Int): Column = {
    val paths = polynomialIndexPaths(base.size, degree)
    termArrayForPaths(base, paths)
  }

  private def termArrayForPaths(base: Seq[Column], paths: Seq[Vector[Int]]): Column = {
    val baseArray = array(base: _*)
    // Keep the exact path ordering, but split the higher-order expression
    // into bounded chunks. A single 1,770-element transform creates an
    // oversized managed Spark Connect plan on Serverless; flattening these
    // public arrays preserves the relational representation and ordering.
    val chunks = paths.grouped(128).map { chunk =>
      val pathArray = array(chunk.map(path => array(path.map(i => lit(i + 1)): _*)): _*)
      transform(pathArray, path =>
        aggregate(path, lit(1.0d), (product, index) =>
          product * element_at(baseArray, index.cast("int"))))
    }.toVector
    flatten(array(chunks.toSeq: _*))
  }

  private def linearMargin(terms: Seq[Column], coefficients: Seq[Double], intercept: Double): Column = {
    require(terms.length == coefficients.length,
      s"Model coefficient count ${coefficients.length} does not match expanded feature count ${terms.length}")
    // Use ordinary public Column arithmetic. Callers bound this operation to
    // small partial-term batches for Serverless so the plan never contains the
    // complete 1,770-term margin as one remote action.
    terms.zip(coefficients).grouped(256).map { batch =>
      batch.foldLeft(lit(0.0d)) {
        case (acc, (term, weight)) => acc + term * lit(weight)
      }
    }.foldLeft(lit(intercept))(_ + _)
  }

  /** Evaluate a materialized term array without expanding one Column per term. */
  private def arrayLinearMargin(termArray: Column, coefficients: Seq[Double], intercept: Double): Column = {
    val coefficientArray = array(coefficients.map(lit): _*)
    aggregate(
      zip_with(termArray, coefficientArray, (term, coefficient) => term * coefficient),
      lit(intercept),
      (accumulator, value) => accumulator + value)
  }

  private def probability(margin: Column): Column =
    lit(1.0d) / (lit(1.0d) + exp(-margin))

  private def finiteDouble(row: Row, index: Int): Double = {
    if (row.isNullAt(index)) 0.0d
    else finiteValue(row.get(index))
  }

  private def finiteValue(value: Any): Double = value match {
      case n: java.lang.Number =>
        val d = n.doubleValue()
        if (d.isNaN || d.isInfinity) 0.0d else d
      case value =>
        val d = value.toString.toDouble
        if (d.isNaN || d.isInfinity) 0.0d else d
  }

  private def finiteArray(row: Row, index: Int, length: Int): Vector[Double] = {
    if (row.isNullAt(index)) Vector.fill(length)(0.0d)
    else row.getSeq[Any](index).toVector.map(finiteValue).padTo(length, 0.0d).take(length)
  }

  /**
   * Fit the same logical binary classifier Zingg expects, using public Spark
   * aggregates instead of Spark ML distributed training.  The objective is
   * logistic loss with L2 regularization and an unregularized intercept.
   */
  private def fitLogistic(
      input: DataFrame,
      terms: Vector[Column],
      baseNames: Vector[String],
      labelColumn: String,
      regParam: Double,
      maxIter: Int,
      polynomialDegree: Int,
      context: RewriteContext): (Vector[Double], Double) = {
    // A single wide aggregate returns one column per polynomial term. That
    // shape made the managed Serverless Scala kernel unresponsive on the
    // 1,770-term production case. The gradient below therefore converts the
    // term vector to indexed rows and performs one public relational aggregate
    // per optimizer iteration, preserving exact ordering without a wide
    // result schema.
    // Keep the bounded Serverless probe (8 base features / 164 terms) on the
    // direct public-expression path. Materializing that small plan costs more
    // than it saves; reserve the persisted-array path for larger production
    // expansions.
    // The launcher materializes base similarity columns, not an array named
    // _native_terms. Keep this false unless a future, explicitly separate
    // term-array property is introduced; otherwise reread frames would be
    // incorrectly treated as if they contained the old term sidecar.
    val termArraySidecarRequested = sys.props.contains("zingg.native.model.termArrayPath")
    val useMaterializedTerms = (termArraySidecarRequested || sys.props.get("zingg.native.model.materializePath").exists(_.trim.nonEmpty)) && terms.length > 164
    // Keep the materialized-margin projection bounded; this batching is only
    // for assembling the margin frame and is separate from gradient
    // aggregation, which is row-oriented below.
    val marginBatchSize = if (quickProbe) math.max(1, terms.length) else 512
    // Match Spark ML's standardization contract before optimization.  The
    // previous implementation optimized raw polynomial terms, which is a
    // different numerical model for terms with very different scales.
    val featureStats = terms.grouped(128).flatMap { batch =>
      val aggregateColumns = batch.zipWithIndex.flatMap { case (term, index) =>
        Seq(avg(term).as(s"_native_mean_$index"), stddev_pop(term).as(s"_native_std_$index"))
      }.toArray
      val row = input.agg(aggregateColumns.head, aggregateColumns.tail: _*).head()
      batch.indices.map { index =>
        val mean = finiteDouble(row, index * 2)
        val standardDeviation = finiteDouble(row, index * 2 + 1)
        (mean, if (standardDeviation.isFinite && standardDeviation > 0.0d) standardDeviation else 1.0d)
      }
    }.toVector
    val standardizedTerms = terms.zip(featureStats).map { case (term, (mean, standardDeviation)) =>
      (term - lit(mean)) / lit(standardDeviation)
    }
    val featureMeans = array(featureStats.map(stat => lit(stat._1)): _*)
    val featureScales = array(featureStats.map(stat => lit(stat._2)): _*)
    def standardizedExpanded(base: Seq[Column]): Column = {
      val expanded = expandedArray(base, polynomialDegree)
      transform(
        expanded,
        (value, index) => (value - featureMeans.getItem(index)) / featureScales.getItem(index))
    }
    val labelSummary = input.agg(
      sum(input.col(labelColumn).cast("double")).as("_native_positive"),
      count(lit(1)).as("_native_rows")).head()
    val rowCount = if (labelSummary.isNullAt(1)) 0L else labelSummary.getLong(1)
    val positives = if (labelSummary.isNullAt(0)) 0.0d else finiteDouble(labelSummary, 0)
    val negatives = math.max(0.0d, rowCount.toDouble - positives)
    var weights = Vector.fill(terms.length)(0.0d)
    var intercept = math.log((positives + 1.0e-12d) / (negatives + 1.0e-12d))
    var iteration = 0
    var previousParameters: Option[Vector[Double]] = None
    var previousGradient: Option[Vector[Double]] = None
    var sHistory = Vector.empty[Vector[Double]]
    var yHistory = Vector.empty[Vector[Double]]
    var rhoHistory = Vector.empty[Double]
    val optimizerMemory = 10

    def dot(left: Vector[Double], right: Vector[Double]): Double =
      left.indices.map(index => left(index) * right(index)).sum

    def objective(frame: DataFrame, frameTerms: Vector[Column], candidateWeights: Vector[Double], candidateIntercept: Double): Double = {
      val margin = linearMargin(frameTerms, candidateWeights, candidateIntercept)
      val stableLogLoss = when(
        margin > lit(0.0d),
        margin + log(lit(1.0d) + exp(-margin)))
        .otherwise(log(lit(1.0d) + exp(margin)))
      val row = frame.withColumn("_native_objective_loss", stableLogLoss -
        col(labelColumn).cast("double") * margin)
        .agg(avg(col("_native_objective_loss")).as("_native_log_loss")).head()
      val dataLoss = finiteDouble(row, 0)
      dataLoss + 0.5d * regParam * candidateWeights.map(weight => weight * weight).sum
    }
    val useMaterializedMargin = sys.props.get("zingg.native.margin.materializePath")
      .exists(_.trim.nonEmpty) && terms.length > 164
    val marginBase = if (useMaterializedMargin) {
      val root = sys.props("zingg.native.margin.materializePath")
      val rowId = "_native_row_id"
      val basePath = s"${root.stripSuffix("/")}/base-${java.util.UUID.randomUUID().toString}"
      input
        .select((Seq(input.col(labelColumn)) ++ baseNames.map(input.col)): _*)
        .withColumn(rowId, monotonically_increasing_id())
        .write.mode("overwrite").parquet(basePath)
      Some(input.sparkSession.read.parquet(basePath))
    } else None

    // Polynomial terms depend only on the input features and are invariant
    // across optimizer iterations.  Materialize them once per fit; writing
    // the same 1,770-term array inside every iteration multiplies the remote
    // I/O boundary by maxIter without changing the model semantics.
    val materializedTermFrame = if (useMaterializedTerms) {
      sys.props.get("zingg.native.model.materializePath") match {
        case Some(root) =>
          val source = input.withColumn(
            "_native_terms",
            standardizedExpanded(baseNames.map(input.col)))
          val path = s"${root.stripSuffix("/")}/term-array-${java.util.UUID.randomUUID().toString}"
          source.select(col(labelColumn), col("_native_terms"))
            .write.mode("overwrite").parquet(path)
          Some(input.sparkSession.read.parquet(path))
        case None => None
      }
    } else None

    while (iteration < maxIter) {
      // The materialized-term path below does not consume `withMargin` at
      // all.  Do not construct the large margin/contribution plan in that
      // case: merely building that unused Connect plan on every iteration
      // causes managed Serverless analysis state to grow across CV fits.
      val withMargin = if (materializedTermFrame.isDefined) {
        input
      } else if (useMaterializedMargin) {
        marginBase match {
        case Some(base) =>
          val rowId = "_native_row_id"
          val marginName = "_native_margin"
          // The original terms belong to `input` (or to the pre-write
          // normalized frame). Rebuild/bind them to the reread frame before
          // constructing the contribution plans; Spark Connect rejects a
          // Column carrying an incompatible dataframe identity.
          val boundTerms = polynomialTerms(baseNames.map(base.col), effectivePolynomialDegree).zip(featureStats).map {
            case (term, (mean, standardDeviation)) => (term - lit(mean)) / lit(standardDeviation)
          }
          val contributionFrames = boundTerms.zip(weights).grouped(marginBatchSize).map { batch =>
            base.select(
              col(rowId),
              linearMargin(batch.map(_._1), batch.map(_._2), 0.0d).alias("_native_partial"))
          }.toVector
          val contributions = contributionFrames.tail.foldLeft(contributionFrames.head) {
            (left, right) => left.unionByName(right)
          }
          val margins = contributions.groupBy(col(rowId)).agg(
            (sum(col("_native_partial")) + lit(intercept)).alias(marginName))
          base.join(margins, Seq(rowId), "inner")
            .withColumn("_native_error",
            probability(col(marginName)) - col(labelColumn).cast("double"))
        case None => input.withColumn("_native_margin", linearMargin(standardizedTerms, weights, intercept))
          .withColumn("_native_error", probability(col("_native_margin")) - col(labelColumn).cast("double"))
        }
      } else {
        input.withColumn("_native_margin", linearMargin(standardizedTerms, weights, intercept))
          .withColumn("_native_error",
            probability(col("_native_margin")) - col(labelColumn).cast("double"))
      }
      // Keep each term as a scalar public Column.  The array expansion form
      // is semantically equivalent, but it creates a large higher-order
      // expression that managed Spark Connect can spend an unbounded amount
      // of time analyzing before the first aggregate.  Scalar projections
      // keep the plan relational and give Serverless a stable action boundary.
      val (gradientFrame, gradientTerms) = materializedTermFrame match {
        case Some(persisted) =>
          val persistedTerms = terms.indices.map(i => persisted.col("_native_terms").getItem(i)).toVector
          val scored = persisted.withColumn(
            "_native_error",
            probability(arrayLinearMargin(persisted.col("_native_terms"), weights, intercept)) - col(labelColumn).cast("double"))
          // The term array is already a narrow, remote Parquet boundary. Do
          // not write and reread another Parquet dataset for every optimizer
          // iteration: in CV this multiplies the workload by every fold and
          // regularization value, and can leave a managed Serverless task
          // waiting indefinitely on the second fit. Each iteration starts
          // from the same materialized frame and constructs a fresh public
          // expression plan, so the extra round trip provides no semantic or
          // lineage benefit.
          (scored, persistedTerms)
        case None if useMaterializedTerms =>
          val arrayFrame = withMargin.withColumn("_native_terms", standardizedExpanded(baseNames.map(withMargin.col)))
          (arrayFrame, terms.indices.map(i => arrayFrame.col("_native_terms").getItem(i)).toVector)
        case None =>
        // Name the terms before the action so the aggregate references only
        // columns in the final frame, rather than carrying Columns captured
        // from an earlier projection through Spark Connect.
        val named = standardizedTerms.zipWithIndex.foldLeft(withMargin) { case (frame, (term, index)) =>
          frame.withColumn(s"_native_term_$index", term)
        }
        val persisted = if (terms.length > 164) {
          named
        } else named
        (persisted, terms.indices.map(i => col(s"_native_term_$i")).toVector)
      }
      NativeDiagnostics.modelStage(context, "gradient-aggregate-start", s"iteration=$iteration terms=${gradientTerms.length}")
      val (gradients, interceptGradient) = materializedTermFrame match {
        case Some(_) =>
          // The persisted term array lets Serverless compute all term
          // gradients in one narrow aggregate without creating a 1,770-column
          // result or issuing seven separate actions per iteration.
          val contributions = transform(
            col("_native_terms"),
            term => term * col("_native_error"))
          val zero = array_repeat(lit(0.0d), gradientTerms.length)
          val summed = aggregate(
            collect_list(contributions),
            zero,
            (accumulator, values) => zip_with(
              accumulator,
              values,
              (left, right) => left + coalesce(right, lit(0.0d))))
          val row = gradientFrame.agg(
            summed.alias("_native_gradient_array"),
            avg(col("_native_error")).alias("_native_intercept_gradient"),
            count(lit(1)).alias("_native_gradient_rows")).head()
          val rows = if (row.isNullAt(2)) 0L else row.getLong(2)
          val divisor = if (rows == 0L) 1.0d else rows.toDouble
          (finiteArray(row, 0, gradientTerms.length).map(_ / divisor), finiteDouble(row, 1))
        case None =>
          // For small non-materialized probes, bounded scalar aggregates keep
          // the result narrow and preserve the exact source term ordering.
          val gradientBuilder = Vector.newBuilder[Double]
          var intercept = 0.0d
          gradientTerms.grouped(256).zipWithIndex.foreach { case (batch, batchIndex) =>
            val gradientColumns = batch.zipWithIndex.map { case (term, termIndex) =>
              avg(col("_native_error") * term).as(s"_native_gradient_${batchIndex}_$termIndex")
            }
            val aggregateColumns = (gradientColumns :+ avg(col("_native_error")).as("_native_intercept_gradient")).toArray
            val row = gradientFrame.agg(aggregateColumns.head, aggregateColumns.tail: _*).head()
            if (batchIndex == 0) intercept = finiteDouble(row, batch.length)
            batch.indices.foreach(index => gradientBuilder += finiteDouble(row, index))
          }
          (gradientBuilder.result(), intercept)
        }
      NativeDiagnostics.modelStage(context, "gradient-aggregate-complete", s"iteration=$iteration terms=${gradientTerms.length}")
      // L-BFGS is implemented in the driver only for its small parameter
      // vectors; all loss and gradient evaluation remains public Spark SQL.
      // This matches Spark ML's optimization contract much more closely than
      // the former decaying gradient step, while remaining Connect-safe.
      val gradient = gradients.zip(weights).map { case (value, weight) => value + regParam * weight } :+ interceptGradient
      val parameters = weights :+ intercept
      previousParameters.zip(previousGradient).foreach { case (oldParameters, oldGradient) =>
        val s = parameters.zip(oldParameters).map { case (current, previous) => current - previous }
        val y = gradient.zip(oldGradient).map { case (current, previous) => current - previous }
        val curvature = dot(y, s)
        if (curvature > 1.0e-12d && curvature.isFinite) {
          sHistory = (sHistory :+ s).takeRight(optimizerMemory)
          yHistory = (yHistory :+ y).takeRight(optimizerMemory)
          rhoHistory = (rhoHistory :+ (1.0d / curvature)).takeRight(optimizerMemory)
        }
      }
      var direction = gradient
      val alphas = Array.fill(sHistory.length)(0.0d)
      var historyIndex = sHistory.length - 1
      while (historyIndex >= 0) {
        alphas(historyIndex) = rhoHistory(historyIndex) * dot(sHistory(historyIndex), direction)
        direction = direction.zip(yHistory(historyIndex)).map { case (value, y) => value - alphas(historyIndex) * y }
        historyIndex -= 1
      }
      val scale = if (yHistory.nonEmpty) {
        val last = yHistory.length - 1
        dot(sHistory(last), yHistory(last)) / math.max(dot(yHistory(last), yHistory(last)), 1.0e-12d)
      } else 1.0d
      direction = direction.map(_ * scale)
      historyIndex = 0
      while (historyIndex < sHistory.length) {
        val beta = rhoHistory(historyIndex) * dot(yHistory(historyIndex), direction)
        direction = direction.zip(sHistory(historyIndex)).map { case (value, s) => value + s * (alphas(historyIndex) - beta) }
        historyIndex += 1
      }
      direction = direction.map(value => -value)
      if (dot(direction, gradient) >= 0.0d || direction.exists(value => !value.isFinite)) {
        direction = gradient.map(value => -value)
      }
      var step = 1.0d
      var candidateWeights = weights
      var candidateIntercept = intercept
      // SQL line search is retained for small semantic/parity probes, where
      // it makes convergence behavior observable without material cost. For
      // the production 1,770-term model, the gradient action is already the
      // dominant relational boundary; evaluating the objective repeatedly
      // would add another full remote action per trial and turn a bounded
      // production fit into an hours-long workload. The L-BFGS direction is
      // therefore accepted directly for the large public-SQL path.
      val useSqlLineSearch = parityProbe || quickProbe || terms.length < 32
      if (useSqlLineSearch) {
        val currentObjective = objective(gradientFrame, gradientTerms, weights, intercept)
        val directionalDerivative = dot(gradient, direction)
        var accepted = false
        var lineSearch = 0
        while (!accepted && lineSearch < 12) {
          candidateWeights = weights.indices.map(index => weights(index) + step * direction(index)).toVector
          candidateIntercept = intercept + step * direction.last
          val candidateObjective = objective(gradientFrame, gradientTerms, candidateWeights, candidateIntercept)
          accepted = candidateObjective <= currentObjective + 1.0e-4d * step * directionalDerivative
          if (!accepted) {
            step *= 0.5d
            lineSearch += 1
          }
        }
      } else {
        candidateWeights = weights.indices.map(index => weights(index) + step * direction(index)).toVector
        candidateIntercept = intercept + step * direction.last
      }
      previousParameters = Some(parameters)
      previousGradient = Some(gradient)
      weights = candidateWeights
      intercept = candidateIntercept
      iteration += 1
    }
    val rawWeights = weights.zip(featureStats).map { case (weight, (_, standardDeviation)) =>
      weight / standardDeviation
    }
    val rawIntercept = intercept - weights.zip(featureStats).map { case (weight, (mean, standardDeviation)) =>
      weight * mean / standardDeviation
    }.sum
    (rawWeights, rawIntercept)
  }

  private def scored(
      input: DataFrame,
      terms: Vector[Column],
      coefficients: Vector[Double],
      intercept: Double,
      scoreColumn: String): DataFrame = {
    val margin = linearMargin(terms, coefficients, intercept)
    input.withColumn(scoreColumn, probability(margin))
  }

  /** Area under ROC computed relationally, including half-credit for ties. */
  private def areaUnderRoc(scoredInput: DataFrame, scoreColumn: String, labelColumn: String): Double = {
    // A cumulative Window over grouped scores is mathematically compact but
    // can leave a managed Serverless Scala kernel waiting indefinitely after
    // a large native fit. Compare positive/negative score pairs instead. The
    // join and aggregate are public relational expressions, and the result
    // is exactly the Mann-Whitney formulation of ROC AUC: wins plus half of
    // ties divided by all positive/negative pairs.
    val scored = scoredInput
      .select(col(scoreColumn).cast("double").as("_native_score"), col(labelColumn).cast("double").as("_native_label"))
    val positivesFrame = scored.filter(col("_native_label") === lit(1.0d)).alias("_native_positive")
    val negativesFrame = scored.filter(col("_native_label") =!= lit(1.0d)).alias("_native_negative")
    val positiveCount = positivesFrame.count()
    val negativeCount = negativesFrame.count()
    val pairResult = if (positiveCount == 0L || negativeCount == 0L) None
    else Some(positivesFrame.crossJoin(negativesFrame)
      .agg(
        sum(when(col("_native_positive._native_score") > col("_native_negative._native_score"), lit(1.0d))
          .when(col("_native_positive._native_score") === col("_native_negative._native_score"), lit(0.5d))
          .otherwise(lit(0.0d))).as("wins"))
      .head())
    val positives = positiveCount.toDouble
    val negatives = negativeCount.toDouble
    if (positives == 0.0d || negatives == 0.0d) 0.5d
    else finiteDouble(pairResult.get, 0) / (positives * negatives)
  }

  private def withFold(input: DataFrame, labelColumn: String): DataFrame = {
    // Zingg 0.7 leaves CrossValidator's random seed implicit.  Native mode
    // intentionally freezes it so model artifacts are reproducible.  rand(seed)
    // is a public SQL expression and remains on the remote Spark plan.
    input.withColumn("_zingg_native_fold", floor(rand(Seed) * lit(effectiveNumFolds)).cast("int"))
  }

  def fit(input: DataFrame, featureColumns: Seq[String], labelColumn: String, context: RewriteContext): NativeTrainedModel = {
    val features = featureColumns.toVector
    require(features.nonEmpty, "Zingg native model requires at least one similarity feature")
    val preMaterializedPath = sys.props.get("zingg.native.model.inputPath").filter(_.trim.nonEmpty)
    val preMaterialized = preMaterializedPath.map(input.sparkSession.read.parquet)
    val modelInput = preMaterialized.getOrElse(input)
    NativeDiagnostics.modelStage(context, "fit-start", s"features=${features.length} label=$labelColumn")
    // Normalize each base feature once before polynomial expansion.  Reusing
    // named public columns avoids embedding the same cast/coalesce expression
    // thousands of times in the Serverless Connect plan while preserving the
    // exact Spark PolynomialExpansion term set (degree 3 in production;
    // degree 1 only for the explicit quick-probe diagnostic).
    val baseNames = features.indices.map(i => s"_native_base_$i").toVector
    val normalizedInput = modelInput.select((Seq(col(labelColumn)) ++
      features.zip(baseNames).map { case (feature, name) =>
        coalesce(modelInput.col(feature).cast("double"), lit(0.0d)).alias(name)
      }): _*)
    NativeDiagnostics.modelStage(context, "terms-build-start", s"baseFeatures=${baseNames.length}")
    // Do not construct polynomial terms from the pre-materialization frame.
    // With fuzzy features that would still embed the original higher-order
    // similarity expressions in the Connect plan before the narrow write.
    // The materialized branch binds terms only after rereading scalar bases.
    val materializeBases = preMaterialized.isEmpty && sys.props.get("zingg.native.model.materializePath").exists(_.trim.nonEmpty)
    val expandedTerms = if (materializeBases) Vector.empty[Column]
      else expanded(normalizedInput, baseNames, effectivePolynomialDegree)
    NativeDiagnostics.modelStage(context, "terms-build-complete", s"terms=${expandedTerms.length}")
    // SparkModel's ordinary-Zingg boundary pre-materializes the original
    // feature columns. Reuse the normalized scalar frame here; using the raw
    // pre-materialized frame would leave _native_base_* columns unresolved
    // and force Connect to carry incompatible dataframe identities into the
    // first gradient action.
    val materializedInput = preMaterialized match {
      case Some(_) => normalizedInput
      case None => sys.props.get("zingg.native.model.materializePath")
      .filter(_.trim.nonEmpty) match {
      case Some(root) =>
        val path = s"${root.stripSuffix("/")}/base-${java.util.UUID.randomUUID().toString}"
        NativeDiagnostics.modelStage(context, "base-features-write-start", s"features=${baseNames.length}")
        // Persist the already-computed similarity columns in one narrow
        // projection. Polynomial terms are built only after reread, so the
        // fuzzy expressions are not repeated in the model plan.
        normalizedInput.write.mode("overwrite").parquet(path)
        NativeDiagnostics.modelStage(context, "base-features-write-complete", s"features=${baseNames.length}")
        input.sparkSession.read.parquet(path)
      case None => normalizedInput
      }
    }
    val termsBoundToMaterializedInput = preMaterialized.nonEmpty || materializeBases
    val termColumns = if (termsBoundToMaterializedInput)
      // Bind the exact polynomial terms to the reread base-feature frame.
      polynomialTerms(baseNames.map(materializedInput.col), effectivePolynomialDegree)
    else expandedTerms
    NativeDiagnostics.modelStage(context, "terms-ready", s"terms=${termColumns.length}")
    val folded = withFold(materializedInput, labelColumn)

    var bestReg = RegGrid.head
    var bestThreshold = ThresholdGrid.head
    var bestMetric = Double.NegativeInfinity

    effectiveRegGrid.foreach { reg =>
      var foldMetric = 0.0d
      var fold = 0
      while (fold < effectiveNumFolds) {
        // A bounded one-fold probe has no held-out partition. Reuse the full
        // input for both fit and validation; filtering fold 0 out of a
        // one-fold assignment would produce an empty training frame.
        val training = if (effectiveNumFolds == 1) folded.drop("_zingg_native_fold")
        else folded.filter(col("_zingg_native_fold") =!= lit(fold)).drop("_zingg_native_fold")
        val validation = if (effectiveNumFolds == 1) folded.drop("_zingg_native_fold")
        else folded.filter(col("_zingg_native_fold") === lit(fold)).drop("_zingg_native_fold")
        // Rebind expression terms to each fold frame. A Column carries its
        // originating dataframe identity in Spark Connect; reusing terms
        // built from normalizedInput against filtered fold frames fails
        // analysis even when the visible column names match.
        // Rebind terms to each filtered frame in every mode. Spark Connect
        // Columns retain dataframe identity; reusing terms from the
        // pre-filter materialized frame can leave a valid-looking plan that
        // eventually stalls during a later aggregate action.
        val trainingTerms = polynomialTerms(baseNames.map(training.col), effectivePolynomialDegree)
        val validationTerms = polynomialTerms(baseNames.map(validation.col), effectivePolynomialDegree)
        val (weights, intercept) = fitLogistic(training, trainingTerms, baseNames, labelColumn, reg, effectiveMaxIter, effectivePolynomialDegree, context)
        val validationScored = scored(validation, validationTerms, weights, intercept, "_zingg_native_cv_score")
        // Bounded probes isolate gradient/materialization scalability. Keep
        // production CV/AUC semantics untouched; the diagnostic metric avoids
        // conflating a remote window action with the model fit boundary.
        foldMetric += (if (boundedProbe) 0.5d
          else areaUnderRoc(validationScored, "_zingg_native_cv_score", labelColumn))
        fold += 1
      }
      val metric = foldMetric / effectiveNumFolds.toDouble
      // BinaryClassificationEvaluator's AUC does not depend on LR threshold.
      // Preserve ParamGrid's first-threshold tie behavior explicitly.
      ThresholdGrid.foreach { threshold =>
        if (metric > bestMetric) {
          bestMetric = metric
          bestReg = reg
          bestThreshold = threshold
        }
      }
    }
    NativeDiagnostics.modelStage(context, "cv-complete", s"bestReg=$bestReg bestMetric=$bestMetric")

    val (coefficients, intercept) = fitLogistic(materializedInput, termColumns, baseNames, labelColumn, bestReg, effectiveMaxIter, effectivePolynomialDegree, context)
    NativeDiagnostics.modelStage(context, "fit-complete", s"coefficients=${coefficients.length}")
    val model = NativeTrainedModel(
      SchemaVersion, features, effectivePolynomialDegree, PolynomialOrdering, coefficients,
      intercept, bestReg, bestThreshold, effectiveMaxIter, effectiveNumFolds, Seed, Optimizer)
    NativeEvidenceCollector.recordRule(context, "model.nativeLogisticCv")
    model
  }

  def predict(
      input: DataFrame,
      model: NativeTrainedModel,
      featureVectorColumn: String,
      expandedFeatureColumn: String,
      probabilityColumn: String,
      rawPredictionColumn: String,
      predictionColumn: String,
      scoreColumn: String,
      context: RewriteContext): DataFrame = {
    require(model.schemaVersion == SchemaVersion,
      s"Unsupported native Zingg model schema version ${model.schemaVersion}; expected $SchemaVersion")
    require(model.polynomialDegree >= 1 && model.polynomialDegree <= PolynomialDegree && model.polynomialOrdering == PolynomialOrdering,
      s"Unsupported native polynomial contract ${model.polynomialDegree}/${model.polynomialOrdering}")

    val base = baseFeatures(input, model.featureColumns)
    val terms = polynomialTerms(base, model.polynomialDegree)
    val margin = linearMargin(terms, model.coefficients, model.intercept)
    val score = probability(margin)

    val result = input
      // Serverless environment 5 does not expose the JVM ML helper or the
      // array_to_vector SQL routine. Native prediction consumes score and
      // prediction directly, so retain these diagnostic columns as ordinary
      // public SQL arrays rather than introducing a forbidden/private vector
      // conversion boundary.
      .withColumn(featureVectorColumn, array(base: _*))
      .withColumn(expandedFeatureColumn, array(terms: _*))
      .withColumn(rawPredictionColumn, array(-margin, margin))
      .withColumn(probabilityColumn, array(lit(1.0d) - score, score))
      .withColumn(predictionColumn, when(score >= lit(model.threshold), lit(1.0d)).otherwise(lit(0.0d)))
      .withColumn(scoreColumn, score.cast("double"))

    NativeEvidenceCollector.recordRule(context, "model.nativePrediction")
    NativePlanGuard.guardDataFrame(result, context)
  }

  def save(spark: SparkSession, model: NativeTrainedModel, path: String, context: RewriteContext): Unit = {
    val coefficientArray = array(model.coefficients.map(lit): _*)
    val featureArray = array(model.featureColumns.map(lit): _*)
    spark.range(1L).select(
      lit(model.schemaVersion).as("schemaVersion"),
      featureArray.as("featureColumns"),
      lit(model.polynomialDegree).as("polynomialDegree"),
      lit(model.polynomialOrdering).as("polynomialOrdering"),
      coefficientArray.as("coefficients"),
      lit(model.intercept).as("intercept"),
      lit(model.regParam).as("regParam"),
      lit(model.threshold).as("threshold"),
      lit(model.maxIter).as("maxIter"),
      lit(model.numFolds).as("numFolds"),
      lit(model.seed).as("seed"),
      lit(model.optimizer).as("optimizer"))
      .write.mode("overwrite").parquet(nativePath(path))
    NativeEvidenceCollector.recordRule(context, "model.nativePersistence.save")
  }

  def load(spark: SparkSession, path: String, context: RewriteContext): NativeTrainedModel = {
    val row = try spark.read.parquet(nativePath(path)).head()
    catch {
      case e: Exception =>
        throw new NativeRewriteUnsupportedException(
          s"No native Zingg model artifact was found at '${nativePath(path)}'. " +
            "REWRITE/STRICT model loading requires a model trained by zingg-native; " +
            "use OFF/AUDIT to load a legacy CrossValidatorModel or convert/retrain it. Cause: ${e.getMessage}")
    }
    val model = try {
      NativeTrainedModel(
        row.getAs[Int]("schemaVersion"),
        row.getAs[scala.collection.Seq[String]]("featureColumns").toVector,
        row.getAs[Int]("polynomialDegree"),
        row.getAs[String]("polynomialOrdering"),
        row.getAs[scala.collection.Seq[Double]]("coefficients").toVector,
        row.getAs[Double]("intercept"),
        row.getAs[Double]("regParam"),
        row.getAs[Double]("threshold"),
        row.getAs[Int]("maxIter"),
        row.getAs[Int]("numFolds"),
        row.getAs[Long]("seed"),
        row.getAs[String]("optimizer"))
    } catch {
      case e: Exception =>
        throw new NativeRewriteUnsupportedException(
          s"Native model persistence rule model.nativePersistence.load rejected a corrupt or incompatible sidecar at '${nativePath(path)}': ${e.getMessage}")
    }
    require(model.schemaVersion == SchemaVersion,
      s"Unsupported native Zingg model schema version ${model.schemaVersion}; expected $SchemaVersion")
    require(model.featureColumns.nonEmpty,
      "Native model persistence rule model.nativePersistence.load requires at least one feature column")
    require(model.polynomialDegree >= 1 && model.polynomialDegree <= PolynomialDegree,
      s"Native model persistence rule model.nativePersistence.load rejected polynomial degree ${model.polynomialDegree}")
    require(model.coefficients.length == polynomialIndexPaths(model.featureColumns.length, model.polynomialDegree).length,
      s"Native model persistence rule model.nativePersistence.load rejected coefficient count ${model.coefficients.length}")
    require(RegGrid.contains(model.regParam) && ThresholdGrid.contains(model.threshold),
      s"Native model persistence rule model.nativePersistence.load rejected grid values regParam=${model.regParam} threshold=${model.threshold}")
    NativeEvidenceCollector.recordRule(context, "model.nativePersistence.load")
    model
  }
}

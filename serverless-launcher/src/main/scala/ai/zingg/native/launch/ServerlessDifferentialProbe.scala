package ai.zingg.native.launch

import ai.zingg.nativebridge.NativeOperationProvider
import java.util.{ArrayList, Locale}
import org.apache.spark.sql.{Row, RowFactory, SparkSession}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

/**
 * Serverless-only differential probe.  It deliberately uses reflection for
 * the oracle classes so the production native artifacts do not depend on the
 * upstream implementation.  The Zingg 0.7.0 assembly supplies those classes
 * at runtime; native output is produced only through the public DataFrame
 * rewrite provider.
 */
object ServerlessDifferentialProbe {
  private final case class Rule(id: String, operation: String, oracleClass: String, constructorArgs: Seq[AnyRef] = Seq.empty)

  private val Rules = Map(
    "exact" -> Rule("exact", "zingg.common.core.similarity.function.SimilarityFunctionExact", "zingg.common.core.similarity.function.SimilarityFunctionExact", Seq("probe")),
    "string_base" -> Rule("string_base", "zingg.common.core.similarity.function.StringSimilarityFunction", "zingg.common.core.similarity.function.StringSimilarityFunction"),
    "jaccard" -> Rule("jaccard", "zingg.common.core.similarity.function.JaccSimFunction", "zingg.common.core.similarity.function.JaccSimFunction"),
    "numbers_jaccard" -> Rule("numbers_jaccard", "zingg.common.core.similarity.function.NumbersJaccardFunction", "zingg.common.core.similarity.function.NumbersJaccardFunction"),
    "jaro" -> Rule("jaro", "zingg.common.core.similarity.function.JaroWinklerFunction", "zingg.common.core.similarity.function.JaroWinklerFunction"),
    "ajaro" -> Rule("ajaro", "zingg.common.core.similarity.function.AJaroWinklerFunction", "zingg.common.core.similarity.function.AJaroWinklerFunction"),
    "affine_gap" -> Rule("affine_gap", "zingg.common.core.similarity.function.AffineGapSimilarityFunction", "zingg.common.core.similarity.function.AffineGapSimilarityFunction"),
    "product_code" -> Rule("product_code", "zingg.common.core.similarity.function.ProductCodeFunction", "zingg.common.core.similarity.function.ProductCodeFunction"),
    "check_null" -> Rule("check_null", "zingg.common.core.similarity.function.CheckNullFunction", "zingg.common.core.similarity.function.CheckNullFunction", Seq("probe")),
    "check_blank_or_null" -> Rule("check_blank_or_null", "zingg.common.core.similarity.function.CheckBlankOrNullFunction", "zingg.common.core.similarity.function.CheckBlankOrNullFunction"),
    "email" -> Rule("email", "zingg.common.core.similarity.function.EmailMatchTypeFunction", "zingg.common.core.similarity.function.EmailMatchTypeFunction"),
    "pin" -> Rule("pin", "zingg.common.core.similarity.function.PinCodeMatchTypeFunction", "zingg.common.core.similarity.function.PinCodeMatchTypeFunction"),
    "only_alpha_exact" -> Rule("only_alpha_exact", "zingg.common.core.similarity.function.OnlyAlphabetsExactSimilarity", "zingg.common.core.similarity.function.OnlyAlphabetsExactSimilarity"),
    "only_alpha_affine" -> Rule("only_alpha_affine", "zingg.common.core.similarity.function.OnlyAlphabetsAffineGapSimilarity", "zingg.common.core.similarity.function.OnlyAlphabetsAffineGapSimilarity"),
    "same_first_word" -> Rule("same_first_word", "zingg.common.core.similarity.function.SameFirstWordFunction", "zingg.common.core.similarity.function.SameFirstWordFunction"))

  private val Fixtures = Seq(
    (null, null), ("", "x"), ("Alice", "alice"), ("martha", "marhta"),
    ("kitten", "sitting"), ("José", "JOSE"), ("A-100 blue", "A100 blue"),
    ("😀a", "😀b"), ("one two 10", "two 10 11"), ("abc", "xyz"),
    ("  leading", "leading  "), ("a\tb\nc", "a b c"),
    ("foo foo bar", "foo bar bar"), ("A.*(x)?", "a.*(x)?"),
    ("élève cafe\u0301", "ELEVE café"),
    ("123.45 -6", "123 45 -6"), ("", ""),
    ("word1-word2", "word1 word2"))

  def run(spark: SparkSession, requested: Option[String]): Unit = {
    val names = requested.map(_.split(",").iterator.map(_.trim.toLowerCase(Locale.ROOT)).filter(_.nonEmpty).toSeq)
      .getOrElse(Rules.keys.toSeq.sorted)
    val selected = names.map(name => Rules.getOrElse(name,
      throw new IllegalArgumentException(s"Unknown differential rule '$name'")))
    val schema = StructType(Seq(
      StructField("left_value", DataTypes.StringType, nullable = true),
      StructField("right_value", DataTypes.StringType, nullable = true)))
    val rows = new ArrayList[Row]()
    Fixtures.foreach { case (left, right) =>
      rows.add(RowFactory.create(left.asInstanceOf[AnyRef], right.asInstanceOf[AnyRef]))
    }
    val input = spark.createDataFrame(rows, schema)
    val provider = NativeOperationProvider.fromSpark(spark, "differential")
    selected.zipWithIndex.foreach { case (rule, ruleIndex) =>
      val nativeOutput = provider.similarityByZinggName(input, rule.operation, "left_value", "right_value", "actual")
      val actual = nativeOutput.select("actual").collect().map(_.getAs[java.lang.Double](0).doubleValue())
      val oracle = oracleValues(rule)
      if (actual.length != oracle.length) {
        throw new AssertionError(s"Differential row-count mismatch for ${rule.id}: native=${actual.length}, oracle=${oracle.length}")
      }
      actual.zip(oracle).zipWithIndex.foreach { case ((nativeValue, referenceValue), index) =>
        if (!sameDouble(nativeValue, referenceValue)) {
          throw new AssertionError(s"Differential mismatch rule=${rule.id}, row=$index, native=$nativeValue, oracle=$referenceValue")
        }
      }
      if (ruleIndex == selected.size - 1)
        provider.captureEvidence(nativeOutput, "unverified")
      println(s"NATIVE_DIFFERENTIAL_PASS rule=${rule.id} rows=${actual.length}")
    }
    println(s"NATIVE_DIFFERENTIAL_SUMMARY rules=${selected.size} rows=${Fixtures.size}")
  }

  private def oracleValues(rule: Rule): Array[Double] = {
    val clazz = Class.forName(rule.oracleClass)
    val instance = instantiate(clazz, rule.constructorArgs)
    val method = clazz.getMethods.find(m => m.getName == "call" && m.getParameterCount == 2).getOrElse {
      throw new NoSuchMethodException(s"No two-argument call method on ${rule.oracleClass}")
    }
    Fixtures.map { case (left, right) =>
      method.invoke(instance, left, right).asInstanceOf[java.lang.Double].doubleValue()
    }.toArray
  }

  private def instantiate(clazz: Class[_], args: Seq[AnyRef]): AnyRef = {
    if (args.isEmpty) clazz.getDeclaredConstructor().newInstance().asInstanceOf[AnyRef]
    else clazz.getConstructor(args.map(_.getClass): _*).newInstance(args: _*).asInstanceOf[AnyRef]
  }

  private def sameDouble(left: Double, right: Double): Boolean =
    (left.isNaN && right.isNaN) || math.abs(left - right) <= 1e-9
}

package ai.zingg.native.launch

import ai.zingg.nativebridge.NativeOperationProvider
import java.util.{ArrayList, Locale}
import org.apache.spark.sql.{Row, RowFactory, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/** Reflection-oracle parity probe for the complete upstream string-hash subset. */
object ServerlessHashDifferentialProbe {
  private final case class Rule(name: String, oracle: String, args: Seq[AnyRef] = Seq.empty)
  private val Rules =
    (1 to 4).map(n => Rule(s"first${n}Chars", "zingg.spark.core.hash.SparkFirstChars", Seq(Integer.valueOf(n)))) ++
    (1 to 3).map(n => Rule(s"last${n}Chars", "zingg.spark.core.hash.SparkLastChars", Seq(Integer.valueOf(n)))) ++
    Seq(
      Rule("lastWord", "zingg.spark.core.hash.SparkLastWord"),
      Rule("isNullOrEmpty", "zingg.spark.core.hash.SparkIsNullOrEmpty"),
      Rule("identityString", "zingg.spark.core.hash.SparkIdentityString"),
      Rule("first2CharsBox", "zingg.spark.core.hash.SparkFirst2CharsBox"),
      Rule("first3CharsBox", "zingg.spark.core.hash.SparkFirst3CharsBox"))

  // Include source-sensitive, null/empty, Unicode, whitespace, and numeric
  // token cases so the probe checks the hash result, not merely construction.
  private val Fixtures = Seq[AnyRef]("Alice", "", null, "A", "Álvaro", "A  B", "123-45")

  def run(spark: SparkSession, requested: Option[String]): Unit = {
    val names = requested.map(_.split(",").iterator.map(_.trim.toLowerCase(Locale.ROOT)).filter(_.nonEmpty).toSeq)
      .getOrElse(Rules.map(_.name))
    val selected = names.map(name => Rules.find(_.name == name).getOrElse(
      throw new IllegalArgumentException(s"Unknown hash differential rule '$name'")))
    val schema = StructType(Seq(StructField("value", DataTypes.StringType, true)))
    val rows = new ArrayList[Row]()
    Fixtures.foreach(value => rows.add(RowFactory.create(value)))
    val input = spark.createDataFrame(rows, schema)
    val provider = NativeOperationProvider.fromSpark(spark, "hash-differential")
    selected.foreach { rule =>
      val actual = provider.hash(input, rule.name, "value", "actual").select("actual").collect().map(_.get(0))
      val clazz = Class.forName(rule.oracle)
      val instance = if (rule.args.isEmpty) clazz.getDeclaredConstructor().newInstance()
      else clazz.getConstructors.find(_.getParameterCount == rule.args.size).get.newInstance(rule.args: _*)
      val method = clazz.getMethods.find(m => m.getName == "call" && m.getParameterCount == 1).get
      val expected = Fixtures.map(value => method.invoke(instance, value)).toArray
      require(actual.length == expected.length, s"Hash row count mismatch for ${rule.name}")
      actual.zip(expected).zipWithIndex.foreach { case ((a, e), index) =>
        require(Option(a).map(_.toString) == Option(e).map(_.toString),
          s"Hash mismatch rule=${rule.name} row=$index native=$a oracle=$e")
      }
      println(s"NATIVE_HASH_DIFFERENTIAL_PASS rule=${rule.name} rows=${actual.length}")
    }
    runNumeric(spark, provider)
    println(s"NATIVE_HASH_DIFFERENTIAL_SUMMARY rules=${selected.size + 17} stringRules=${selected.size} numericRules=17 rows=${Fixtures.size}")
  }

  private def runNumeric(spark: SparkSession, provider: NativeOperationProvider): Unit = {
    final case class Group(dataType: org.apache.spark.sql.types.DataType, value: AnyRef, rules: Seq[Rule])
    val groups = Seq(
      Group(DataTypes.IntegerType, Integer.valueOf(-12), Seq(
        Rule("identityInteger", "zingg.spark.core.hash.SparkIdentityInteger"),
        Rule("lessThanZeroInt", "zingg.spark.core.hash.SparkLessThanZeroInt"),
        Rule("trimLast1DigitsInt", "zingg.spark.core.hash.SparkTrimLastDigitsInt", Seq(Integer.valueOf(1))),
        Rule("rangeBetween0And10Int", "zingg.spark.core.hash.SparkRangeInt", Seq(Integer.valueOf(0), Integer.valueOf(10))))),
      Group(DataTypes.LongType, java.lang.Long.valueOf(-12L), Seq(
        Rule("identityLong", "zingg.spark.core.hash.SparkIdentityLong"),
        Rule("lessThanZeroLong", "zingg.spark.core.hash.SparkLessThanZeroLong"),
        Rule("trimLast1DigitsLong", "zingg.spark.core.hash.SparkTrimLastDigitsLong", Seq(Integer.valueOf(1))),
        Rule("rangeBetween0And10Long", "zingg.spark.core.hash.SparkRangeLong", Seq(Integer.valueOf(0), Integer.valueOf(10))))),
      Group(DataTypes.FloatType, java.lang.Float.valueOf(-12.5f), Seq(
        Rule("lessThanZeroFloat", "zingg.spark.core.hash.SparkLessThanZeroFloat"),
        Rule("truncateFloatTo1Places", "zingg.spark.core.hash.SparkTruncateFloat", Seq(Integer.valueOf(1))),
        Rule("trimLast1DigitsFloat", "zingg.spark.core.hash.SparkTrimLastDigitsFloat", Seq(Integer.valueOf(1))),
        Rule("rangeBetween0And10Float", "zingg.spark.core.hash.SparkRangeFloat", Seq(Integer.valueOf(0), Integer.valueOf(10))))),
      Group(DataTypes.DoubleType, java.lang.Double.valueOf(-12.5d), Seq(
        Rule("lessThanZeroDbl", "zingg.spark.core.hash.SparkLessThanZeroDbl"),
        Rule("truncateDoubleTo1Places", "zingg.spark.core.hash.SparkTruncateDouble", Seq(Integer.valueOf(1))),
        Rule("trimLast1DigitsDbl", "zingg.spark.core.hash.SparkTrimLastDigitsDbl", Seq(Integer.valueOf(1))),
        Rule("rangeBetween0And10Dbl", "zingg.spark.core.hash.SparkRangeDbl", Seq(Integer.valueOf(0), Integer.valueOf(10))),
        Rule("round", "zingg.spark.core.hash.SparkRound")))
    )
    groups.foreach { group =>
      val dataType = group.dataType
      val value = group.value
      val input = spark.createDataFrame(
        new ArrayList[Row](java.util.Arrays.asList(RowFactory.create(value))),
        StructType(Seq(StructField("value", dataType, true))))
      group.rules.foreach { rule =>
        val actual = provider.hash(input, rule.name, "value", "actual").select("actual").collect().map(_.get(0))
        val clazz = Class.forName(rule.oracle)
        val instance = if (rule.args.isEmpty) clazz.getDeclaredConstructor().newInstance()
        else clazz.getConstructors.find(_.getParameterCount == rule.args.size).get.newInstance(rule.args: _*)
        val expected = clazz.getMethods.find(m => m.getName == "call" && m.getParameterCount == 1).get.invoke(instance, value)
        require(Option(actual.head).map(_.toString) == Option(expected).map(_.toString),
          s"Numeric hash mismatch rule=${rule.name} native=${actual.head} oracle=$expected")
        println(s"NATIVE_HASH_DIFFERENTIAL_PASS rule=${rule.name} rows=1")
      }
    }
  }
}

package ai.zingg.native

import java.util.ArrayList
import org.apache.spark.sql.{Row, RowFactory, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

class StringHashCompatibilityTest {
  private final case class HashCase(
      name: String,
      expected: Seq[String],
      property: (String, String) => Boolean
  )

  @Test def matchesPinnedZinggStringHashOracleVectorsAndStructuralProperties()
      : Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("StringHashCompatibilityTest")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    try {
      val values =
        Seq[String]("Alice", "", null, "A", "Álvaro", "A  B", "123-45")
      val rows = new ArrayList[Row]()
      values.foreach(value => rows.add(RowFactory.create(value)))
      val input = spark.createDataFrame(
        rows,
        StructType(Seq(StructField("value", DataTypes.StringType, true)))
      )
      val provider =
        NativeOperationProvider.fromSpark(spark, "string-hash-compatibility")

      def prefix(width: Int): (String, String) => Boolean =
        (source, result) =>
          source == null || (result != null && source.startsWith(
            result
          ) && result.length <= width)
      def suffix(width: Int): (String, String) => Boolean =
        (source, result) =>
          source == null || (result != null && source.endsWith(
            result
          ) && result.length <= width)
      val wordSuffix: (String, String) => Boolean =
        (source, result) =>
          source == null || result == null || source.trim.endsWith(result)

      // Expected values are pinned from the Zingg 0.7.0 Spark hash functions
      // exercised by ServerlessHashDifferentialProbe's reflection oracle.
      val cases = Seq(
        HashCase(
          "first1Chars",
          Seq("A", "", null, "A", "Á", "A", "1"),
          prefix(1)
        ),
        HashCase(
          "first2Chars",
          Seq("Al", "", null, "A", "Ál", "A ", "12"),
          prefix(2)
        ),
        HashCase(
          "first3Chars",
          Seq("Ali", "", null, "A", "Álv", "A  ", "123"),
          prefix(3)
        ),
        HashCase(
          "first4Chars",
          Seq("Alic", "", null, "A", "Álva", "A  B", "123-"),
          prefix(4)
        ),
        HashCase(
          "last1Chars",
          Seq("e", "", null, "A", "o", "B", "5"),
          suffix(1)
        ),
        HashCase(
          "last2Chars",
          Seq("ce", "", null, "A", "ro", " B", "45"),
          suffix(2)
        ),
        HashCase(
          "last3Chars",
          Seq("ice", "", null, "A", "aro", "  B", "-45"),
          suffix(3)
        ),
        HashCase(
          "lastWord",
          Seq("Alice", "", null, "A", "Álvaro", "B", "123-45"),
          wordSuffix
        )
      )

      cases.foreach { testCase =>
        val actual = provider
          .hash(input, testCase.name, "value", "actual")
          .select("actual")
          .collect()
          .map(row => if (row.isNullAt(0)) null else row.get(0).toString)
          .toSeq
        assertEquals(testCase.expected, actual, testCase.name)
        assertEquals(values.length, actual.length, testCase.name)
        values.zip(actual).foreach { case (source, result) =>
          assertTrue(
            testCase.property(source, result),
            s"${testCase.name} property source=$source result=$result"
          )
        }
      }
    } finally spark.stop()
  }
}

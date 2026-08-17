package ai.zingg.native

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertTrue
import org.apache.spark.sql.catalyst.expressions.{Literal, RegExpExtractAll}

class JaccardContractTest {
  @Test def usesUnicodeLetterAndNumberTokenPattern(): Unit = {
    val expression = CatalystSimilarity(
      "JACCARD_SIMILARITY",
      Literal("Ångström 2026"),
      Literal("ångström 2026")
    )
    val patterns = expression.collect { case e: RegExpExtractAll => e.regexp.toString }
    assertTrue(patterns.exists(_.contains("\\p{L}")))
    assertTrue(patterns.exists(_.contains("\\p{N}")))
  }
}

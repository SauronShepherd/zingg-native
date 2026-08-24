package ai.zingg.native

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

class NativeModelParityTest {
  @Test def productionPolynomialTermCountAndOrderingAreStable(): Unit = {
    val paths = NativeModelEngine.polynomialIndexPaths(20, 3)
    assertEquals(1770, paths.length)
    assertEquals(Vector(0), paths.head)
    assertEquals(paths, NativeModelEngine.polynomialIndexPaths(20, 3))
    assertTrue(paths.forall(path => path.nonEmpty && path.length <= 3))
    assertTrue(paths.zip(paths.drop(1)).forall { case (left, right) => left != right })
  }

  @Test def polynomialExpansionRejectsUnsupportedDegrees(): Unit = {
    assertEquals(9, NativeModelEngine.polynomialIndexPaths(3, 2).length)
    assertEquals(3, NativeModelEngine.polynomialIndexPaths(3, 1).length)
    val failure = org.junit.jupiter.api.Assertions.assertThrows(classOf[IllegalArgumentException], () =>
      NativeModelEngine.polynomialIndexPaths(3, 4))
    assertTrue(failure.getMessage.nonEmpty)
  }

  @Test def twoFeatureDegreeTwoOrderMatchesSparkPolynomialExpansionContract(): Unit = {
    assertEquals(
      Vector(Vector(0), Vector(0, 0), Vector(1), Vector(0, 1), Vector(1, 1)),
      NativeModelEngine.polynomialIndexPaths(2, 2))
  }

  @Test def twoFeatureDegreeThreeOrderMatchesSparkPolynomialExpansionContract(): Unit = {
    assertEquals(
      Vector(
        Vector(0), Vector(0, 0), Vector(0, 0, 0),
        Vector(1), Vector(0, 1), Vector(0, 0, 1),
        Vector(1, 1), Vector(0, 1, 1), Vector(1, 1, 1)),
      NativeModelEngine.polynomialIndexPaths(2, 3))
  }
}

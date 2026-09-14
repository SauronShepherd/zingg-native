package ai.zingg.native

import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}
import org.junit.jupiter.api.Test

class NativeOptimizerStatusTest {
  @Test def optimizerStatusesRemainDistinct(): Unit = {
    assertTrue(
      NativeModelEngine.OptimizerStatuses.contains(
        NativeModelEngine.OptimizerStatusConverged
      )
    )
    assertTrue(
      NativeModelEngine.OptimizerStatuses.contains(
        NativeModelEngine.OptimizerStatusLineSearchStalled
      )
    )
    assertTrue(
      NativeModelEngine.OptimizerStatuses.contains(
        NativeModelEngine.OptimizerStatusMaxIterExhausted
      )
    )
    assertFalse(
      NativeModelEngine.OptimizerStatusConverged ==
        NativeModelEngine.OptimizerStatusLineSearchStalled
    )
    assertFalse(
      NativeModelEngine.OptimizerStatusConverged ==
        NativeModelEngine.OptimizerStatusMaxIterExhausted
    )
  }

  @Test def curvatureGuardRejectsDegenerateAndNonFinitePairs(): Unit = {
    assertTrue(NativeModelEngine.keepCurvaturePair(1.0e-6d))
    assertFalse(NativeModelEngine.keepCurvaturePair(0.0d))
    assertFalse(NativeModelEngine.keepCurvaturePair(1.0e-13d))
    assertFalse(NativeModelEngine.keepCurvaturePair(Double.NaN))
    assertFalse(NativeModelEngine.keepCurvaturePair(Double.PositiveInfinity))
  }
}

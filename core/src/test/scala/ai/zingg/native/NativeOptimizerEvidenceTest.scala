package ai.zingg.native

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class NativeOptimizerEvidenceTest {
  @Test def phaseEvidenceCarriesOptimizerTerminationMetadata(): Unit = {
    val runId = "optimizer-evidence-test"
    val context = RewriteContext(
      null,
      NativeExecutionMode.STRICT,
      RuntimeDescriptor("test-spark", "2.13"),
      phase = "train",
      correlationId = runId
    )
    try {
      NativeEvidenceCollector.recordOptimizer(
        context,
        NativeModelEngine.OptimizerStatusLineSearchStalled,
        3
      )
      val evidence = NativeEvidenceCollector.phaseSummary(context)
      assertEquals(
        Some(NativeModelEngine.OptimizerStatusLineSearchStalled),
        evidence.optimizerStatus
      )
      assertEquals(Some(3), evidence.discardedCurvaturePairs)
    } finally NativeEvidenceCollector.clear(runId)
  }
}

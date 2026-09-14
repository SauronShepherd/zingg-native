package ai.zingg.native

import java.util.logging.Logger
import scala.collection.concurrent.TrieMap

/** Privacy-safe, plan-level diagnostics. Never logs row values. */
object NativeDiagnostics {
  private val logger = Logger.getLogger("ai.zingg.native")
  private val once = TrieMap.empty[String, Boolean]
  val upstreamZinggVersion = "0.7.0"

  private def key(context: RewriteContext, kind: String, id: String): String =
    s"${context.correlationId}|${context.phase}|$kind|$id"

  def rewrite(context: RewriteContext, ruleId: String): Unit = {
    if (once.putIfAbsent(key(context, "rewrite", ruleId), true).isEmpty)
      logger.info(
        s"zingg-native rewrite applied run=${context.correlationId} phase=${context.phase} " +
          s"rule=$ruleId mode=${context.mode.id} nativeVersion=${Core.libraryVersion} " +
          s"zinggVersion=$upstreamZinggVersion spark=${context.runtime.sparkVersion}")
  }

  def auditLegacy(context: RewriteContext, operationId: String, construct: String, upstreamClass: String): Unit = {
    if (once.putIfAbsent(key(context, "audit", operationId), true).isEmpty)
      logger.warning(
        s"zingg-native audit legacy construct run=${context.correlationId} phase=${context.phase} " +
          s"operation=$operationId construct=$construct upstream=$upstreamClass mode=${context.mode.id} " +
          s"nativeVersion=${Core.libraryVersion} zinggVersion=$upstreamZinggVersion")
  }

  def phaseSummary(e: NativeExecutionEvidence): Unit =
    logger.info(
      s"zingg-native phase summary run=${e.correlationId} phase=${e.phase} mode=${e.mode} " +
        s"rules=${e.appliedRules.mkString("[", ",", "]")} " +
        s"planFingerprint=${e.planFingerprint} outputFingerprint=${e.outputFingerprint.getOrElse("unavailable")} " +
        s"photon=${e.photonEvidence.getOrElse("unverified")} " +
        s"nativeVersion=${Core.libraryVersion} zinggVersion=$upstreamZinggVersion")

  def modelStage(context: RewriteContext, stage: String, detail: String): Unit =
    logger.info(
      s"zingg-native model stage run=${context.correlationId} phase=${context.phase} " +
        s"stage=$stage detail=$detail mode=${context.mode.id} nativeVersion=${Core.libraryVersion} " +
        s"zinggVersion=$upstreamZinggVersion")

  def planGuard(context: RewriteContext, stage: String, detail: String): Unit =
    logger.info(
      s"zingg-native plan guard run=${context.correlationId} phase=${context.phase} " +
        s"stage=$stage detail=$detail mode=${context.mode.id} nativeVersion=${Core.libraryVersion} " +
        s"zinggVersion=$upstreamZinggVersion")

  def graphIteration(context: RewriteContext, iteration: Int, frontierEmpty: Boolean): Unit =
    logger.info(
      s"zingg-native graph iteration run=${context.correlationId} phase=${context.phase} " +
        s"iteration=$iteration frontierEmpty=$frontierEmpty mode=${context.mode.id} " +
        s"nativeVersion=${Core.libraryVersion} zinggVersion=$upstreamZinggVersion")

  def unsupported(
      context: RewriteContext,
      operationId: String,
      upstreamClass: String,
      construct: String,
      detail: String): NativeRewriteUnsupportedException =
    new NativeRewriteUnsupportedException(
      s"STRICT native execution cannot rewrite operation='$operationId' phase='${context.phase}' " +
        s"upstream='$upstreamClass' construct='$construct' mode='${context.mode.id}' " +
        s"spark='${context.runtime.sparkVersion}' nativeVersion='${Core.libraryVersion}' " +
        s"zinggVersion='$upstreamZinggVersion' run='${context.correlationId}'. $detail")
}

/** The operation is known, but the supplied semantic shape cannot be represented safely. */
final class NativeRewriteSemanticException(message: String, cause: Throwable = null)
  extends IllegalArgumentException(message, cause)

/** Used by later Databricks evidence gates when a successful query cannot prove required Photon execution. */
final class NativeExecutionVerificationException(message: String, cause: Throwable = null)
  extends IllegalStateException(message, cause)

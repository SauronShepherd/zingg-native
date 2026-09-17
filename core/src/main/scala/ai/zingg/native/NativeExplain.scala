package ai.zingg.native

import java.util.logging.Logger

/** Opt-in rewrite-decision diagnostics for --native-explain.
  *
  * The explain surface is intentionally metadata-only: it reports the phase,
  * operation, selected rule and decision, but never renders a Spark plan or
  * row/literal values. It therefore adds no Spark actions and is safe to use
  * while comparing rewrite/refactor behavior.
  */
object NativeExplain {
  private val logger = Logger.getLogger("ai.zingg.native")

  private def truthy(value: String): Boolean =
    Set("1", "true", "yes", "on").contains(
      Option(value).getOrElse("").trim.toLowerCase
    )

  def enabled: Boolean =
    sys.props
      .get("zingg.native.explain")
      .orElse(sys.env.get("ZINGG_NATIVE_EXPLAIN"))
      .exists(truthy)

  def emit(
      context: RewriteContext,
      operationId: String,
      ruleId: Option[String],
      decision: String
  ): Unit =
    if (enabled)
      logger.info(
        s"zingg-native explain run=${context.correlationId} phase=${context.phase} " +
          s"mode=${context.mode.id} operation=$operationId " +
          s"rule=${ruleId.getOrElse("unavailable")} decision=$decision"
      )
}

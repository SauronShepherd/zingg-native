package ai.zingg.native

import org.apache.spark.sql.{Column,DataFrame}
import org.apache.spark.sql.functions._

/** Compile NativeBlockingTreeIR to public Spark SQL expressions. */
object BlockingTreeCompiler {
  private def intSetting(property:String,environment:String,default:Int):Int=
    sys.props.get(property).orElse(sys.env.get(environment)).flatMap(v=>scala.util.Try(v.toInt).toOption).getOrElse(default)

  private def hashExpression(df:DataFrame,node:NativeBlockingNode,context:RewriteContext):Column={
    val operation=NativeOperation.resolve(s"blocking.${node.hashFunctionName}")
    if(!NativeRewriteRegistry.default.contains(operation))
      throw new NativeRewriteUnsupportedException(s"No native blocking hash rewrite for '${node.hashFunctionName}' on '${node.fieldName}'")
    val rule=NativeRewriteRegistry.default.resolve(operation)
    if(context.isDisabled(operation.id,rule.id))
      throw new NativeRewriteUnsupportedException(s"Native blocking rule disabled for ${operation.id}")
    NativeEvidenceCollector.recordRule(context,rule.id)
    rule(df.col(node.fieldName),None,context)
  }

  /** String.valueOf-compatible text for the hash types used by Zingg blocking. */
  private def javaStringValue(value:Column):Column=coalesce(value.cast("string"),lit("null"))

  private def suffix(df:DataFrame,node:NativeBlockingNode,context:RewriteContext):Column={
    if(node.hashFunctionName.isEmpty)
      return node.children.foldLeft(lit(""))((acc, child)=>concat(acc, suffix(df, child, context)))
    val hash=hashExpression(df,node,context)
    val self=concat(lit("|"),javaStringValue(hash))
    node.children.foldLeft(self){(acc,child)=>
      child.incomingHash match {
        case None=>acc // upstream explicitly ignores child canopies whose hash is null
        case Some(expected)=>concat(acc,when(hash.isNotNull && hash===lit(expected),suffix(df,child,context)).otherwise(lit("")))
      }
    }
  }

  /** Java String.hashCode over UTF-16 code units with exact signed 32-bit wraparound. */
  def javaStringHash(value:Column):Column={
    val hexValue=hex(encode(value,"UTF-16BE"))
    val units=floor(length(hexValue).cast("double")/lit(4.0)).cast("int")
    val unsigned=aggregate(sequence(lit(0),greatest(units-lit(1),lit(0))),lit(0L),(h,i)=>{
      val code=conv(substring(hexValue,i*lit(4)+lit(1),lit(4)),16,10).cast("long")
      pmod(h*lit(31L)+code,lit(4294967296L))
    })
    when(units===0,lit(0)).otherwise(when(unsigned>=lit(2147483648L),unsigned-lit(4294967296L)).otherwise(unsigned).cast("int"))
  }

  def compile(df:DataFrame,tree:AnyRef,outputColumn:String,context:RewriteContext):DataFrame={
    if(context.isDisabled("blocking.tree","rewrite.blocking.tree"))
      throw new NativeRewriteUnsupportedException("Native blocking-tree rewrite disabled for blocking.tree")
    val ir=NativeBlockingTreeIR.fromZingg(tree)
    val maxNodes=intSetting("zingg.native.blocking.maxNodes","ZINGG_NATIVE_BLOCKING_MAX_NODES",1024)
    val maxDepth=intSetting("zingg.native.blocking.maxDepth","ZINGG_NATIVE_BLOCKING_MAX_DEPTH",96)
    if(ir.nodeCount>maxNodes||ir.maxDepth>maxDepth)
      throw new NativeRewriteUnsupportedException(s"Blocking tree would create an unsafe native expression plan: nodes=${ir.nodeCount}/$maxNodes depth=${ir.maxDepth}/$maxDepth")
    NativeEvidenceCollector.recordRule(context,"rewrite.blocking.tree")
    NativePlanGuard.guardDataFrame(
      df.withColumn(outputColumn,javaStringHash(suffix(df,ir.root,context)).cast("int")), context)
  }
}

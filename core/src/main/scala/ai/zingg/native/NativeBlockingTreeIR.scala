package ai.zingg.native

import scala.jdk.CollectionConverters._

/** Spark-independent representation of exactly the execution information in a Zingg blocking tree. */
final case class NativeBlockingNode(
  hashFunctionName:String,
  fieldName:String,
  incomingHash:Option[AnyRef],
  children:Vector[NativeBlockingNode])
final case class NativeBlockingTreeIR(root:NativeBlockingNode,nodeCount:Int,maxDepth:Int)

object NativeBlockingTreeIR {
  private def invoke(target:AnyRef,name:String,args:AnyRef*):AnyRef={
    if(target==null) throw new NativeRewriteUnsupportedException(s"Cannot invoke $name on null Zingg blocking object")
    val candidates=target.getClass.getMethods.filter(m=>m.getName==name&&m.getParameterCount==args.size)
    if(candidates.isEmpty) throw new NativeRewriteUnsupportedException(s"${target.getClass.getName} has no $name/${args.size}")
    candidates.head.invoke(target,args:_*).asInstanceOf[AnyRef]
  }

  def fromZingg(tree:AnyRef):NativeBlockingTreeIR={
    if(tree==null) throw new IllegalArgumentException("blocking tree must not be null")
    val head=invoke(tree,"getHead")
    if(head==null) throw new NativeRewriteUnsupportedException("blocking tree has no head")
    var nodes=0; var deepest=0
    def convert(canopy:AnyRef,depth:Int):NativeBlockingNode={
      nodes+=1; deepest=math.max(deepest,depth)
      val fn=invoke(canopy,"getFunction")
      val successors=invoke(tree,"getSuccessors",canopy).asInstanceOf[java.util.Collection[AnyRef]].asScala.toVector
      // Zingg may use a functionless canopy as a structural pass-through,
      // including when it has successors. Preserve the topology and let the
      // compiler recursively emit its children.
      val ctx=if(fn==null) null else invoke(canopy,"getContext")
      if(fn!=null && ctx==null) throw new NativeRewriteUnsupportedException(s"Blocking canopy at depth $depth has no field context")
      val fnName=if(fn==null) "" else invoke(fn,"getName").toString
      val field=if(ctx==null) "" else invoke(ctx,"getFieldName").toString
      val incoming=Option(invoke(canopy,"getHash"))
      NativeBlockingNode(fnName,field,incoming,successors.map(c=>convert(c,depth+1)))
    }
    val root=convert(head,1); NativeBlockingTreeIR(root,nodes,deepest)
  }
}

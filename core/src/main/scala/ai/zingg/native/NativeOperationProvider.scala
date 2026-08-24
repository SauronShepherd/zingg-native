package ai.zingg.native

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.monotonically_increasing_id

/**
 * Integration seam called from patched Zingg 0.7 choke points. It intentionally
 * accepts only Spark public Dataset/DataFrame objects and ordinary Zingg domain
 * objects. There is no Catalyst/SparkContext/Spark Connect server-plugin API.
 */
final class NativeOperationProvider private (val spark:SparkSession,val context:RewriteContext){
  def mode:String=context.mode.id
  def shouldRewrite:Boolean=context.mode.rewrites
  def shouldAudit:Boolean=context.mode.audits
  def strict:Boolean=context.mode==NativeExecutionMode.STRICT

  private def semanticSimpleName(value:String):String=Option(value).getOrElse("").split('.').lastOption.getOrElse("")
  private def withParameters(parameters:Map[String,String]):RewriteContext=context.copy(parameters=context.parameters++parameters)

  def similarity(input:Dataset[Row],operationId:String,leftColumn:String,rightColumn:String,outputColumn:String):Dataset[Row]=
    Core.rewrite(input.toDF(),operationId,leftColumn,Some(rightColumn),outputColumn,context).asInstanceOf[Dataset[Row]]

  def similarityBatchByZinggName(
      input:Dataset[Row], semanticClassNames:Array[String], leftColumns:Array[String],
      rightColumns:Array[String], outputColumns:Array[String]):Dataset[Row] = {
    require(semanticClassNames.length == leftColumns.length &&
      leftColumns.length == rightColumns.length && rightColumns.length == outputColumns.length,
      "Similarity batch arrays must have equal lengths")
    val operations = semanticClassNames.indices.map { index =>
      val simple = semanticSimpleName(semanticClassNames(index))
      val alias = simple match {
        case "SimilarityFunctionExact" => "SimilarityFunctionExact"
        case "StringSimilarityFunction" => "StringSimilarityFunction"
        case "CheckNullFunction" => "CheckNullFunction"
        case "CheckBlankOrNullFunction" => "CheckBlankOrNullFunction"
        case "IntegerSimilarityFunction" => "IntegerSimilarityFunction"
        case "LongSimilarityFunction" => "LongSimilarityFunction"
        case "DoubleSimilarityFunction" => "DoubleSimilarityFunction"
        case "FloatSimilarityFunction" => "FloatSimilarityFunction"
        case "DateSimilarityFunction" => "DateSimilarityFunction"
        case "ArrayDoubleSimilarityFunction" => "ArrayDoubleSimilarityFunction"
        case "JaccSimFunction" => "JaccSimFunction"
        case "NumbersJaccardFunction" => "NumbersJaccardFunction"
        case "ProductCodeFunction" => "ProductCodeFunction"
        case "JaroWinklerFunction" => "JaroWinklerFunction"
        case "AJaroWinklerFunction" => "AJaroWinklerFunction"
        case "AffineGapSimilarityFunction" => "AffineGapSimilarityFunction"
        case "EmailMatchTypeFunction" => "EmailMatchTypeFunction"
        case "PinCodeMatchTypeFunction" => "PinCodeMatchTypeFunction"
        case "OnlyAlphabetsExactSimilarity" => "OnlyAlphabetsExactSimilarity"
        case "OnlyAlphabetsAffineGapSimilarity" => "OnlyAlphabetsAffineGapSimilarity"
        case "SameFirstWordFunction" => "SameFirstWordFunction"
        case other => throw new NativeRewriteUnsupportedException(s"No native similarity mapping for upstream class '$semanticClassNames(index)' ($other)")
      }
      (s"similarity.$alias", leftColumns(index), Some(rightColumns(index)), outputColumns(index))
    }
    // On managed Connect, a single batch containing both dynamic-programming
    // fuzzy expressions can become an excessively large analysis plan. When
    // the launcher supplies a transient materialization root, evaluate each
    // public expression at a named relational boundary and reread it before
    // adding the next feature. This preserves row/column semantics while
    // preventing Jaro and affine-gap plans from being fused together.
    sys.props.get("zingg.native.similarity.materializePath").filter(_.trim.nonEmpty) match {
      case Some(root) =>
        val rowIdColumn = "_native_similarity_row_id"
        // Keep each fuzzy expression in its own managed-Connect analysis
        // envelope. Pairing expressions reduced actions, but the full
        // feature grid still exposed a batch-specific stall before model
        // training. One-operation boundaries trade a few remote actions for
        // deterministic isolation of every public similarity expression.
        val materialized = operations.grouped(1).zipWithIndex.foldLeft(input.toDF()) {
          case (frame, (batch, groupIndex)) =>
            val keyedFrame = if (groupIndex == 0) frame.withColumn(rowIdColumn, monotonically_increasing_id()) else frame
            val computed = Core.rewriteColumns(keyedFrame, batch, context)
            val path = s"${root.stripSuffix("/")}/similarity-batch-${groupIndex}-${java.util.UUID.randomUUID().toString}"
            computed.write.mode("overwrite").parquet(path)
            input.sparkSession.read.parquet(path)
        }
        materialized.drop(rowIdColumn).asInstanceOf[Dataset[Row]]
      case None => Core.rewriteColumns(input.toDF(), operations, context).asInstanceOf[Dataset[Row]]
    }
  }

  /** Map the concrete upstream SimFunction class, never the UDF registration name. */
  def similarityByZinggName(input:Dataset[Row],semanticClassName:String,leftColumn:String,rightColumn:String,outputColumn:String):Dataset[Row]={
    val simple=semanticSimpleName(semanticClassName)
    val alias=simple match {
      case "SimilarityFunctionExact" => "SimilarityFunctionExact"
      case "StringSimilarityFunction" => "StringSimilarityFunction"
      case "CheckNullFunction" => "CheckNullFunction"
      case "CheckBlankOrNullFunction" => "CheckBlankOrNullFunction"
      case "IntegerSimilarityFunction" => "IntegerSimilarityFunction"
      case "LongSimilarityFunction" => "LongSimilarityFunction"
      case "DoubleSimilarityFunction" => "DoubleSimilarityFunction"
      case "FloatSimilarityFunction" => "FloatSimilarityFunction"
      case "DateSimilarityFunction" => "DateSimilarityFunction"
      case "ArrayDoubleSimilarityFunction" => "ArrayDoubleSimilarityFunction"
      case "JaccSimFunction" => "JaccSimFunction"
      case "NumbersJaccardFunction" => "NumbersJaccardFunction"
      case "ProductCodeFunction" => "ProductCodeFunction"
      case "JaroWinklerFunction" => "JaroWinklerFunction"
      case "AJaroWinklerFunction" => "AJaroWinklerFunction"
      case "AffineGapSimilarityFunction" => "AffineGapSimilarityFunction"
      case "EmailMatchTypeFunction" => "EmailMatchTypeFunction"
      case "PinCodeMatchTypeFunction" => "PinCodeMatchTypeFunction"
      case "OnlyAlphabetsExactSimilarity" => "OnlyAlphabetsExactSimilarity"
      case "OnlyAlphabetsAffineGapSimilarity" => "OnlyAlphabetsAffineGapSimilarity"
      case "SameFirstWordFunction" => "SameFirstWordFunction"
      case "BigramJaccSimFn" => throw new NativeRewriteUnsupportedException("BigramJaccSimFn is not part of the supported Zingg 0.7 execution path")
      case other => throw new NativeRewriteUnsupportedException(s"No native similarity mapping for upstream class '$semanticClassName' ($other)")
    }
    similarity(input,s"similarity.$alias",leftColumn,rightColumn,outputColumn)
  }

  def hash(input:Dataset[Row],zinggHashName:String,inputColumn:String,outputColumn:String):Dataset[Row]={
    val id=s"blocking.${Option(zinggHashName).getOrElse("")}"; Core.rewrite(input.toDF(),id,inputColumn,None,outputColumn,context).asInstanceOf[Dataset[Row]]
  }

  def preprocess(input:Dataset[Row],operationId:String,columns:Array[String]):Dataset[Row]={
    val op=operationId.trim.toLowerCase match {case "trim"|"preprocess.trim"=>"preprocess.trim";case "case_normalize"|"casenormalize"|"preprocess.casenormalize"=>"preprocess.caseNormalize";case x=>x}
    columns.foldLeft(input.toDF())((df,c)=>Core.rewrite(df,op,c,None,c,context)).asInstanceOf[Dataset[Row]]
  }

  def removeStopWords(input:Dataset[Row],fieldName:String,pattern:String):Dataset[Row]={
    val ctx=withParameters(Map("pattern"->pattern)); Core.rewrite(input.toDF(),"preprocess.stopWords",fieldName,None,fieldName,ctx).asInstanceOf[Dataset[Row]]
  }

  def vectorValue(input:Dataset[Row],inputColumn:String,outputColumn:String):Dataset[Row]=
    Core.rewrite(input.toDF(),"model.vectorValue",inputColumn,None,outputColumn,context).asInstanceOf[Dataset[Row]]

  def fitModel(input:Dataset[Row],featureColumns:Array[String],labelColumn:String):NativeTrainedModel = {
    if(!shouldRewrite) throw new IllegalStateException("fitModel is only valid in REWRITE/STRICT mode")
    NativeModelEngine.fit(input.toDF(),featureColumns.toSeq,labelColumn,context)
  }


  def loadModel(path:String):NativeTrainedModel = {
    if(!shouldRewrite) throw new IllegalStateException("loadModel is only valid in REWRITE/STRICT mode")
    NativeModelEngine.load(spark,path,context)
  }

  def saveModel(model:NativeTrainedModel,path:String):Unit = {
    if(!shouldRewrite) throw new IllegalStateException("saveModel is only valid in REWRITE/STRICT mode")
    NativeModelEngine.save(spark,model,path,context)
  }

  def predictModel(
      input:Dataset[Row],model:NativeTrainedModel,featureVectorColumn:String,expandedFeatureColumn:String,
      probabilityColumn:String,rawPredictionColumn:String,predictionColumn:String,scoreColumn:String):Dataset[Row] = {
    if(!shouldRewrite) throw new IllegalStateException("predictModel is only valid in REWRITE/STRICT mode")
    NativeModelEngine.predict(input.toDF(),model,featureVectorColumn,expandedFeatureColumn,probabilityColumn,
      rawPredictionColumn,predictionColumn,scoreColumn,context).asInstanceOf[Dataset[Row]]
  }

  def blockHashes(input:Dataset[Row],tree:AnyRef,outputColumn:String):Dataset[Row]=
    BlockingTreeCompiler.compile(input.toDF(),tree,outputColumn,context).asInstanceOf[Dataset[Row]]

  def connectedComponents(vertices:Dataset[Row],edges:Dataset[Row],idColumn:String,rightIdColumn:String,clusterColumn:String,maxIterations:Int):Dataset[Row]=
    NativeGraph.connectedComponents(vertices.toDF(),edges.toDF(),idColumn,rightIdColumn,clusterColumn,context,maxIterations).asInstanceOf[Dataset[Row]]

  def auditLegacyOperation(operationId:String,construct:String,upstreamClass:String):Unit =
    if(context.mode.audits && !context.mode.rewrites)
      NativeDiagnostics.auditLegacy(context,operationId,construct,upstreamClass)

  def analyze(phase:String,operationId:String,construct:String):NativeCompatibilityReport={
    val op=NativeOperation.resolve(operationId); NativeCompatibilityAnalyzer.analyze(phase,Seq((op,NativeRewriteRegistry.default.contains(op),construct)))
  }
  def guard(report:NativeCompatibilityReport):Unit=NativePlanGuard.requireCompatible(report,context)
  def captureEvidence(df:Dataset[Row],photonEvidence:String):NativeExecutionEvidence=
    NativeEvidenceCollector.capture(df.toDF(),context,Option(photonEvidence).filter(_.nonEmpty))
  def emitPhaseSummary():NativeExecutionEvidence=NativeEvidenceCollector.phaseSummary(context)
}

object NativeOperationProvider{
  def fromSpark(spark:SparkSession,phase:String):NativeOperationProvider={
    val modeValue=sys.props.get("zingg.native.mode").orElse(sys.env.get("ZINGG_NATIVE_MODE")).getOrElse("STRICT")
    val correlationId=sys.props.get("zingg.native.run.id").orElse(sys.env.get("ZINGG_NATIVE_RUN_ID")).getOrElse(java.util.UUID.randomUUID().toString)
    val disabled=sys.props.get("zingg.native.disabled.rules").orElse(sys.env.get("ZINGG_NATIVE_DISABLED_RULES")).getOrElse("")
    new NativeOperationProvider(spark,RewriteContext(spark,NativeExecutionMode.parse(modeValue),RuntimeDescriptor(spark.version,"2.13"),phase,correlationId,Map("disabledRules"->disabled)))
  }
}

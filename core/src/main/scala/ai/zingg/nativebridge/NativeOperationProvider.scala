package ai.zingg.nativebridge

import org.apache.spark.sql.{Dataset,Row,SparkSession}

/** Opaque Java-friendly handle for a native model artifact. */
final class NativeModelHandle private[nativebridge](private[nativebridge] val model:ai.zingg.native.NativeTrainedModel)

/** Java-source-compatible facade consumed by the Zingg 0.7 integration patch. */
final class NativeOperationProvider private(private val delegate:ai.zingg.native.NativeOperationProvider){
  def mode:String=delegate.mode
  def shouldRewrite:Boolean=delegate.shouldRewrite
  def shouldAudit:Boolean=delegate.shouldAudit
  def strict:Boolean=delegate.strict
  def similarityByZinggName(input:Dataset[Row],semanticClassName:String,leftColumn:String,rightColumn:String,outputColumn:String):Dataset[Row]=delegate.similarityByZinggName(input,semanticClassName,leftColumn,rightColumn,outputColumn)
  def similarityBatchByZinggName(input:Dataset[Row],semanticClassNames:Array[String],leftColumns:Array[String],rightColumns:Array[String],outputColumns:Array[String]):Dataset[Row]=delegate.similarityBatchByZinggName(input,semanticClassNames,leftColumns,rightColumns,outputColumns)
  def hash(input:Dataset[Row],hashName:String,inputColumn:String,outputColumn:String):Dataset[Row]=delegate.hash(input,hashName,inputColumn,outputColumn)
  def removeStopWords(input:Dataset[Row],fieldName:String,pattern:String):Dataset[Row]=delegate.removeStopWords(input,fieldName,pattern)
  def vectorValue(input:Dataset[Row],inputColumn:String,outputColumn:String):Dataset[Row]=delegate.vectorValue(input,inputColumn,outputColumn)
  def fitModel(input:Dataset[Row],featureColumns:Array[String],labelColumn:String):NativeModelHandle=new NativeModelHandle(delegate.fitModel(input,featureColumns,labelColumn))
  def loadModel(path:String):NativeModelHandle=new NativeModelHandle(delegate.loadModel(path))
  def saveModel(model:NativeModelHandle,path:String):Unit=delegate.saveModel(model.model,path)
  def predictModel(input:Dataset[Row],model:NativeModelHandle,featureVectorColumn:String,expandedFeatureColumn:String,probabilityColumn:String,rawPredictionColumn:String,predictionColumn:String,scoreColumn:String):Dataset[Row]=delegate.predictModel(input,model.model,featureVectorColumn,expandedFeatureColumn,probabilityColumn,rawPredictionColumn,predictionColumn,scoreColumn)
  def blockHashes(input:Dataset[Row],tree:Object,outputColumn:String):Dataset[Row]=delegate.blockHashes(input,tree,outputColumn)
  def connectedComponents(vertices:Dataset[Row],edges:Dataset[Row],idColumn:String,rightIdColumn:String,clusterColumn:String,maxIterations:Int):Dataset[Row]=delegate.connectedComponents(vertices,edges,idColumn,rightIdColumn,clusterColumn,maxIterations)
  def captureEvidence(input:Dataset[Row],photonEvidence:String):ai.zingg.native.NativeExecutionEvidence=delegate.captureEvidence(input,photonEvidence)
  def auditLegacyOperation(operationId:String,construct:String,upstreamClass:String):Unit=delegate.auditLegacyOperation(operationId,construct,upstreamClass)
  def emitPhaseSummary():Unit=delegate.emitPhaseSummary()
}
object NativeOperationProvider{def fromSpark(spark:SparkSession,phase:String):NativeOperationProvider=new NativeOperationProvider(ai.zingg.native.NativeOperationProvider.fromSpark(spark,phase))}

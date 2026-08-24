package ai.zingg.native.launch

import ai.zingg.nativebridge.NativeOperationProvider
import java.util.ArrayList
import org.apache.spark.sql.{Row,RowFactory,SparkSession}
import org.apache.spark.sql.types.{DataTypes,StructField,StructType}

/** Compare native blocking hashes and candidate-pair sets with the pinned legacy oracle. */
object ServerlessBlockingDifferentialProbe {
  private def invoke(target:AnyRef, name:String, args:AnyRef*):AnyRef =
    target.getClass.getMethods.find(m => m.getName == name && m.getParameterCount == args.size)
      .getOrElse(throw new IllegalArgumentException(s"Missing reflective method $name/${args.size}"))
      .invoke(target, args:_*).asInstanceOf[AnyRef]

  private def buildTree(functionName:String, width:Option[Int]):AnyRef = {
    val canopyClass = Class.forName("zingg.common.core.block.Canopy")
    val treeClass = Class.forName("zingg.common.core.block.Tree")
    val functionClass = Class.forName(s"zingg.spark.core.hash.$functionName")
    val function = width match {
      case Some(value) => functionClass.getConstructor(classOf[Int]).newInstance(Integer.valueOf(value)).asInstanceOf[AnyRef]
      case None => functionClass.getDeclaredConstructor().newInstance().asInstanceOf[AnyRef]
    }
    val field = Class.forName("zingg.common.client.FieldDefinition").getDeclaredConstructor().newInstance().asInstanceOf[AnyRef]
    invoke(field, "setFieldName", "value")
    val canopy = canopyClass.getDeclaredConstructor().newInstance().asInstanceOf[AnyRef]
    invoke(canopy, "setFunction", function)
    invoke(canopy, "setContext", field)
    treeClass.getConstructors.find(_.getParameterCount == 1)
      .getOrElse(throw new IllegalArgumentException("Tree single-head constructor is missing"))
      .newInstance(canopy).asInstanceOf[AnyRef]
  }

  private def key(value:Any):String = Option(value).map(_.toString).getOrElse("<null>")

  def run(spark:SparkSession):Unit = {
    val values = Seq[AnyRef]("Alice", "Aaron", "Bob", "", null, "Álvaro", "A  B")
    val rows = new ArrayList[Row](); values.foreach(v => rows.add(RowFactory.create(v)))
    val input = spark.createDataFrame(rows, StructType(Seq(StructField("value", DataTypes.StringType, true))))
    def candidatePairs(hashes:Seq[String]):Set[String] = hashes.zipWithIndex.groupBy(_._1).values
      .flatMap(group => group.toSeq.combinations(2).map(pair => s"${pair.head._2}:${pair.last._2}")).toSet
    val cases = Seq(
      ("first1Chars", "SparkFirstChars", Some(1)), ("first2Chars", "SparkFirstChars", Some(2)),
      ("first3Chars", "SparkFirstChars", Some(3)), ("first4Chars", "SparkFirstChars", Some(4)),
      ("last1Chars", "SparkLastChars", Some(1)), ("last2Chars", "SparkLastChars", Some(2)),
      ("last3Chars", "SparkLastChars", Some(3)),
      ("lastWord", "SparkLastWord", None))
    val passed = cases.map { case (label, functionName, width) =>
      val tree = buildTree(functionName, width)
      val native = NativeOperationProvider.fromSpark(spark, "blocking-differential")
        .blockHashes(input, tree, "_native_hash").select("_native_hash").collect().map(r => key(r.get(0))).toSeq
      val legacyFunction = Class.forName("zingg.spark.core.block.SparkBlockFunction")
        .getConstructor(Class.forName("zingg.common.core.block.Tree")).newInstance(tree).asInstanceOf[AnyRef]
      val legacy = input.collect().map(row => key(invoke(legacyFunction, "call", row).asInstanceOf[Row].get(1))).toSeq
      require(native == legacy, s"Blocking hash mismatch hash=$label native=$native legacy=$legacy")
      val nativePairs = candidatePairs(native); val legacyPairs = candidatePairs(legacy)
      require(nativePairs == legacyPairs, s"Blocking candidate-pair mismatch hash=$label native=$nativePairs legacy=$legacyPairs")
      s"$label:${nativePairs.size}"
    } :+ {
      var rejected = false
      try {
        val unsupportedTree = buildTree("SparkLastChars", Some(4))
        NativeOperationProvider.fromSpark(spark, "blocking-differential")
          .blockHashes(input, unsupportedTree, "_native_hash").collect()
      } catch {
        case _: ai.zingg.native.NativeRewriteUnsupportedException => rejected = true
      }
      require(rejected, "STRICT blocking probe must fail closed for unsupported last4Chars")
      "last4Chars:unsupported"
    }
    println(s"NATIVE_BLOCKING_DIFFERENTIAL_PASS rows=${values.size} hashes=${passed.mkString(",")}")
  }
}

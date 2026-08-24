package ai.zingg.native.launch

import ai.zingg.nativebridge.NativeOperationProvider
import java.util.ArrayList
import org.apache.spark.sql.{Row, RowFactory, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/** Isolates fuzzy feature execution from model construction and CV. */
object ServerlessFuzzyActionProbe {
  def run(spark: SparkSession): Unit = {
    val schema = StructType(Seq(
      StructField("left", DataTypes.StringType, false),
      StructField("right", DataTypes.StringType, false)))
    val rows = new ArrayList[Row]()
    val seeds = Seq(("maria garcia", "maria garcía"), ("john smith", "jon smyth"),
      ("alpha-001", "alpha-002"), ("", ""))
    val rowCount = sys.props.get("zingg.native.fuzzy.action.rows")
      .orElse(sys.props.get("zingg.native.fuzzy.rows"))
      .flatMap(_.toIntOption).getOrElse(100)
    require(rowCount > 0 && rowCount <= 100, s"Invalid fuzzy probe row count: $rowCount")
    (0 until rowCount).foreach { index =>
      val (left, right) = seeds(index % seeds.length)
      rows.add(RowFactory.create(s"$left-$index", s"$right-$index"))
    }
    val input = spark.createDataFrame(rows, schema)
    val provider = NativeOperationProvider.fromSpark(spark, "fuzzy-action-probe")
    val modelOnly = sys.props.get("zingg.native.fuzzy.modelOnly").exists(_.equalsIgnoreCase("true"))
    if (modelOnly) {
      System.setProperty("zingg.native.model.maxIter", "1")
      System.setProperty("zingg.native.model.boundedProbe", "true")
      System.setProperty("zingg.native.model.materializePath", "/Volumes/sda_dev/default/zingg_native_e2e_volume/probes/fuzzy-action-model-only")
      val modeled = provider.similarityBatchByZinggName(
        input.withColumn("label", (monotonically_increasing_id() % 2L).cast("double")),
        Array("JaroWinklerFunction", "AffineGapSimilarityFunction"),
        Array("left", "left"), Array("right", "right"), Array("jaro_model", "affine_model"))
      val handle = provider.fitModel(modeled, Array("jaro_model", "affine_model"), "label")
      require(handle != null, "Fuzzy model-only integration returned no model handle")
      println(s"FUZZY_ACTION_MODEL_ONLY_PASS features=2 degree=3 bounded=true rows=$rowCount")
      return
    }
    val onlyRule = sys.props.get("zingg.native.fuzzy.action.rule").map(_.trim).filter(_.nonEmpty)
    val runJaro = onlyRule.forall(_.equalsIgnoreCase("JaroWinklerFunction"))
    val runAffine = onlyRule.forall(_.equalsIgnoreCase("AffineGapSimilarityFunction"))
    require(runJaro || runAffine, s"Unsupported fuzzy action rule: ${onlyRule.getOrElse("")}")
    val stages = scala.collection.mutable.ArrayBuffer.empty[String]
    if (runJaro) {
      val jaro = provider.similarityByZinggName(input, "JaroWinklerFunction", "left", "right", "jaro")
      jaro.select("jaro").write.mode("overwrite").parquet("/Volumes/sda_dev/default/zingg_native_e2e_volume/probes/fuzzy-action-jaro")
      val jaroRows = spark.read.parquet("/Volumes/sda_dev/default/zingg_native_e2e_volume/probes/fuzzy-action-jaro").count()
      require(jaroRows == rowCount, s"Jaro probe row count mismatch: $jaroRows")
      stages += "jaro"
      println(s"FUZZY_ACTION_STAGE_PASS stage=jaro rows=$rowCount")
    }
    if (runAffine) {
      val affine = provider.similarityByZinggName(input, "AffineGapSimilarityFunction", "left", "right", "affine")
      affine.select("affine").write.mode("overwrite").parquet("/Volumes/sda_dev/default/zingg_native_e2e_volume/probes/fuzzy-action-affine")
      val affineRows = spark.read.parquet("/Volumes/sda_dev/default/zingg_native_e2e_volume/probes/fuzzy-action-affine").count()
      require(affineRows == rowCount, s"Affine probe row count mismatch: $affineRows")
      stages += "affine"
      println(s"FUZZY_ACTION_STAGE_PASS stage=affine rows=$rowCount")
    }
    println(s"FUZZY_ACTION_PROBE_PASS stages=${stages.mkString(",")} rows=$rowCount")
    if (sys.props.get("zingg.native.fuzzy.actionsOnly").exists(_.equalsIgnoreCase("true"))) return
    System.setProperty("zingg.native.model.maxIter", "1")
    System.setProperty("zingg.native.model.boundedProbe", "true")
    System.setProperty("zingg.native.model.materializePath", "/Volumes/sda_dev/default/zingg_native_e2e_volume/probes/fuzzy-action-model")
    val modeled = provider.similarityBatchByZinggName(
      input.withColumn("label", (monotonically_increasing_id() % 2L).cast("double")),
      Array("JaroWinklerFunction", "AffineGapSimilarityFunction"),
      Array("left", "left"), Array("right", "right"), Array("jaro_model", "affine_model"))
    val handle = provider.fitModel(modeled, Array("jaro_model", "affine_model"), "label")
    require(handle != null, "Fuzzy action/model integration returned no model handle")
    println(s"FUZZY_ACTION_MODEL_PROBE_PASS features=2 degree=3 bounded=true rows=$rowCount")
  }
}

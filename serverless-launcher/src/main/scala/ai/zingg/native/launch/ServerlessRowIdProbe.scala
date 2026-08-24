package ai.zingg.native.launch

import java.util.ArrayList
import org.apache.spark.sql.{Row,RowFactory,SparkSession}
import org.apache.spark.sql.types.{DataTypes,StructField,StructType}

/** Runtime contract probe for opaque native row IDs used during feature reassembly. */
object ServerlessRowIdProbe {
  def run(spark:SparkSession):Unit = {
    val rows = new ArrayList[Row](); (0 until 32).foreach(i => rows.add(RowFactory.create(s"value-$i")))
    val input = spark.createDataFrame(rows, StructType(Seq(StructField("value", DataTypes.StringType, false))))
    val module = Class.forName("zingg.scala.DFUtil$").getField("MODULE$").get(null).asInstanceOf[AnyRef]
    val method = module.getClass.getMethods.find(m => m.getName == "addRowNumber" && m.getParameterCount == 2)
      .getOrElse(throw new IllegalArgumentException("DFUtil.addRowNumber is missing"))
    val output = method.invoke(module, input, spark).asInstanceOf[org.apache.spark.sql.DataFrame]
      .select("value", "z_zid").collect()
    val ids = output.map(_.getLong(1))
    require(ids.forall(_ >= 0L), "native row IDs must be non-negative")
    require(ids.distinct.length == ids.length && ids.length == 32, s"native row-ID uniqueness failed rows=${ids.length} distinct=${ids.distinct.length}")
    println(s"NATIVE_ROW_ID_CONTRACT_PASS rows=${ids.length} distinct=${ids.distinct.length} publishedClusterId=false crossJobStable=false")
  }
}

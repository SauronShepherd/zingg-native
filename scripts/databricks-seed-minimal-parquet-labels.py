from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
base = "/Volumes/sda_dev/default/zingg_native_e2e_volume/models-minimal-parquet/100/trainingData"
source = spark.read.parquet(base + "/unmarked")
marked = source.withColumn("z_isMatch", F.when((F.col("z_zid") % 2) == 0, F.lit(1)).otherwise(F.lit(0)).cast("int"))
marked.write.mode("overwrite").parquet(base + "/marked")
print("SYNTHETIC_MINIMAL_PARQUET_MARKED=" + str(marked.count()))

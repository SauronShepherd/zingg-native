from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
base = "/Volumes/sda_dev/default/zingg_native_e2e_volume/models-job-v9/100/trainingData"
source = spark.read.parquet(base + "/unmarked")
# This is a test-only fixture rule, never a production labeling algorithm:
# the bundled test data names duplicate records with the '-dup-' marker.
marked = source.withColumn(
    "z_isMatch",
    F.when(F.col("id").contains("-dup-"), F.lit(1)).otherwise(F.lit(0)).cast("int")
)
marked.write.mode("overwrite").parquet(base + "/marked")
print("SYNTHETIC_MARKED_COUNT=" + str(marked.count()))
marked.groupBy("z_isMatch").count().show()

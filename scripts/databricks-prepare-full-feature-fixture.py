from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
root = "/Volumes/sda_dev/default/zingg_native_e2e_volume"
source = spark.read.parquet(root + "/input-minimal-parquet")
ordered = source.withColumn(
    "z_zid",
    (F.row_number().over(Window.orderBy(F.col("id"))) - F.lit(1)).cast("long"),
)
for name in ("fname", "lname", "stNo", "add1", "add2", "city", "areacode", "state", "dob", "ssn"):
    ordered = ordered.withColumn(name + "_2", F.col(name))
ordered.write.mode("overwrite").parquet(root + "/input-full-20-feature")
marked = ordered.withColumn(
    "z_isMatch",
    # Deterministic test-only labels keep both classes present even when the
    # source fixture has already been normalized to duplicate-looking IDs.
    F.when((F.col("z_zid") % F.lit(2)) == F.lit(0), F.lit(1)).otherwise(F.lit(0)).cast("int"),
).withColumn("z_cluster", F.regexp_replace(F.col("id"), "-dup-.*$", ""))
training_root = root + "/models-job-v10/100/trainingData"
ordered.write.mode("overwrite").parquet(training_root + "/unmarked")
marked.write.mode("overwrite").parquet(training_root + "/marked")
print("FULL_FEATURE_MARKED_ROWS=" + str(marked.count()))
marked.groupBy("z_isMatch").count().show()

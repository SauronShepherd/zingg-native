from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
path = "/Volumes/sda_dev/default/zingg_native_e2e_volume/models-job-v9/100/trainingData/unmarked"
df = spark.read.parquet(path)
print("TRAINING_SCHEMA=" + df.schema.json())
print("TRAINING_COUNT=" + str(df.count()))
df.limit(5).show(truncate=False)

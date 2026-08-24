from pyspark.sql import functions as F

source = "/Volumes/sda_dev/default/zingg_native_e2e_volume/models-job-v9/100/trainingData/marked"
target = "/Volumes/sda_dev/default/zingg_native_e2e_volume/models-job-v10/100/trainingData/marked"
marked = spark.read.parquet(source)
marked.write.mode("overwrite").parquet(target)
print("FULL_FEATURE_MARKED_ROWS=" + str(marked.count()))

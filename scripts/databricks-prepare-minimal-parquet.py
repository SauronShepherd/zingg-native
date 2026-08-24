from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
source = spark.read.option("header", "false").option("delimiter", ",").schema(
    "id string, fname string, lname string, stNo string, add1 string, add2 string, city string, areacode string, state string, dob string, ssn string"
).csv("/Workspace/Shared/zingg-native/e2e/test.csv")
target = "/Volumes/sda_dev/default/zingg_native_e2e_volume/input-minimal-parquet"
source.write.mode("overwrite").parquet(target)
print("MINIMAL_PARQUET_ROWS=" + str(source.count()))

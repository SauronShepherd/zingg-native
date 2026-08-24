from pyspark.sql import types as T
from pyspark.sql import functions as F

target = "/Volumes/sda_dev/default/zingg_native_e2e_volume/models-four-feature-marked/100/trainingData/marked"
input_target = "/Volumes/sda_dev/default/zingg_native_e2e_volume/input-tiny-marked"
schema = T.StructType([
    T.StructField("z_zid", T.LongType(), False),
    T.StructField("z_isMatch", T.IntegerType(), False),
    T.StructField("z_cluster", T.StringType(), False),
    T.StructField("fname", T.StringType(), True),
    T.StructField("lname", T.StringType(), True),
    T.StructField("stNo", T.StringType(), True),
    T.StructField("add1", T.StringType(), True),
])
rows = [
    (1, 1, "c1", "ana", "garcia", "10", "main street"),
    (2, 1, "c1", "ana", "garcia", "10", "main street"),
    (3, 0, "c2", "bob", "smith", "77", "oak avenue"),
    (4, 0, "c2", "carla", "jones", "88", "pine road"),
    (5, 1, "c4", "diego", "lopez", "20", "central plaza"),
    (6, 1, "c4", "diego", "lopez", "20", "central plaza"),
    (7, 0, "c5", "erin", "brown", "91", "river lane"),
    (8, 0, "c5", "finn", "white", "32", "hill drive"),
    (9, 1, "c7", "gina", "lee", "44", "market street"),
    (10, 1, "c7", "gina", "lee", "44", "market street"),
    (11, 0, "c8", "hugo", "king", "55", "garden walk"),
    (12, 0, "c8", "iris", "stone", "66", "forest path"),
    (13, 1, "c9", "jane", "miller", "71", "lake road"),
    (14, 1, "c9", "jane", "miller", "71", "lake road"),
    (15, 0, "c10", "karl", "young", "82", "south street"),
    (16, 0, "c10", "lisa", "green", "93", "north street"),
    (17, 1, "c11", "maria", "rodriguez", "105", "east avenue"),
    (18, 1, "c11", "maria", "rodriguez", "105", "east avenue"),
    (19, 0, "c12", "nora", "adams", "116", "west road"),
    (20, 0, "c12", "oscar", "baker", "127", "west road"),
]
marked = spark.createDataFrame(rows, schema)
marked.write.mode("overwrite").parquet(target)
input_frame = marked.select("z_zid", "fname", "lname", "stNo", "add1")
for offset in range(1, 2):
    input_frame = input_frame.unionByName(
        marked.select(
            (F.col("z_zid") + F.lit(offset * 1000)).alias("z_zid"),
            "fname", "lname", "stNo", "add1"))
input_frame.write.mode("overwrite").parquet(input_target)
print("FOUR_FEATURE_MARKED_ROWS=" + str(marked.count()))

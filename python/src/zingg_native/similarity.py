"""Similarity expressions implemented exclusively with Spark SQL expressions."""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def exact_similarity(left: Column | str, right: Column | str) -> Column:
    """Return Zingg 0.7 EXACT semantics as a native Spark expression.

    Zingg's reference implementation returns 1.0 when either operand is null,
    including the null/null case. Non-null equal values score 1.0; all other
    values score 0.0.
    """
    left = F.col(left) if isinstance(left, str) else left
    right = F.col(right) if isinstance(right, str) else right
    return F.when(left.isNull() | right.isNull(), F.lit(1.0)).otherwise(
        F.when(left == right, F.lit(1.0)).otherwise(F.lit(0.0))
    )


def exact(df: DataFrame, left: str, right: str, output: str = "z_exact") -> DataFrame:
    """Add an exact-similarity column to ``df`` without a UDF."""
    return df.withColumn(output, exact_similarity(F.col(left), F.col(right)))


def jaccard_similarity(left: Column | str, right: Column | str) -> Column:
    """Native equivalent of Zingg 0.7 JaccSimFunction.

    The upstream SimpleTokenizer lowercases and emits contiguous letter or
    digit runs, ignoring punctuation. Empty/null inputs score 1.0.
    """
    left = F.col(left) if isinstance(left, str) else left
    right = F.col(right) if isinstance(right, str) else right
    def tokens(value: Column) -> Column:
        normalized = F.trim(F.regexp_replace(F.lower(value.cast("string")), r"[^\p{L}\p{N}]+", " "))
        return F.array_distinct(F.filter(F.split(normalized, " "), lambda x: x != F.lit("")))

    lt, rt = tokens(left), tokens(right)
    intersection = F.size(F.array_intersect(lt, rt)).cast("double")
    union = (F.size(lt) + F.size(rt) - F.size(F.array_intersect(lt, rt))).cast("double")
    return F.when(left.isNull() | right.isNull() | (F.length(left) == 0) | (F.length(right) == 0), F.lit(1.0)).otherwise(
        F.when(union == 0, F.lit(0.0)).otherwise(intersection / union)
    )


def jaro_similarity(left: Column | str, right: Column | str) -> Column:
    """Native Spark expression for SecondString's Zingg 0.7 Jaro score.

    The implementation follows SecondString's matching-window algorithm:
    lowercase inputs, mark each matched character at most once, compare the
    two ordered common-character sequences, and divide transpositions by two.
    """
    left = F.col(left) if isinstance(left, str) else left
    right = F.col(right) if isinstance(right, str) else right
    first = F.lower(left.cast("string"))
    second = F.lower(right.cast("string"))
    n, m = F.length(first), F.length(second)
    chars1, chars2 = F.split(first, ""), F.split(second, "")
    half = F.floor(F.least(n, m) / F.lit(2)).cast("int") + F.lit(1)

    def common(source: Column, target: Column, source_len: Column) -> Column:
        indexes = F.sequence(F.lit(0), F.greatest(source_len - F.lit(1), F.lit(0)))
        initial = F.struct(
            F.array().cast("array<string>").alias("common"),
            target.alias("remaining"),
        )

        def select_match(state: Column, i: Column) -> Column:
            start = F.greatest(F.lit(0), i - half)
            # SecondString loops while j < i + half, so the inclusive SQL
            # sequence endpoint is i + half - 1.
            stop = F.least(F.size(state["remaining"]) - F.lit(1), i + half - F.lit(1))
            candidates = F.sequence(start, F.greatest(start, stop))
            found = F.aggregate(
                candidates,
                F.struct(F.lit(False).alias("matched"), F.lit(-1).alias("index")),
                lambda hit, j: F.when(hit["matched"], hit).otherwise(
                    F.when(
                        F.element_at(source, i + F.lit(1)) == F.element_at(state["remaining"], j + F.lit(1)),
                        F.struct(F.lit(True).alias("matched"), j.alias("index")),
                    ).otherwise(hit)
                ),
            )
            marked = F.transform(
                state["remaining"],
                lambda value, j: F.when(j == found["index"], F.lit("*" )).otherwise(value),
            )
            return F.struct(
                F.when(found["matched"], F.concat(state["common"], F.array(F.element_at(source, i + F.lit(1)))))
                 .otherwise(state["common"]).alias("common"),
                F.when(found["matched"], marked).otherwise(state["remaining"]).alias("remaining"),
            )

        return F.aggregate(indexes, initial, select_match)["common"]

    common1 = common(chars1, chars2, n)
    common2 = common(chars2, chars1, m)
    common_count = F.size(common1)
    mismatches = F.aggregate(
        F.zip_with(common1, common2, lambda a, b: (a != b).cast("int")),
        F.lit(0),
        lambda total, mismatch: total + F.coalesce(mismatch, F.lit(0)),
    )
    score = ((common_count / n) + (common_count / m) + ((common_count - F.floor(mismatches / F.lit(2))) / common_count)) / F.lit(3.0)
    return F.when(left.isNull() | right.isNull() | (F.length(left) == 0) | (F.length(right) == 0), F.lit(1.0)).otherwise(
        F.when((common_count == 0) | (n == 0) | (m == 0), F.lit(0.0)).otherwise(score)
    )

package ai.zingg.native

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

/**
 * Photon/Serverless-safe rewrites for the row-level operations Zingg 0.7
 * previously expressed as Scala UDFs. Every rule is built exclusively from
 * public Spark SQL/DataFrame expressions so the same logical expression can be
 * serialized through Spark Connect or executed by Spark Classic.
 */
object NativeExpressions {
  private val EmptyStringArray = array().cast("array<string>")
  private val EmptyIntArray = array().cast("array<int>")

  /**
   * Java String/Zingg string algorithms operate on UTF-16 code units, while
   * Spark SQL string length/substring are Unicode-code-point based.  Keep the
   * public-expression implementation byte based so blocking hashes and
   * SecondString algorithms retain Java String semantics without a JVM UDF.
   */
  private def utf16Hex(value: Column): Column = upper(hex(encode(value.cast("string"), "UTF-16BE")))
  private def utf16Length(value: Column): Column = (length(utf16Hex(value)) / lit(4)).cast("int")
  private def indexArray(sizeValue: Column, oneBased: Boolean = false): Column = {
    val start = if (oneBased) lit(1) else lit(0)
    val stop = if (oneBased) sizeValue else sizeValue - lit(1)
    when(sizeValue <= 0, EmptyIntArray).otherwise(sequence(start, stop))
  }
  private def utf16Units(value: Column): Column = {
    val h = utf16Hex(value)
    val n = utf16Length(value)
    transform(indexArray(n), i => substring(h, i * lit(4) + lit(1), lit(4)))
  }
  private def decodeUtf16Hex(value: Column): Column = decode(unhex(value), "UTF-16BE")
  private def utf16Prefix(value: Column, count: Int): Column = {
    val h = utf16Hex(value); val n = utf16Length(value); val take = least(n, lit(count))
    when(value.isNull, lit(null).cast("string"))
      .otherwise(decodeUtf16Hex(substring(h, lit(1), take * lit(4))))
  }
  private def utf16Suffix(value: Column, count: Int): Column = {
    val h = utf16Hex(value); val n = utf16Length(value); val take = least(n, lit(count))
    val start = (n - take) * lit(4) + lit(1)
    when(value.isNull, lit(null).cast("string"))
      .otherwise(decodeUtf16Hex(substring(h, start, take * lit(4))))
  }

  def stringBase(left: Column, right: Column)(body: => Column): Column =
    when(left.isNull || right.isNull || length(left.cast("string")) === 0 || length(right.cast("string")) === 0, lit(1.0))
      .when(left.cast("string") === right.cast("string"), lit(1.0))
      .otherwise(body)

  def exact(left: Column, right: Column): Column =
    when(left.isNull || right.isNull, lit(1.0)).when(left === right, lit(1.0)).otherwise(lit(0.0))

  def checkNull(left: Column, right: Column): Column =
    when(left.isNotNull && right.isNotNull, lit(1.0)).otherwise(lit(0.0))

  def checkBlankOrNull(left: Column, right: Column): Column =
    when(left.isNotNull && right.isNotNull && length(left.cast("string")) =!= 0 && length(right.cast("string")) =!= 0, lit(1.0)).otherwise(lit(0.0))

  private val Two32=BigDecimal("4294967296")
  private val Half32=BigDecimal("2147483648")
  private val Two64=BigDecimal("18446744073709551616")
  private val Half64=BigDecimal("9223372036854775808")
  private def wrapSigned(value:Column,bits:Int):Column={
    val (mod,half)=if(bits==32)(Two32,Half32)else(Two64,Half64)
    pmod(value.cast("decimal(38,0)")+lit(half),lit(mod))-lit(half)
  }

  def integerSimilarity(left: Column, right: Column): Column = {
    val l=left.cast("int"); val r=right.cast("int")
    val sum=wrapSigned(l.cast("decimal(38,0)")+r.cast("decimal(38,0)"),32)
    val diff=wrapSigned(l.cast("decimal(38,0)")-r.cast("decimal(38,0)"),32)
    val javaAbs=when(diff===lit(BigDecimal("-2147483648")),diff).otherwise(abs(diff))
    when(l.isNull || r.isNull || sum===lit(0),lit(0.0)).otherwise(lit(2.0)*javaAbs.cast("double")/sum.cast("double"))
  }

  def longSimilarity(left: Column, right: Column): Column = {
    val l=left.cast("long"); val r=right.cast("long")
    val sum=wrapSigned(l.cast("decimal(38,0)")+r.cast("decimal(38,0)"),64)
    val diff=wrapSigned(l.cast("decimal(38,0)")-r.cast("decimal(38,0)"),64)
    val javaAbs=when(diff===lit(BigDecimal("-9223372036854775808")),diff).otherwise(abs(diff))
    when(l.isNull || r.isNull || sum===lit(0),lit(0.0)).otherwise(lit(2.0)*javaAbs.cast("double")/sum.cast("double"))
  }

  def floatingSimilarity(left: Column, right: Column, dataType: String): Column = {
    val l=left.cast(dataType); val r=right.cast(dataType)
    val difference=if(dataType=="float") abs((l-r).cast("float")).cast("double") else abs(l-r).cast("double")
    when(l.isNull || r.isNull || isnan(l.cast("double")) || isnan(r.cast("double")),lit(1.0))
      .otherwise(lit(1.0)-difference/(lit(1.0)+l.cast("double")+r.cast("double")))
  }

  def dateSimilarity(left: Column, right: Column): Column = {
    val l=unix_millis(left.cast("timestamp")); val r=unix_millis(right.cast("timestamp"))
    val diff=wrapSigned(l.cast("decimal(38,0)")-r.cast("decimal(38,0)"),64)
    val sum=wrapSigned(l.cast("decimal(38,0)")+r.cast("decimal(38,0)")+lit(1),64)
    // DateSimilarityFunction converts to double before Math.abs, unlike the
    // Integer/Long functions which call Math.abs on the wrapped primitive.
    when(left.isNull || right.isNull,lit(1.0))
      .otherwise(lit(1.0)-abs(diff.cast("double")/sum.cast("double")))
  }

  def arrayDoubleSimilarity(left: Column, right: Column): Column = {
    val l = left.cast("array<double>"); val r = right.cast("array<double>")
    val sameSize = size(l) === size(r)
    val hasNull = exists(l, x => x.isNull) || exists(r, x => x.isNull)
    val dot = aggregate(zip_with(l, r, (a,b) => a*b), lit(0.0), (a,b) => a + coalesce(b,lit(0.0)))
    val ln = sqrt(aggregate(l, lit(0.0), (a,b) => a + b*b))
    val rn = sqrt(aggregate(r, lit(0.0), (a,b) => a + b*b))
    val cosine = abs(dot / (ln*rn))
    when(l.isNull || r.isNull || size(l) === 0 || size(r) === 0 || !sameSize || hasNull, lit(0.0))
      .when(ln > 0.0 && rn > 0.0, least(lit(1.0), cosine)).otherwise(lit(0.0))
  }

  /** SecondString SimpleTokenizer-compatible tokens: letter runs and digit runs, case-insensitive. */
  def simpleTokens(value: Column): Column = array_distinct(regexp_extract_all(lower(value.cast("string")), lit("[\\p{L}]+|[\\p{N}]+"), lit(0)))

  def jaccard(left: Column, right: Column): Column = stringBase(left,right) {
    val l = simpleTokens(left); val r = simpleTokens(right)
    val union = size(array_union(l,r))
    when(union === 0, lit(0.0)).otherwise(size(array_intersect(l,r)).cast("double") / union.cast("double"))
  }

  def numbersJaccard(left: Column, right: Column): Column = {
    val l = array_distinct(regexp_extract_all(left.cast("string"), lit("[0-9]+"), lit(0)))
    val r = array_distinct(regexp_extract_all(right.cast("string"), lit("[0-9]+"), lit(0)))
    val union = size(array_union(l,r))
    when(left.isNull || right.isNull || length(left.cast("string")) === 0 || length(right.cast("string")) === 0, lit(0.0))
      .when(size(l) === 0 || size(r) === 0 || union === 0, lit(0.0))
      .otherwise(size(array_intersect(l,r)).cast("double") / union.cast("double"))
  }

  private val ProductCodeRegex = "\\s[a-zA-Z]{0,4}\\s\\d+\\s([a-zA-Z]{0,4}\\s)?|(\\s?([a-z0-9A-Z]*\\d+(\\.\\d+)?[a-z0-9A-Z]*)\\s?)"
  def productCode(left: Column, right: Column): Column = {
    def codes(c:Column) = when(c.isNull || length(c.cast("string"))===0,EmptyStringArray).otherwise(
      array_distinct(transform(regexp_extract_all(c.cast("string"),lit(ProductCodeRegex),lit(0)),x=>regexp_replace(x," ",""))))
    val l=codes(left); val r=codes(right); val union=size(array_union(l,r)); val both=size(l)>0 && size(r)>0
    when(size(l)===0 && size(r)===0,lit(1.0))
      .when(both && union>0,size(array_intersect(l,r)).cast("double")/union.cast("double"))
      .otherwise(lit(0.0))
  }

  /** SecondString Jaro implementation used by Zingg's SJaroWinkler class. */
  def jaro(left: Column, right: Column): Column = {
    val first = left.cast("string"); val second = right.cast("string")
    // SecondString's Jaro implementation normalizes case before matching.
    // Apply the same public-expression normalization to UTF-16 units so the
    // reference behavior is preserved for mixed-case inputs.
    val chars1 = transform(utf16Units(first), unit => lowerAffineUnit(unit))
    val chars2 = transform(utf16Units(second), unit => lowerAffineUnit(unit))
    val n = size(chars1); val m = size(chars2)
    val half = floor(least(n,m) / lit(2)).cast("int") + lit(1)
    // The working arrays contain UTF-16 code units encoded as four hex
    // characters. Do not use 002A as the marker: that is a real '*' code unit
    // and would incorrectly consume literal asterisks in input data.
    val marker = lit("__NATIVE_JARO_USED__")

    // SecondString greedily scans the first string and consumes the first
    // unused equal character in the second string's match window. Carry the
    // consumed positions as relational array state so duplicate and combining
    // characters have the same one-to-one behavior without a UDF/RDD.
    val emptyIntArray = array().cast("array<int>")
    val initialMatches = struct(
      emptyIntArray.alias("used"),
      emptyIntArray.alias("sources"),
      emptyIntArray.alias("targets"))
    val matches = aggregate(indexArray(size(chars1)), initialMatches, (state, i) => {
      val start = greatest(lit(0), i - half)
      // SecondString iterates j < min(i + halflen + 1, length), so the
      // zero-based upper bound is inclusive at i + half.
      val stop = least(size(chars2) - lit(1), i + half)
      val sourceValue = element_at(chars1, i + lit(1))
      // The Jaro window is already bounded by [start, stop].  Building and
      // filtering the complete right-hand index array here makes every row
      // pay O(|right|) work for every source character and produces a very
      // large higher-order Connect plan.  Scan only window offsets and map
      // them back to the original zero-based target indexes.  This preserves
      // the greedy first-unused candidate semantics exactly.
      val windowLength = greatest(lit(0), stop - start + lit(1))
      val candidates = transform(filter(indexArray(windowLength), offset => {
        val j = start + offset
        element_at(chars2, j + lit(1)) === sourceValue &&
          !exists(state.getField("used"), used => used === j)
      }), offset => start + offset)
      val candidate = try_element_at(candidates, lit(1))
      when(candidate.isNotNull,
        struct(
          concat(state.getField("used"), array(candidate)).alias("used"),
          concat(state.getField("sources"), array(i)).alias("sources"),
          concat(state.getField("targets"), array(candidate)).alias("targets")))
        .otherwise(state)
    })
    val c1 = transform(matches.getField("sources"), i => element_at(chars1, i + lit(1)))
    val c2 = transform(array_sort(matches.getField("targets")), i => element_at(chars2, i + lit(1)))
    val count = size(c1)
    val mismatches = aggregate(zip_with(c1,c2,(a,b)=>(a =!= b).cast("int")),lit(0),(a,b)=>a+coalesce(b,lit(0)))
    val transpositions = floor(mismatches.cast("double") / lit(2.0))
    val score = (count.cast("double")/n.cast("double") + count.cast("double")/m.cast("double") +
      (count.cast("double")-transpositions)/count.cast("double")) / lit(3.0)
    // Avoid evaluating the bounded matching scan for the common exact-value
    // case.  The normalized arrays preserve the reference case-insensitive
    // semantics, and stringBase still owns null/blank handling.
    stringBase(left,right) { when(chars1 === chars2, lit(1.0)).otherwise(when(size(c1) =!= size(c2) || count === 0, lit(0.0)).otherwise(score)) }
  }

  // MongeElkan calls Character.toLowerCase(char) before equality/group tests.
  // Preserve surrogate code units as-is; for normal BMP chars Spark lower() is
  // a public expression and gives the required case-insensitive comparison.
  // Normalize each source array once rather than repeating decode/lower inside
  // every affine dynamic-programming cell.
  private def lowerAffineUnit(unit: Column): Column = {
    val c = conv(unit, 16, 10).cast("int")
    when(c >= 0xD800 && c <= 0xDFFF, unit)
      .otherwise(utf16Hex(lower(decodeUtf16Hex(unit))))
  }

  private def affineCharScore(aUnit: Column, bUnit: Column): Column = {
    def code(unit: Column): Column = conv(unit, 16, 10).cast("int")
    val x = aUnit; val y = bUnit
    // Keep the same MongeElkan approximate-character groups, but express
    // membership as scalar public predicates. Constructing two temporary
    // arrays for every dynamic-programming cell creates avoidable higher-order
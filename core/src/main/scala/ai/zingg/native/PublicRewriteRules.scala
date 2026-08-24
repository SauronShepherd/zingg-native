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
      val stop = least(size(chars2) - lit(1), i + half - lit(1))
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
    // work on Serverless while being semantically identical.
    val approximateGroups = Seq("dt","gj","rl","mn","pbv","aeuio",",.")
      .map(_.toCharArray.map(ch => f"${ch.toInt}%04X").toVector)
    val approximate = approximateGroups.map { group =>
      val xInGroup = group.map(code => x === lit(code)).reduce(_ || _)
      val yInGroup = group.map(code => y === lit(code)).reduce(_ || _)
      xInGroup && yInGroup
    }.reduce(_ || _)
    when(x === y, lit(5.0)).when(approximate, lit(3.0)).otherwise(lit(-3.0))
  }

  /**
   * SecondString AffineGap/MongeElkan score expressed as higher-order Spark SQL.
   * State contains the previous M/S rows and the best score; the inner fold
   * builds the current M/S/T rows. No JVM row callback is used.
   */
  def affineGap(left:Column,right:Column):Column={
    val a = utf16Units(left.cast("string")); val b = utf16Units(right.cast("string")); val n=size(a); val m=size(b)
    val normalizedA = transform(a, unit => lowerAffineUnit(unit))
    val normalizedB = transform(b, unit => lowerAffineUnit(unit))
    val zeros=transform(sequence(lit(0),m),_=>lit(0.0))
    val initial=struct(zeros.alias("mrow"),zeros.alias("srow"),lit(0.0).alias("best"))
    val rows=aggregate(indexArray(n, oneBased = true),initial,(outer,i)=>{
      val innerInit=struct(array(lit(0.0)).alias("mrow"),array(lit(0.0)).alias("srow"),array(lit(0.0)).alias("trow"),outer.getField("best").alias("best"))
      val row=aggregate(indexArray(m, oneBased = true),innerInit,(state,j)=>{
        val score=affineCharScore(element_at(normalizedA,i),element_at(normalizedB,j))
        val diagM=element_at(outer.getField("mrow"),j)+score
        val diagS=element_at(outer.getField("srow"),j)+score
        val diagT=element_at(state.getField("trow"),j)+score
        val mv=greatest(lit(0.0),diagM,diagS,diagT)
        val sv=greatest(lit(0.0),element_at(outer.getField("mrow"),j+lit(1))-5.0,element_at(outer.getField("srow"),j+lit(1))-1.0)
        val tv=greatest(lit(0.0),element_at(state.getField("mrow"),j)-5.0,element_at(state.getField("trow"),j)-1.0)
        struct(concat(state.getField("mrow"),array(mv)).alias("mrow"),concat(state.getField("srow"),array(sv)).alias("srow"),
          concat(state.getField("trow"),array(tv)).alias("trow"),greatest(state.getField("best"),mv,sv,tv).alias("best"))
      })
      struct(row.getField("mrow").alias("mrow"),row.getField("srow").alias("srow"),row.getField("best").alias("best"))
    })
    val denominator=least(n,m).cast("double")*5.0
    // Exact normalized strings have the reference maximum score and do not
    // need the quadratic dynamic-programming state at runtime.
    stringBase(left,right){when(normalizedA === normalizedB, lit(1.0)).otherwise(when(denominator<=0.0,lit(0.0)).otherwise(rows.getField("best")/denominator))}
  }

  def email(left:Column,right:Column):Column={
    def local(c:Column)=element_at(split(c.cast("string"),"@",-1),1)
    stringBase(left,right){affineGap(local(left),local(right))}
  }
  def pin(left:Column,right:Column):Column={
    def part(c:Column)=element_at(split(c.cast("string"),"-",-1),1)
    stringBase(left,right){when(part(left)===part(right),lit(1.0)).otherwise(lit(0.0))}
  }
  def onlyAlphaExact(left:Column,right:Column):Column=stringBase(left,right){
    when(regexp_replace(left.cast("string"),"[0-9.]","")===regexp_replace(right.cast("string"),"[0-9.]",""),lit(1.0)).otherwise(lit(0.0))
  }
  def onlyAlphaAffine(left:Column,right:Column):Column=stringBase(left,right){
    affineGap(regexp_replace(left.cast("string"),"[0-9.]",""),regexp_replace(right.cast("string"),"[0-9.]",""))
  }
  def sameFirstWord(left:Column,right:Column):Column=stringBase(left,right){
    affineGap(element_at(split(left.cast("string"),"-",-1),1),element_at(split(right.cast("string"),"-",-1),1))
  }

  def javaLastWord(value:Column):Column={
    val s=value.cast("string"); val withoutTrailing=regexp_replace(s," +$","")
    when(s.isNull,s).when(length(s)===0,s).when(length(withoutTrailing)===0,lit(null).cast("string"))
      .otherwise(element_at(split(withoutTrailing," ",-1),-1))
  }
  def firstChars(value:Column,n:Int):Column=utf16Prefix(value,n)
  def lastChars(value:Column,n:Int):Column=utf16Suffix(value,n)
  def first2Box(value:Column):Column={val s=value.cast("string");val p=utf16Prefix(s,2);when(s.isNull || utf16Length(s)<=2,0).when(p>="aa"&&p<"jz",1).when(p>="jz"&&p<"oz",2).when(p>="oz",3).otherwise(4)}
  def first3Box(value:Column):Column={val s=value.cast("string");val p=utf16Prefix(s,3);when(s.isNull || utf16Length(s)<=3,0).when(p>="aaa"&&p<"ezz",1).when(p>="ezz"&&p<"izz",2).when(p>="izz"&&p<"mzz",3).when(p>="mzz"&&p<"qzz",4).when(p>="qzz"&&p<"uzz",5).when(p>="uzz",6).otherwise(7)}
  def truncate(value:Column,places:Int,dataType:String):Column={val scale=math.pow(10,places);when(value.isNull,value).otherwise((floor(value.cast("double")*scale)/scale).cast(dataType))}
  def trimDigits(value:Column,digits:Int,dataType:String):Column={
    val scale=BigDecimal(10).pow(digits)
    if(dataType=="int"||dataType=="long") {
      val q=value.cast("decimal(38,0)")/lit(scale)
      when(value.isNull,value).otherwise(when(q>=0,floor(q)).otherwise(ceil(q)).cast(dataType))
    } else when(value.isNull,value).otherwise(floor(value.cast("double")/lit(scale.toDouble)).cast(dataType))
  }
  def range(value:Column,lo:Double,hi:Double,outType:String):Column=when(value.isNull,lit(0).cast(outType)).when(value.cast("double")>=lo&&value.cast("double")<hi,lit(1).cast(outType)).otherwise(lit(0).cast(outType))
  def javaRound(value:Column):Column={
    val x=value.cast("double")
    when(x.isNull,lit(null).cast("long")).when(isnan(x),lit(0L))
      .when(x >= lit(Long.MaxValue.toDouble),lit(Long.MaxValue))
      .when(x <= lit(Long.MinValue.toDouble),lit(Long.MinValue))
      .otherwise(floor(x+0.5).cast("long"))
  }
  def stopWords(value:Column,pattern:String):Column=when(value.isNull || lit(pattern).isNull,lit(null).cast("string")).otherwise(regexp_replace(value.cast("string"),pattern,""))
  // Spark's public VectorUDT is represented as a struct. Read index 2 with
  // public Column operations, handling both dense and sparse encodings; this
  // avoids the optional ML helper class and unavailable Serverless SQL routine.
  def vectorValue(value:Column):Column={
    val vectorType=value.getField("type")
    val size=value.getField("size")
    val indices=value.getField("indices")
    val values=value.getField("values")
    // Spark 4 Serverless evaluates both branches under ANSI array bounds.
    // Sparse vectors commonly store fewer than three values, so direct
    // element_at(values, 3) can fail even when the sparse branch is selected.
    // Safe access preserves null/missing entries without changing the vector
    // contract or introducing a UDF.
    val dense=try_element_at(values,lit(3))
    val sparse=aggregate(sequence(lit(1),size),lit(0.0),(acc,pos)=>
      when(try_element_at(indices,pos.cast("int"))===lit(2),try_element_at(values,pos.cast("int"))).otherwise(acc))
    when(vectorType===lit(0),sparse).otherwise(dense)
  }
}

/** Registry of public-expression rewrite rules. */
object PublicRewriteRules {
  import NativeExpressions._
  private def binary(name:String,id:String)(f:(Column,Column)=>Column)=new FunctionalRewriteRule(id,NativeOperation.resolve(s"similarity.$name"),(l,r,c)=>f(l,r.getOrElse(throw new IllegalArgumentException(s"$name requires right operand"))))
  private def unary(op:String,id:String)(f:(Column,RewriteContext)=>Column)=new FunctionalRewriteRule(id,NativeOperation.resolve(op),(l,r,c)=>f(l,c))

  private val similarities:Seq[RewriteRule]=Seq(
    binary("SimilarityFunctionExact","rewrite.similarity.exact")(exact),
    binary("StringSimilarityFunction","rewrite.similarity.string_base")((l,r)=>stringBase(l,r){lit(0.0)}),
    binary("CheckNullFunction","rewrite.similarity.check_null")(checkNull),
    binary("CheckBlankOrNullFunction","rewrite.similarity.check_blank_or_null")(checkBlankOrNull),
    binary("IntegerSimilarityFunction","rewrite.similarity.integer")(integerSimilarity),
    binary("LongSimilarityFunction","rewrite.similarity.long")(longSimilarity),
    binary("DoubleSimilarityFunction","rewrite.similarity.double")((l,r)=>floatingSimilarity(l,r,"double")),
    binary("FloatSimilarityFunction","rewrite.similarity.float")((l,r)=>floatingSimilarity(l,r,"float")),
    binary("DateSimilarityFunction","rewrite.similarity.date")(dateSimilarity),
    binary("ArrayDoubleSimilarityFunction","rewrite.similarity.array_double")(arrayDoubleSimilarity),
    binary("JaccSimFunction","rewrite.similarity.jaccard")(jaccard),
    binary("NumbersJaccardFunction","rewrite.similarity.numbers_jaccard")(numbersJaccard),
    binary("ProductCodeFunction","rewrite.similarity.product_code")(productCode),
    binary("JaroWinklerFunction","rewrite.similarity.jaro")(jaro),
    binary("AJaroWinklerFunction","rewrite.similarity.ajaro")(jaro),
    binary("AffineGapSimilarityFunction","rewrite.similarity.affine_gap")(affineGap),
    binary("EmailMatchTypeFunction","rewrite.similarity.email")(email),
    binary("PinCodeMatchTypeFunction","rewrite.similarity.pin")(pin),
    binary("OnlyAlphabetsExactSimilarity","rewrite.similarity.only_alpha_exact")(onlyAlphaExact),
    binary("OnlyAlphabetsAffineGapSimilarity","rewrite.similarity.only_alpha_affine")(onlyAlphaAffine),
    binary("SameFirstWordFunction","rewrite.similarity.same_first_word")(sameFirstWord)
  )

  private val hashes:Seq[RewriteRule]=Seq(
    (1 to 4).map(n=>unary(s"blocking.first${n}Chars",s"rewrite.blocking.first${n}Chars")((c,_)=>firstChars(c,n))),
    (1 to 3).map(n=>unary(s"blocking.last${n}Chars",s"rewrite.blocking.last${n}Chars")((c,_)=>lastChars(c,n))),
    Seq(unary("blocking.lastWord","rewrite.blocking.lastWord")((c,_)=>javaLastWord(c)),
      unary("blocking.isNullOrEmpty","rewrite.blocking.isNullOrEmpty")((c,_)=>c.isNull||length(c.cast("string"))===0),
      unary("blocking.identityString","rewrite.blocking.identityString")((c,_)=>c), unary("blocking.identityInteger","rewrite.blocking.identityInteger")((c,_)=>c),
      unary("blocking.identityLong","rewrite.blocking.identityLong")((c,_)=>c), unary("blocking.identityBoolean","rewrite.blocking.identityBoolean")((c,_)=>c),
      unary("blocking.first2CharsBox","rewrite.blocking.first2CharsBox")((c,_)=>first2Box(c)), unary("blocking.first3CharsBox","rewrite.blocking.first3CharsBox")((c,_)=>first3Box(c))),
    Seq("Dbl"->"double","Float"->"float","Int"->"int","Long"->"long").map{case(s,t)=>unary(s"blocking.lessThanZero$s",s"rewrite.blocking.lessThanZero$s")((c,_)=>when(c.isNull,lit(false)).otherwise(c.cast(t)<0))},
    Seq("Double"->"double","Float"->"float").flatMap{case(s,t)=>(1 to 3).map(n=>unary(s"blocking.truncate${s}To${n}Places",s"rewrite.blocking.truncate${s}To${n}Places")((c,_)=>truncate(c,n,t)))},
    Seq("Dbl"->"double","Float"->"float","Int"->"int","Long"->"long").flatMap{case(s,t)=>(1 to 3).map(n=>unary(s"blocking.trimLast${n}Digits$s",s"rewrite.blocking.trimLast${n}Digits$s")((c,_)=>trimDigits(c,n,t)))},
    Seq("Dbl"->"int","Float"->"int","Int"->"int","Long"->"long").flatMap{case(s,t)=>Seq((0,10),(10,100),(100,1000),(1000,10000)).map{case(lo,hi)=>unary(s"blocking.rangeBetween${lo}And${hi}$s",s"rewrite.blocking.rangeBetween${lo}And${hi}$s")((c,_)=>range(c,lo,hi,t))}},
    Seq(unary("blocking.round","rewrite.blocking.round")((c,_)=>javaRound(c)))
  ).flatten

  private val misc:Seq[RewriteRule]=Seq(
    unary("preprocess.trim","rewrite.preprocess.trim")((c,_)=>trim(c)),
    unary("preprocess.caseNormalize","rewrite.preprocess.case_normalize")((c,_)=>lower(c.cast("string"))),
    unary("preprocess.stopWords","rewrite.preprocess.stop_words")((c,ctx)=>stopWords(c,ctx.parameters.getOrElse("pattern",throw new IllegalArgumentException("stopWords rewrite requires pattern")))),
    unary("model.vectorValue","rewrite.model.vector_value")((c,_)=>vectorValue(c))
  )

  val all:Seq[RewriteRule]=similarities++hashes++misc
}

object NativeRewriteRegistry { val default:RewriteRegistry=RewriteRegistry(PublicRewriteRules.all) }

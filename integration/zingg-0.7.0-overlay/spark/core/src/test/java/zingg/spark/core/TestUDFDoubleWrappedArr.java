package zingg.spark.core;

import org.apache.spark.sql.api.java.UDF2;

import scala.collection.Seq;
import zingg.common.core.similarity.function.ArrayDoubleSimilarityFunction;

/** Scala 2.13-compatible form of the pinned upstream WrappedArray test helper. */
public class TestUDFDoubleWrappedArr implements UDF2<Seq<Double>, Seq<Double>, Double> {

    private static final long serialVersionUID = 1L;

    @Override
    public Double call(Seq<Double> t1, Seq<Double> t2) throws Exception {
        Double[] t1Arr = new Double[t1 == null ? 0 : t1.size()];
        Double[] t2Arr = new Double[t2 == null ? 0 : t2.size()];
        for (int i = 0; i < t1Arr.length; i++) t1Arr[i] = t1.apply(i);
        for (int i = 0; i < t2Arr.length; i++) t2Arr[i] = t2.apply(i);
        return ArrayDoubleSimilarityFunction.cosineSimilarity(t1Arr, t2Arr);
    }
}

package org.openu.fimcmp.algbase;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.openu.fimcmp.SparkContextFactory;
import org.openu.fimcmp.apriori.AprioriAlg;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

/**
 * Useful base class for all other main algorithms.
 */
public abstract class AlgBase<P extends AlgBaseProperties> implements Serializable {
    protected final P props;

    public AlgBase(P props) {
        this.props = props;
    }

    public static JavaSparkContext createSparkContext(boolean useKryo, String sparkMasterUrl, StopWatch sw) {
        pp(sw, "Starting the Spark context");
        JavaSparkContext sc = SparkContextFactory.createSparkContext(useKryo, sparkMasterUrl);
        pp(sw, "Completed starting the Spark context");
        return sc;
    }

    public JavaRDD<String[]> readInput(JavaSparkContext sc, String inputFile, StopWatch sw) {
        pp(sw, "Start reading " + inputFile);
        JavaRDD<String[]> res = BasicOps.readLinesAsSortedItemsArr(inputFile, props.inputNumParts, sc);
        if (props.isPersistInput) {
            res = res.persist(StorageLevel.MEMORY_ONLY_SER());
        }
        pp(sw, "Done reading " + inputFile);
        return res;
    }

    protected F1Context computeF1Context(JavaRDD<String[]> trs, StopWatch sw) {
        TrsCount cnts = computeCounts(trs, sw);
        AprioriAlg<String> apr = new AprioriAlg<>(cnts.minSuppCnt);

        List<Tuple2<String, Integer>> sortedF1 = apr.computeF1WithSupport(trs);
        pp(sw, "F1 size = " + sortedF1.size());
        pp(sw, sortedF1);

        return new F1Context(apr, sortedF1, cnts, sw);
    }

    private TrsCount computeCounts(JavaRDD<String[]> trs, StopWatch sw) {
        final long totalTrs = trs.count();
        final long minSuppCount = BasicOps.minSuppCount(totalTrs, props.minSupp);

        pp(sw, "Total records: " + totalTrs);
        pp(sw, "Min support: " + minSuppCount);

        return new TrsCount(totalTrs, minSuppCount);
    }



    public static void pp(StopWatch sw, Object msg) {
        print(String.format("%-15s %s", tt(sw), msg));
    }

    private static void print(String msg) {
        System.out.println(msg);
    }

    private static String tt(StopWatch sw) {
        return "[" + sw.toString() + "] ";
    }
}

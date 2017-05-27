package org.openu.fimcmp.algs.algbase;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.openu.fimcmp.SparkContextFactory;
import org.openu.fimcmp.algs.apriori.AprioriAlg;
import org.openu.fimcmp.itemset.FreqItemset;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Useful base class for all other main algorithms.
 */
public abstract class AlgBase<P extends CommonAlgProperties, R> implements Serializable {
    protected final P props;
    private final String inputFile;

    public static Class[] getClassesToRegister() {
        return new Class[]{
                AlgBase.class,
                BasicOps.class,
                CommonAlgProperties.class,
                F1Context.class,
                TrsCount.class
        };
    }

    public AlgBase(P props, String inputFile) {
        this.props = props;
        this.inputFile = inputFile;
    }

    public abstract R run(JavaSparkContext sc, StopWatch sw) throws Exception;

    public static void pp(StopWatch sw, Object msg) {
        print(String.format("%-15s %s", tt(sw), msg));
    }

    public static void print(Object msg) {
        System.out.println(msg);
    }

    public static JavaSparkContext createSparkContext(boolean useKryo, String sparkMasterUrl, StopWatch sw) {
        pp(sw, "Starting the Spark context");
        JavaSparkContext sc = SparkContextFactory.createSparkContext(useKryo, sparkMasterUrl);
        pp(sw, "Completed starting the Spark context");
        return sc;
    }

    protected JavaRDD<String[]> readInput(JavaSparkContext sc, StopWatch sw) {
        pp(sw, "Start reading " + inputFile);
        JavaRDD<String[]> res = BasicOps.readLinesAsSortedItemsArr(inputFile, props.inputNumParts, sc);
        if (props.isPersistInput) {
            res = res.persist(StorageLevel.MEMORY_ONLY_SER());
        }
        pp(sw, "Done reading " + inputFile);
        return res;
    }

    protected JavaRDD<ArrayList<String>> readInputAsListRdd(JavaSparkContext sc, StopWatch sw) {
        pp(sw, "Start reading " + inputFile);
        JavaRDD<ArrayList<String>> res = BasicOps.readLinesAsSortedItemsList(inputFile, props.inputNumParts, sc);
        if (props.isPersistInput) {
            pp(sw, "Persisting the original input");
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
        if (props.isPrintIntermediateRes) {
            pp(sw, sortedF1);
        }

        return new F1Context(apr, sortedF1, cnts, sw);
    }

    public static List<FreqItemset> printAllItemsets(List<FreqItemset> allFrequentItemsets) {
        allFrequentItemsets = allFrequentItemsets.stream().
                sorted(FreqItemset::compareForNiceOutput2).collect(Collectors.toList());
        allFrequentItemsets.forEach(AlgBase::print);
        return allFrequentItemsets;
    }

    private TrsCount computeCounts(JavaRDD<String[]> trs, StopWatch sw) {
        final long totalTrs = trs.count();
        final long minSuppCount = BasicOps.minSuppCount(totalTrs, props.minSupp);

        pp(sw, "Total records: " + totalTrs);
        pp(sw, "Min support: " + minSuppCount);

        return new TrsCount(totalTrs, minSuppCount);
    }

    private static String tt(StopWatch sw) {
        return "[" + sw.toString() + "] ";
    }
}

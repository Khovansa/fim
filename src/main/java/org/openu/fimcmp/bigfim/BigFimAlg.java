package org.openu.fimcmp.bigfim;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.openu.fimcmp.BasicOps;
import org.openu.fimcmp.apriori.*;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

/**
 * The main class that implements the Big FIM algorithm.
 */
public class BigFimAlg implements Serializable {
    private final BigFimProperties props;

    public BigFimAlg(BigFimProperties props) {
        this.props = props;
    }

    public JavaRDD<String[]> readInput(JavaSparkContext sc, String inputFile) {
        JavaRDD<String[]> res = BasicOps.readLinesAsSortedItemsArr(inputFile, props.inputNumParts, sc);
        if (props.isPersistInput) {
            res = res.persist(StorageLevel.MEMORY_ONLY_SER());
        }
        return res;
    }

    public BigFimResult computeFis(JavaRDD<String[]> trs, StopWatch sw) {
        BigFimStepExecutor helper = new BigFimStepExecutor(props, computeAprioriContext(trs, sw));

        JavaRDD<int[]> ranks1Rdd = helper.computeRddRanks1(trs);
        AprioriStepRes currStep = helper.computeF2(ranks1Rdd);

        JavaRDD<Tuple2<int[], long[]>> ranks1AndK = null;
        while (helper.isContinueWithApriori()) {
            ranks1AndK = helper.computeCurrSizeRdd(currStep, ranks1AndK, ranks1Rdd, false);
            currStep = helper.computeFk(ranks1AndK, currStep);
        }

        JavaRDD<List<long[]>> optionalEclatFis = null;
        if (ranks1AndK != null && helper.canContinue()) {
            ranks1AndK = helper.computeCurrSizeRdd(currStep, ranks1AndK, ranks1Rdd, true);
            optionalEclatFis = helper.computeWithEclat(currStep, ranks1AndK);
        }

        return helper.createResult(optionalEclatFis);
    }

    private AprContext computeAprioriContext(JavaRDD<String[]> trs, StopWatch sw) {
        TrsCount cnts = computeCounts(trs, sw);
        AprioriAlg<String> apr = new AprioriAlg<>(cnts.minSuppCnt);

        List<Tuple2<String, Integer>> sortedF1 = apr.computeF1WithSupport(trs);
        pp(sw, "F1 size = " + sortedF1.size());
        pp(sw, sortedF1);

        return new AprContext(apr, sortedF1, cnts, sw);
    }

    private TrsCount computeCounts(JavaRDD<String[]> trs, StopWatch sw) {
        final long totalTrs = trs.count();
        final long minSuppCount = BasicOps.minSuppCount(totalTrs, props.minSupp);

        pp(sw, "Total records: " + totalTrs);
        pp(sw, "Min support: " + minSuppCount);

        return new TrsCount(totalTrs, minSuppCount);
    }

    static void pp(StopWatch sw, Object msg) {
        print(String.format("%-15s %s", tt(sw), msg));
    }

    private static void print(String msg) {
        System.out.println(msg);
    }

    private static String tt(StopWatch sw) {
        return "[" + sw.toString() + "] ";
    }
}

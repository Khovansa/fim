package org.openu.fimcmp.bigfim;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.openu.fimcmp.BasicOps;
import org.openu.fimcmp.FreqItemset;
import org.openu.fimcmp.apriori.AprioriAlg;
import org.openu.fimcmp.apriori.CurrSizeFiRanks;
import org.openu.fimcmp.apriori.FiRanksToFromItems;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
        //TODO
        AprContext aprContext = computeAprioriContext(trs, sw);
        JavaRDD<int[]> ranks1Rdd = computeRddRanks1(trs, aprContext);
        AprioriStepRes resPref2 = computeF2(ranks1Rdd, aprContext);
        return null;
    }

    private AprContext computeAprioriContext(JavaRDD<String[]> trs, StopWatch sw) {
        TrsCount cnts = computeCounts(trs, sw);
        AprioriAlg<String> apr = new AprioriAlg<>(cnts.minSuppCnt);

        List<String> sortedF1 = apr.computeF1(trs);
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

    private JavaRDD<int[]> computeRddRanks1(JavaRDD<String[]> trs, AprContext cxt) {
        JavaRDD<int[]> res = trs.map(t -> BasicOps.getMappedFilteredAndSortedTrs(t, cxt.itemToRank));
        res = res.persist(StorageLevel.MEMORY_ONLY_SER());

        pp(cxt.sw, "Filtered and saved ranks 1");

        return res;
    }

    private AprioriStepRes computeF2(JavaRDD<int[]> ranks1Rdd, AprContext cxt) {
        AprioriStepRes res = new AprioriStepRes(2);
        res.fkAsArrays = cxt.apr.computeF2_Part(ranks1Rdd, cxt.totalFreqItems);
        res.fk = cxt.apr.fkAsArraysToRankPairs(res.fkAsArrays);
        res.prevSizeAllRanks = new FiRanksToFromItems();
        res.currSizeRanks = CurrSizeFiRanks.construct(res.fk, cxt.totalFreqItems, cxt.totalFreqItems);
        res.currSizeAllRanks = res.prevSizeAllRanks.toNextSize(res.currSizeRanks);
        res.print(cxt, props.isPrintFks);

        res.ranks1AndK = cxt.apr.toRddOfRanks1And2(ranks1Rdd, res.currSizeRanks);
        return res;
    }

    private static void pp(StopWatch sw, Object msg) {
        print(String.format("%-15s %s", tt(sw), msg));
    }

    private static void print(String msg) {
        System.out.println(msg);
    }

    private static String tt(StopWatch sw) {
        return "[" + sw.toString() + "] ";
    }

    private static class AprContext {
        final AprioriAlg<String> apr;
        final List<String> sortedF1;
        final TrsCount cnts;
        final int totalFreqItems;
        final private Map<String, Integer> itemToRank;
        final private String[] rankToItem;
        final StopWatch sw;

        AprContext(AprioriAlg<String> apr, List<String> sortedF1, TrsCount cnts, StopWatch sw) {
            this.apr = apr;
            this.sortedF1 = sortedF1;
            this.totalFreqItems = sortedF1.size();
            this.itemToRank = BasicOps.itemToRank(sortedF1);
            this.cnts = cnts;
            this.rankToItem = BasicOps.getRankToItem(itemToRank);
            this.sw = sw;
        }
    }

    private static class AprioriStepRes {
        final int kk;
        FiRanksToFromItems prevSizeAllRanks;
        List<int[]> fkAsArrays;
        List<int[]> fk;
        CurrSizeFiRanks currSizeRanks;
        FiRanksToFromItems currSizeAllRanks;
        JavaRDD<Tuple2<int[], long[]>> ranks1AndK;

        AprioriStepRes(int kk) {
            this.kk = kk;
        }

        void print(AprContext cxt, boolean isPrintFks) {
            pp(cxt.sw, String.format("F%s size: %s", kk, fk.size()));

            if (isPrintFks) {
                List<FreqItemset> fkRes = cxt.apr.fkAsArraysToResItemsets(fkAsArrays, cxt.rankToItem, prevSizeAllRanks);
                fkRes = fkRes.stream()
                        .sorted((fi1, fi2) -> Integer.compare(fi2.freq, fi1.freq))
                        .collect(Collectors.toList());
                fkRes = fkRes.subList(0, Math.min(10, fkRes.size()));
                pp(cxt.sw, String.format("F%s\n: %s", kk, StringUtils.join(fkRes, "\n")));
            }
        }
    }
}

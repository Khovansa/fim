package org.openu.fimcmp.bigfim;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.openu.fimcmp.BasicOps;
import org.openu.fimcmp.FreqItemset;
import org.openu.fimcmp.FreqItemsetAsRanksBs;
import org.openu.fimcmp.ItemsetAndTidsCollection;
import org.openu.fimcmp.apriori.*;
import org.openu.fimcmp.eclat.EclatAlg;
import org.openu.fimcmp.eclat.EclatProperties;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
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
        Helper helper = new Helper(computeAprioriContext(trs, sw));

        JavaRDD<int[]> ranks1Rdd = helper.computeRddRanks1(trs);
        AprioriStepRes currStep = helper.computeF2(ranks1Rdd);

        JavaRDD<Tuple2<int[], long[]>> ranks1AndK = null;
        while (isContinueWithApriori(helper.aprioriFis)) {
            ranks1AndK = helper.computeCurrSizeRdd(currStep, ranks1AndK, ranks1Rdd, false);
            currStep = helper.computeFk(ranks1AndK, currStep);
        }

        JavaRDD<List<long[]>> optionalEclatFis = null;
        if (ranks1AndK != null && canContinue(helper.aprioriFis)) {
            ranks1AndK = helper.computeCurrSizeRdd(currStep, ranks1AndK, ranks1Rdd, true);
            optionalEclatFis = helper.computeWithEclat(currStep, ranks1AndK);
        }

        return helper.createResult(optionalEclatFis);
    }

    private JavaRDD<List<long[]>> computeWithSequentialEclat(
            JavaPairRDD<Integer, ItemsetAndTidsCollection> rKm1ToEclatInput, AprContext cxt) {
        pp(cxt.sw, "Starting Eclat computations");
        EclatProperties eclatProps = new EclatProperties(cxt.cnts.minSuppCnt, cxt.totalFreqItems);
        eclatProps.setUseDiffSets(props.isUseDiffSets);
        eclatProps.setSqueezingEnabled(props.isSqueezingEnabled);
        eclatProps.setCountingOnly(props.isCountingOnly);
        eclatProps.setRankToItem(cxt.rankToItem);

        EclatAlg eclat = new EclatAlg(eclatProps);
        JavaRDD<List<long[]>> resRdd = eclat.computeFreqItemsetsRdd(rKm1ToEclatInput);
        pp(cxt.sw, "Num parts for Eclat: " + resRdd.getNumPartitions());

        return resRdd;
    }

    private JavaPairRDD<Integer, ItemsetAndTidsCollection> computeEclatInput(
            AprioriStepRes currStep, AprContext cxt,
            JavaRDD<Tuple2<int[], long[]>> ranks1AndK, ArrayList<JavaRDD> allRanksRdds) {
        //prepare the input RDD:
        pp(cxt.sw, "Preparing to generate Eclat input");
        JavaRDD<long[]> kRanksBsRdd = ranks1AndK.map(r1AndK -> r1AndK._2);
        unpersistPrevIfNeeded(allRanksRdds);
        kRanksBsRdd = kRanksBsRdd.persist(StorageLevel.MEMORY_AND_DISK_SER());

        //compute TIDs
        pp(cxt.sw, "Computing TIDs");
        TidsGenHelper tidsGenHelper = currStep.constructTidGenHelper(cxt.cnts.totalTrs);
        PairRanks rkToRkm1AndR1 = currStep.currSizeAllRanks.constructRkToRkm1AndR1ForMaxK();
        JavaRDD<long[][]> rankToTidBsRdd = cxt.apr.computeCurrRankToTidBitSet_Part(kRanksBsRdd, tidsGenHelper);
        rankToTidBsRdd = rankToTidBsRdd.persist(StorageLevel.MEMORY_AND_DISK_SER());
        allRanksRdds.get(allRanksRdds.size() - 2).unpersist(); //the last one computed for Eclat should not be persisted
        kRanksBsRdd.unpersist();

        //preparing the input
        pp(cxt.sw, "Preparing Eclat input");
        JavaPairRDD<Integer, List<long[]>> rkm1ToTidSets =
                cxt.apr.groupTidSetsByRankKm1(rankToTidBsRdd, rkToRkm1AndR1, props.maxEclatNumParts);
        return rkm1ToTidSets.mapValues(tidSets ->
                TidMergeSet.mergeTidSetsWithSameRankDropMetadata(tidSets, tidsGenHelper, currStep.currSizeAllRanks));
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

    private boolean isContinueWithApriori(ArrayList<List<long[]>> aprioriFis) {
        if (!canContinue(aprioriFis)) {
            return false;
        }

        final int currPrefLen = aprioriFis.size() - 1;
        if (currPrefLen < props.prefixLenToStartEclat) {
            //if we have not reached the possible 'Eclat switching point', just continue
            return true;
        }

        //We have reached the possible 'Eclat switching point', now need to decide whether indeed to switch to Eclat:
        if (props.prefixLenToStartEclat <= 1) {
            //prefix length = 1 is a special case: if chosen, never go beyond it
            return false;
        }

        //the general case: check whether Apriori could produce the results fast - this happens in sparse datasets:
        assert aprioriFis.size() >= 2 : "size > currPrefLen >= prefixLenToStartEclat >= 2";
        List<long[]> lastRes = aprioriFis.get(aprioriFis.size() - 1);
        List<long[]> prevRes = aprioriFis.get(aprioriFis.size() - 2);
        double resIncreaseRatio = (1.0 * lastRes.size()) / prevRes.size();
        boolean isSignificantlyIncreased = (resIncreaseRatio < props.currToPrevResSignificantIncreaseRatio);
        //if the increase is not significant, it is a sparse dataset and we should continue with Apriori:
        return !isSignificantlyIncreased;
    }

    private boolean canContinue(ArrayList<List<long[]>> aprioriFis) {
        List<long[]> lastRes = aprioriFis.get(aprioriFis.size() - 1);
        return lastRes.size() > 1;
    }

    private JavaRDD<int[]> computeRddRanks1(JavaRDD<String[]> trs, AprContext cxt) {
        JavaRDD<int[]> res = trs.map(t -> BasicOps.getMappedFilteredAndSortedTrs(t, cxt.itemToRank));
        res = res.persist(StorageLevel.MEMORY_ONLY_SER());

        pp(cxt.sw, "Filtered and saved RDD ranks 1");

        return res;
    }

    private JavaRDD<Tuple2<int[], long[]>> computeCurrSizeRdd(
            AprioriStepRes currStep, AprContext cxt, JavaRDD<Tuple2<int[], long[]>> ranks1AndK, JavaRDD<int[]> ranks1Rdd,
            ArrayList<JavaRDD> ranksRdds, boolean isForEclat) {

        JavaRDD<Tuple2<int[], long[]>> res;
        StorageLevel storageLevel;
        if (ranks1AndK == null) {
            res = cxt.apr.toRddOfRanks1And2(ranks1Rdd, currStep.currSizeRanks);
            storageLevel = StorageLevel.MEMORY_ONLY_SER();
        } else {
            res = cxt.apr.toRddOfRanks1AndK(ranks1AndK, currStep.currSizeRanks);
            storageLevel = StorageLevel.MEMORY_AND_DISK_SER();
        }

        if (!isForEclat) {
            res = res.persist(storageLevel);
            unpersistPrevIfNeeded(ranksRdds);
            pp(cxt.sw, "Computed and saved RDD ranks " + currStep.kk);
        }

        return res;
    }

    private void unpersistPrevIfNeeded(ArrayList<JavaRDD> ranksRdds) {
        if (ranksRdds.size() >= 2) {
            ranksRdds.get(ranksRdds.size() - 2).unpersist();
        }
    }

    private AprioriStepRes computeF2(JavaRDD<int[]> ranks1Rdd, AprContext cxt) {
        AprioriStepRes res = new AprioriStepRes(2);
        res.fkAsArrays = cxt.apr.computeF2_Part(ranks1Rdd, cxt.totalFreqItems);
        res.fk = cxt.apr.fkAsArraysToRankPairs(res.fkAsArrays);
        res.prevSizeAllRanks = new FiRanksToFromItems();
        res.currSizeRanks = CurrSizeFiRanks.construct(res.fk, cxt.totalFreqItems, cxt.totalFreqItems);
        res.currSizeAllRanks = res.prevSizeAllRanks.toNextSize(res.currSizeRanks);
        res.print(cxt, props.isPrintFks);

        return res;
    }

    private AprioriStepRes computeFk(
            JavaRDD<Tuple2<int[], long[]>> ranks1AndK,
            NextSizeItemsetGenHelper nextSizeGenHelper,
            AprioriStepRes currStep,
            AprContext cxt) {
        AprioriStepRes res = new AprioriStepRes(currStep.kk + 1);
        res.fkAsArrays = cxt.apr.computeFk_Part(res.kk, ranks1AndK, nextSizeGenHelper);
        res.fk = cxt.apr.fkAsArraysToRankPairs(res.fkAsArrays);
        res.prevSizeAllRanks = currStep.currSizeAllRanks;
        res.currSizeRanks = CurrSizeFiRanks.construct(res.fk, cxt.totalFreqItems, currStep.fk.size());
        res.currSizeAllRanks = res.prevSizeAllRanks.toNextSize(res.currSizeRanks);
        res.print(cxt, props.isPrintFks);

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

    private class Helper {
        final AprContext cxt;
        final ArrayList<List<long[]>> aprioriFis;
        final ArrayList<JavaRDD> allRanksRdds;

        Helper(AprContext cxt) {
            this.cxt = cxt;
            aprioriFis = new ArrayList<>();
            allRanksRdds = new ArrayList<>();

            aprioriFis.add(cxt.freqItemRanksAsItemsetBs());
        }

        JavaRDD<int[]> computeRddRanks1(JavaRDD<String[]> trs) {
            JavaRDD<int[]> res = BigFimAlg.this.computeRddRanks1(trs, cxt);
            allRanksRdds.add(res);
            return res;
        }

        AprioriStepRes computeF2(JavaRDD<int[]> ranks1Rdd) {
            AprioriStepRes currStep = BigFimAlg.this.computeF2(ranks1Rdd, cxt);
            aprioriFis.add(currStep.getItemsetBitsets(cxt));
            return currStep;
        }

        AprioriStepRes computeFk(JavaRDD<Tuple2<int[], long[]>> ranks1AndK, AprioriStepRes currStep) {
            NextSizeItemsetGenHelper nextSizeGenHelper = currStep.computeNextSizeGenHelper(cxt.totalFreqItems);
            AprioriStepRes nextStep = BigFimAlg.this.computeFk(ranks1AndK, nextSizeGenHelper, currStep, cxt);
            aprioriFis.add(currStep.getItemsetBitsets(cxt));
            return nextStep;
        }

        JavaRDD<Tuple2<int[], long[]>> computeCurrSizeRdd(
                AprioriStepRes currStep, JavaRDD<Tuple2<int[], long[]>> ranks1AndKm1, JavaRDD<int[]> ranks1Rdd,
                boolean isForEclat) {
            JavaRDD<Tuple2<int[], long[]>> ranks1AndK =
                    BigFimAlg.this.computeCurrSizeRdd(currStep, cxt, ranks1AndKm1, ranks1Rdd, allRanksRdds, isForEclat);
            allRanksRdds.add(ranks1AndK);
            return ranks1AndK;
        }

        JavaRDD<List<long[]>> computeWithEclat(AprioriStepRes currStep, JavaRDD<Tuple2<int[], long[]>> ranks1AndK) {
            JavaPairRDD<Integer, ItemsetAndTidsCollection> rKm1ToEclatInput =
                    BigFimAlg.this.computeEclatInput(currStep, cxt, ranks1AndK, allRanksRdds);
            return BigFimAlg.this.computeWithSequentialEclat(rKm1ToEclatInput, cxt);
        }

        BigFimResult createResult(JavaRDD<List<long[]>> optionalEclatFis) {
            return new BigFimResult(cxt.itemToRank, cxt.rankToItem, aprioriFis, optionalEclatFis);
        }
    }

    private static class AprContext {
        final AprioriAlg<String> apr;
        final List<Tuple2<String, Integer>> sortedF1;
        final TrsCount cnts;
        final int totalFreqItems;
        final private Map<String, Integer> itemToRank;
        final private String[] rankToItem;
        final StopWatch sw;

        AprContext(AprioriAlg<String> apr, List<Tuple2<String, Integer>> sortedF1, TrsCount cnts, StopWatch sw) {
            this.apr = apr;
            this.sortedF1 = sortedF1;
            this.totalFreqItems = sortedF1.size();
            this.itemToRank = BasicOps.itemToRank(BasicOps.toItems(sortedF1));
            this.cnts = cnts;
            this.rankToItem = BasicOps.getRankToItem(itemToRank);
            this.sw = sw;
        }

        List<long[]> freqItemRanksAsItemsetBs() {
            List<long[]> res = new ArrayList<>(totalFreqItems);
            for (Tuple2<String, Integer> itemWithSupp : sortedF1) {
                int rank = itemToRank.get(itemWithSupp._1);
                int[] itemset = new int[]{rank};
                res.add(FreqItemsetAsRanksBs.toBitSet(itemWithSupp._2, itemset, totalFreqItems));
            }
            return res;
        }
    }

    private static class AprioriStepRes {
        final int kk;
        FiRanksToFromItems prevSizeAllRanks;
        List<int[]> fkAsArrays;
        List<int[]> fk;
        CurrSizeFiRanks currSizeRanks;
        FiRanksToFromItems currSizeAllRanks;

        AprioriStepRes(int kk) {
            this.kk = kk;
        }

        List<long[]> getItemsetBitsets(AprContext cxt) {
            return cxt.apr.fkAsArraysToItemsetBitsets(fkAsArrays, prevSizeAllRanks, cxt.totalFreqItems);
        }

        NextSizeItemsetGenHelper computeNextSizeGenHelper(int totalFreqItems) {
            return NextSizeItemsetGenHelper.construct(currSizeAllRanks, totalFreqItems, fk.size());
        }

        TidsGenHelper constructTidGenHelper(long totalTrs) {
            return currSizeRanks.constructTidGenHelper(fk, (int)totalTrs);
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

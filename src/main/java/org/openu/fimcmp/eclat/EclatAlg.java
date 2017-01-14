package org.openu.fimcmp.eclat;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.openu.fimcmp.ItemsetAndTids;
import org.openu.fimcmp.ItemsetAndTidsCollection;
import org.openu.fimcmp.util.BitArrays;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The main class that implements the Eclat algorithm.
 */
public class EclatAlg implements Serializable {
    private static final int TIDS_START_IND = ItemsetAndTids.getTidsStartInd();
    private static final double WORTH_SQUEEZING_RATIO = 0.8;
    private final long minSuppCount;
    private final boolean isUseDiffSets;
    private final boolean isSqueezingEnabled;

    public EclatAlg(long minSuppCount, boolean isUseDiffSets, boolean isSqueezingEnabled) {
        this.minSuppCount = minSuppCount;
        this.isUseDiffSets = isUseDiffSets;
        this.isSqueezingEnabled = isSqueezingEnabled;
    }

    public JavaRDD<List<Tuple2<int[], Integer>>> computeFreqItemsetsRdd(
            JavaPairRDD<Integer, ItemsetAndTidsCollection> prefRankAndIsAndTidSetRdd) {
        return
                prefRankAndIsAndTidSetRdd.mapValues(this::computeFreqItemsetsSingle)
                .sortByKey()
                .values();
    }

    /**
     * Returns a list of {FI as an array of r1s, its frequency}
     */
    public List<Tuple2<int[], Integer>> computeFreqItemsetsSingle(ItemsetAndTidsCollection initFis) {
        ArrayList<ItemsetAndTids> itemsetAndTidsList = initFis.getItemsetAndTidsList();
        if (initFis.isEmpty()) {
            return Collections.emptyList();
        }

        List<Tuple2<int[], Integer>> res = new ArrayList<>(10_000);
        sortByKm1Item(itemsetAndTidsList, initFis.getItemsetSize());
        ItemsetAndTidsCollection nextGen = initFis;
        do {
            nextGen = produceNextGenFis(nextGen);
            res.addAll((nextGen != null) ? nextGen.toResult() : Collections.emptyList());
        } while (nextGen != null && nextGen.size() > 1);

        return res;
    }

    private void sortByKm1Item(ArrayList<ItemsetAndTids> fis, int itemsetSize) {
        final int sortItemInd = itemsetSize - 1;
        fis.sort((o1, o2) -> Integer.compare(o1.getItem(sortItemInd), o2.getItem(sortItemInd)));
    }

    private ItemsetAndTidsCollection produceNextGenFis(ItemsetAndTidsCollection currGen) {
        if (currGen.size() <= 1) {
            return null;
        }

        ArrayList<ItemsetAndTids> fis = currGen.getItemsetAndTidsList();
        final int kk = currGen.getItemsetSize();
        final int prefSize = kk - 1;

        final int totalTids = currGen.getTotalTids();
        ArrayList<ItemsetAndTids> resList = new ArrayList<>(10_000);
        for (int ii=0; ii < fis.size() - 1; ++ii) {
            ItemsetAndTids is1 = fis.get(ii);
            for (int jj = ii + 1; jj < fis.size(); ++jj) {
                ItemsetAndTids is2 = fis.get(jj);
                if (!is1.haveCommonPrefix(prefSize, is2)) {
                    break;
                }

                ItemsetAndTids newIs = computeGoodNextSizeItemsetOrNull(is1, is2, totalTids);
                if (newIs != null) {
                    resList.add(newIs);
                }
            }
        }

        return squeeze(resList, kk+1, totalTids);
    }

    private ItemsetAndTids computeGoodNextSizeItemsetOrNull(ItemsetAndTids is1, ItemsetAndTids is2, int totalTids) {
//        return is1.computeNewItemsetByAnd(is2, totalTids);
        return is1.computeNewFromNextDiffsetWithSamePrefixOrNull(is2, totalTids, minSuppCount, isUseDiffSets);
    }

    /**
     * Try to shorten to TIDs bit sets by removing 0's that are the same for all itemsets. <br/>
     */
    private ItemsetAndTidsCollection squeeze(
            ArrayList<ItemsetAndTids> inTidsList, int itemsetSize, int totalTids) {
        if (!isSqueezingEnabled || inTidsList.size() <= 1) {
            //no point in squeezing a single bit set since it will not go anywhere
            return new ItemsetAndTidsCollection(inTidsList, itemsetSize, totalTids);
        }

//        StopWatch sw = new StopWatch();
//        sw.start();
//        System.out.println(String.format("%-15s start squeeze check (%s elems): %s (%s)", tt(sw), inTidsList.size(), itemsetSize, totalTids));
        long[] tidsToKeepBitSet = computeTidsPresentInAtLeastOneFi(inTidsList, totalTids);
        final int resTotalTids = BitArrays.cardinality(tidsToKeepBitSet, TIDS_START_IND);
//        System.out.println(String.format("%-15s done squeeze check: %s (%s)", tt(sw), itemsetSize, totalTids));

        if (1.0 * resTotalTids / totalTids > WORTH_SQUEEZING_RATIO) {
            return new ItemsetAndTidsCollection(inTidsList, itemsetSize, totalTids);
        }

//        System.out.println(String.format("%-15s start squeezing: ratio=%s", tt(sw), 1.0 * resTotalTids / totalTids));
        int[] tidsToKeep = BitArrays.asNumbers(tidsToKeepBitSet, TIDS_START_IND);
        ArrayList<ItemsetAndTids> resTidsList = initNewItemsetAndTidList(inTidsList, resTotalTids);
        int resTid = 0;
        for (int origTid : tidsToKeep) {
            setTidToEachMatchingItemset(resTidsList, resTid, inTidsList, origTid);
            ++resTid;
        }

//        System.out.println(String.format("%-15s done squeezing", tt(sw)));
        return new ItemsetAndTidsCollection(resTidsList, itemsetSize, resTotalTids);
    }

    private static String tt(StopWatch sw) {
        return "[" + sw.toString() + "] ";
    }

    private long[] computeTidsPresentInAtLeastOneFi(ArrayList<ItemsetAndTids> iatList, int totalTids) {
        long[] resBitSet = new long[BitArrays.requiredSize(totalTids, TIDS_START_IND)]; //all 0's
        for (ItemsetAndTids iat : iatList) {
            BitArrays.or(resBitSet, iat.getTidBitSet(), TIDS_START_IND, totalTids);
        }
        return resBitSet;

    }

    private ArrayList<ItemsetAndTids> initNewItemsetAndTidList(ArrayList<ItemsetAndTids> inTidsList, int newTotalTids) {
        ArrayList<ItemsetAndTids> res = new ArrayList<>(inTidsList.size());
        for (ItemsetAndTids tids : inTidsList) {
            res.add(tids.withNewTids(newTotalTids));
        }
        return res;
    }

    private void setTidToEachMatchingItemset(
            ArrayList<ItemsetAndTids> resIatList, int resTid, ArrayList<ItemsetAndTids> origIatList, int origTid) {
        for (int ii=0; ii<origIatList.size(); ++ii) {
            ItemsetAndTids origSet = origIatList.get(ii);
            if (origSet.hasTid(origTid)) {
                ItemsetAndTids resSet = resIatList.get(ii);
                resSet.setTid(resTid);
            }
        }
    }
}

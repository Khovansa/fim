package org.openu.fimcmp.eclat;

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

    public EclatAlg(long minSuppCount) {
        this.minSuppCount = minSuppCount;
    }

    public JavaRDD<List<Tuple2<int[], Integer>>> computeFreqItemsetsRdd(
            JavaPairRDD<Integer, ItemsetAndTidsCollection> prefRankAndIsAndTidSetRdd) {
        return
//                prefRankAndIsAndTidSetRdd.mapValues(this::computeFisFromTidSets)
                prefRankAndIsAndTidSetRdd.mapValues(this::asIs)
                .sortByKey()
                .values();
    }

    //TODO - remove this method
    List<Tuple2<int[], Integer>> asIs(ItemsetAndTidsCollection itemsetAndTidsCollection) {
        ArrayList<ItemsetAndTids> itemsetAndTidsList = itemsetAndTidsCollection.getItemsetAndTidsList();
        List<Tuple2<int[], Integer>> res = new ArrayList<>(itemsetAndTidsList.size());
        for (ItemsetAndTids itemsetAndTids : itemsetAndTidsList) {
            res.add(new Tuple2<>(itemsetAndTids.getItemset(), itemsetAndTids.getSupportCntIfExists()));
        }

        return res;
    }

    /**
     * Returns a list of {FI as an array of r1s, its frequency}
     */
    List<Tuple2<int[], Integer>> computeFreqItemsetsSingle(ItemsetAndTidsCollection initFis) {
        //TODO
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
        } while (nextGen != null && nextGen.size() > 1 && nextGen.getTotalTids() > 0);

        return res;
    }

    private void sortByKm1Item(ArrayList<ItemsetAndTids> fis, int itemsetSize) {
        final int sortItemInd = itemsetSize - 1;
        fis.sort((o1, o2) -> Integer.compare(o1.getItem(sortItemInd), o2.getItem(sortItemInd)));
    }

    private ItemsetAndTidsCollection produceNextGenFis(ItemsetAndTidsCollection currGen) {
        ArrayList<ItemsetAndTids> fis = currGen.getItemsetAndTidsList();
        final int kk = currGen.getItemsetSize();
        final int supportCorrection = currGen.getSupportCorrection();

        ArrayList<ItemsetAndTids> resList = new ArrayList<>(10_000);
        for (int ii=0; ii < fis.size() - 1; ++ii) {
            for (int jj = ii + 1; jj < fis.size(); ++jj) {
                ItemsetAndTids is1 = fis.get(ii);
                ItemsetAndTids is2 = fis.get(jj);
                if (!haveCommonPrefix(kk-1, is1, is2)) {
                    break;
                }

                ItemsetAndTids newIs = computeNextSizeItemset(is1, is2);
                if (supportCorrection + newIs.getSupportCntIfExists() >= minSuppCount) {
                    resList.add(newIs);
                }
            }
        }

        return squeeze(resList, supportCorrection, kk+1, currGen.getTotalTids());
    }

    private boolean haveCommonPrefix(int prefSize, ItemsetAndTids is1, ItemsetAndTids is2) {
        //TODO
        return false;
    }

    private ItemsetAndTids computeNextSizeItemset(ItemsetAndTids is1, ItemsetAndTids is2) {
        //TODO
        return null;
    }

    /**
     * Try to shorten to TIDs bit sets by removing bits that are the same for all itemsets (all 0's or all 1's). <br/>
     * Removing a bit that is 1 for each itemset means we should correct the support count by one. <br/>
     * That's why we use 'supportCorrection'.
     */
    private ItemsetAndTidsCollection squeeze(
            ArrayList<ItemsetAndTids> inTidsList, int supportCorrection, int itemsetSize, int totalTids) {
        long[] tidsInAnyFiBitSet = computeTidsPresentInAnyFi(inTidsList, totalTids);
        long[] tidsInEveryFiBitSet = computeTidsPresentInEveryFi(inTidsList, totalTids);
        int[] tidsInAnyFi = BitArrays.asNumbers(tidsInAnyFiBitSet, TIDS_START_IND);

        Tuple2<Integer, Integer> resSupportCorrectionAndTotalTids =
                computeSupportCorrectionAndTotalTids(tidsInAnyFi, tidsInEveryFiBitSet);
        final int resSupportCorrection = supportCorrection + resSupportCorrectionAndTotalTids._1;
        final int resTotalTids = resSupportCorrectionAndTotalTids._2;
        if (1.0 * resTotalTids / totalTids > WORTH_SQUEEZING_RATIO) {
            return new ItemsetAndTidsCollection(inTidsList, supportCorrection, itemsetSize, totalTids);
        }

        ArrayList<ItemsetAndTids> resTidsList = initNewItemsetAndTidList(inTidsList, resTotalTids);
        int resTid = 0;
        for (int candTid : tidsInAnyFi) {
            if (!BitArrays.get(tidsInEveryFiBitSet, TIDS_START_IND, candTid)) {
                setTidToEachItemset(resTidsList, resTid);
                ++resTid;
            }
        }

        return new ItemsetAndTidsCollection(resTidsList, resSupportCorrection, itemsetSize, resTotalTids);
    }

    private long[] computeTidsPresentInAnyFi(ArrayList<ItemsetAndTids> iatList, int totalTids) {
        long[] resBitSet = new long[BitArrays.requiredSize(totalTids, TIDS_START_IND)]; //all 0's
        for (ItemsetAndTids iat : iatList) {
            BitArrays.or(resBitSet, iat.getTidBitSet(), TIDS_START_IND, totalTids);
        }
        return resBitSet;

    }

    private long[] computeTidsPresentInEveryFi(ArrayList<ItemsetAndTids> iatList, int totalTids) {
        final int maxBit = totalTids - 1;
        long[] resBitSet = new long[BitArrays.requiredSize(totalTids, TIDS_START_IND)];
        BitArrays.not(resBitSet, TIDS_START_IND, maxBit); //all 1's
        for (ItemsetAndTids iat : iatList) {
            BitArrays.and(resBitSet, iat.getTidBitSet(), TIDS_START_IND, totalTids);
        }
        return resBitSet;
    }

    private Tuple2<Integer, Integer> computeSupportCorrectionAndTotalTids(int[] tidsInAnyFi, long[] tidsInEveryFiBitSet) {
        int tidInEveryFiCount = 0;
        for (int tid : tidsInAnyFi) {
            if (BitArrays.get(tidsInEveryFiBitSet, TIDS_START_IND, tid)) {
                ++tidInEveryFiCount; //TID contains all itemsets => skip, but remember to correct to support count
            }
        }

        int resTotalTids = tidsInAnyFi.length - tidInEveryFiCount; //neither all 0's nor all 1's
        return new Tuple2<>(tidInEveryFiCount, resTotalTids);
    }

    private ArrayList<ItemsetAndTids> initNewItemsetAndTidList(ArrayList<ItemsetAndTids> inTidsList, int newTotalTids) {
        ArrayList<ItemsetAndTids> res = new ArrayList<>(inTidsList.size());
        for (ItemsetAndTids tids : inTidsList) {
            res.add(tids.withNewTids(newTotalTids));
        }
        return res;
    }

    private void setTidToEachItemset(ArrayList<ItemsetAndTids> resTidsList, int resTid) {
        for (ItemsetAndTids iat : resTidsList) {
            iat.setTid(resTid);
        }
    }
}

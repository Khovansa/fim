package org.openu.fimcmp.apriori;

import org.apache.commons.lang.ArrayUtils;
import org.openu.fimcmp.util.BoundedIntPairSet;

import java.io.Serializable;
import java.util.*;

/**
 * Auxiliary class to hold F2-related data in a way that allows fast processing.
 */
class PreprocessedF2 implements NextSizeItemsetGenHelper, Serializable {
    private final PairRanks pairRanks;
    private final BoundedIntPairSet cand3; //(item, pair) -> is possible

    static PreprocessedF2 construct(List<int[]> f2, int totalFreqItems) {
        List<int[]> sortedF2 = getSortedByDecreasingFreq(f2);
        PairRanks pairRanks = PairRanks.construct(sortedF2, totalFreqItems, totalFreqItems);
        int[][] firstElemToPairRanks = constructFirstElemToPairRanks(sortedF2, totalFreqItems, pairRanks.pairToRank);
        BoundedIntPairSet cand3 = constructCand3(totalFreqItems, sortedF2, pairRanks, firstElemToPairRanks);
        return new PreprocessedF2(pairRanks, cand3);
    }

    @Override
    public boolean isGoodNextSizeItemset(int item, int currItemsetRank) {
        //Ordering: item < current-itemset[0] < current-itemset[1] < ...:
        return item < pairRanks.rankToPair[currItemsetRank][0] && cand3.contains(item, currItemsetRank);
    }

    int[] getPairByRank(int rank) {
        return pairRanks.rankToPair[rank];
    }

    int getPairRank(int elem1, int elem2) {
        return pairRanks.pairToRank[elem1][elem2];
    }

    private static List<int[]> getSortedByDecreasingFreq(List<int[]> f2) {
        ArrayList<int[]> res = new ArrayList<>(f2);
        Collections.sort(res, (o1, o2) -> Integer.compare(o2[2], o1[2]));
        return res;
    }

    private static int[][] constructFirstElemToPairRanks(
            List<int[]> sortedF2, int totalFreqItems, int[][] pairToRank) {
        //compute it
        Map<Integer, List<Integer>> firstElemToPairRanks = new LinkedHashMap<>(totalFreqItems * 2);
        for (int[] pair : sortedF2) {
            int elem1 = pair[0];
            List<Integer> pairRanks = firstElemToPairRanks.get(elem1);
            if (pairRanks == null) {
                pairRanks = new ArrayList<>(totalFreqItems);
                firstElemToPairRanks.put(elem1, pairRanks);
            }
            int elem2 = pair[1];
            pairRanks.add(pairToRank[elem1][elem2]);
        }

        //produce array: first elem -> list of pair ranks
        int[][] res = new int[totalFreqItems][];
        for (Map.Entry<Integer, List<Integer>> entry : firstElemToPairRanks.entrySet()) {
            Integer firstElem = entry.getKey();
            List<Integer> pairRanks = entry.getValue();
            //convert list to array:
            int[] pairRanksArr = ArrayUtils.toPrimitive(pairRanks.toArray(new Integer[pairRanks.size()]));
            res[firstElem] = pairRanksArr;
        }
        return res;
    }

    /**
     * Assuming the item ranks are [0, totalFreqItems) with 0 as the most frequent item. <br/>
     */
    private static BoundedIntPairSet constructCand3(
            int totalFreqItems, List<int[]> f2, PairRanks pairRanks, int[][] firstElemToPairRanks) {
        BoundedIntPairSet res = new BoundedIntPairSet(totalFreqItems - 1, f2.size() - 1);
        for (int elem1 = 0; elem1 < totalFreqItems; ++elem1) {
            for (int elem2 = elem1 + 1; elem2 < firstElemToPairRanks.length; ++elem2) {
                int[] ranks = firstElemToPairRanks[elem2];
                if (ranks == null) {
                    continue;
                }
                for (int rank : ranks) {
                    //need to check whether the triplet (item, pair) have a chance to be frequent:
                    if (couldBeFrequent(elem1, rank, pairRanks.pairToRank, pairRanks.rankToPair)) {
                        res.add(elem1, rank);
                    }
                }
            }
        }

        return res;
    }

    private static boolean couldBeFrequent(int item1, int pairRank, int[][] pairToRank, int[][] rankToPair) {
        int[] pair = rankToPair[pairRank];
        int item2 = pair[0];
        int item3 = pair[1];
        //pairToRank[...] < 0 means this pair is not frequent:
        return pairToRank[item1][item2] >= 0 && pairToRank[item1][item3] >= 0;
    }

    private PreprocessedF2(PairRanks pairRanks, BoundedIntPairSet cand3) {
        this.pairRanks = pairRanks;
        this.cand3 = cand3;
    }
}
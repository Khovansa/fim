package org.openu.fimcmp.apriori;

import org.openu.fimcmp.util.BoundedIntPairSet;

import java.io.Serializable;
import java.util.*;

/**
 * Auxiliary class to hold F2-related data in a way that allows fast processing.
 */
class CurrSizeFiRanks implements NextSizeItemsetGenHelper, Serializable {
    private final PairRanks pairRanks;
    private final BoundedIntPairSet cand3; //(item, pair) -> is possible

    static CurrSizeFiRanks construct(List<int[]> f2, int totalFreqItems) {
        List<int[]> sortedF2 = getSortedByDecreasingFreq(f2);
        PairRanks pairRanks = PairRanks.construct(sortedF2, totalFreqItems, totalFreqItems);
        BoundedIntPairSet cand3 = constructCand3(totalFreqItems, sortedF2, pairRanks);
        return new CurrSizeFiRanks(pairRanks, cand3);
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

    /**
     * Assuming the item ranks are [0, totalFreqItems) with 0 as the most frequent item. <br/>
     */
    private static BoundedIntPairSet constructCand3(int totalItems, List<int[]> f2, PairRanks pairRanks) {
        BoundedIntPairSet res = new BoundedIntPairSet(totalItems, f2.size());

        Set<Integer> firstElems = new TreeSet<>();
        for (int[] pair : f2) {
            firstElems.add(pair[0]);
        }

        for (int elem1 : firstElems) {
            for (int pairRank = 0; pairRank < pairRanks.rankToPair.length; ++pairRank) {
                int[] pair = pairRanks.rankToPair[pairRank];
                if (elem1 < pair[0] && couldBeFrequent(elem1, pair[0], pair[1], pairRanks)) {
                    //only considering cases of elem1 < elem2 < elem3
                    res.add(elem1, pairRank);
                }
            }
        }
        return res;
    }

    private static boolean couldBeFrequent(int item1, int item2, int item3, PairRanks pairRanks) {
        return pairRanks.existsPair(item1, item2) && pairRanks.existsPair(item1, item3);
    }

    private CurrSizeFiRanks(PairRanks pairRanks, BoundedIntPairSet cand3) {
        this.pairRanks = pairRanks;
        this.cand3 = cand3;
    }
}
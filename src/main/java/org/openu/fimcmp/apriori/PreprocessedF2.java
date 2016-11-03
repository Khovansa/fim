package org.openu.fimcmp.apriori;

import org.apache.commons.lang.ArrayUtils;
import org.openu.fimcmp.util.Assert;
import org.openu.fimcmp.util.BoundedIntPairSet;

import java.io.Serializable;
import java.util.*;

/**
 * Auxiliary class to hold F2-related data in a way that allows fast processing.
 */
class PreprocessedF2 implements NextSizeItemsetGenHelper, Serializable {
    private final int[][] rankToPair;
    private final int[][] pairToRank;
    private final BoundedIntPairSet cand3; //(item, pair) -> is possible

    static PreprocessedF2 construct(List<Integer[]> f2, int totalFreqItems) {
        List<Integer[]> sortedF2 = getSortedByDecreasingFreq(f2);
        int[][] rankToPair = constructPairRankToPair(sortedF2);
        int[][] pairToRank = constructPairToRank(rankToPair, totalFreqItems);
        int[][] firstElemToPairRanks = constructFirstElemToPairRanks(sortedF2, totalFreqItems, pairToRank);
        BoundedIntPairSet cand3 = constructCand3(totalFreqItems, sortedF2, pairToRank, rankToPair, firstElemToPairRanks);
        return new PreprocessedF2(rankToPair, pairToRank, cand3);
    }

    public int size() {
        return rankToPair.length;
    }

    @Override
    public boolean isGoodNextSizeItemset(int item, int currItemsetRank) {
        //Ordering: item < current-itemset[0] < current-itemset[1] < ...:
        return item < rankToPair[currItemsetRank][0] && cand3.contains(item, currItemsetRank);
    }

    int[] getPairByRank(int rank) {
        return rankToPair[rank];
    }

    int getPairRank(int elem1, int elem2) {
        return pairToRank[elem1][elem2];
    }

    private PreprocessedF2(int[][] rankToPair, int[][] pairToRank, BoundedIntPairSet cand3) {
        this.rankToPair = rankToPair;
        this.pairToRank = pairToRank;
        this.cand3 = cand3;
    }

    private static List<Integer[]> getSortedByDecreasingFreq(List<Integer[]> f2) {
        ArrayList<Integer[]> res = new ArrayList<>(f2);
        Collections.sort(res, (o1, o2) -> Integer.compare(o2[2], o1[2]));
        return res;
    }

    private static int[][] constructPairRankToPair(List<Integer[]> sortedF2) {
        final int arrSize = sortedF2.size();
        int[][] res = new int[arrSize][];
        int rank = 0;
        for (Integer[] pair : sortedF2) {
            res[rank++] = ArrayUtils.toPrimitive(pair);
        }
        return res;
    }

    private static int[][] constructPairToRank(int[][] rankToPair, int totalFreqItems) {
        Assert.isTrue(totalFreqItems > 1);

        //initialize the result
        int[][] res = new int[totalFreqItems][totalFreqItems];
        for (int ii=0; ii<totalFreqItems; ++ii) {
            for (int jj=0; jj<totalFreqItems; ++jj) {
                res[ii][jj] = -1;
            }
        }

        //have a map: pair -> rank
        for (int rank = 0; rank < rankToPair.length; ++rank) {
            int[] pair = rankToPair[rank];
            res[pair[0]][pair[1]] = rank;
        }
        return res;
    }

    private static int[][] constructFirstElemToPairRanks(
            List<Integer[]> sortedF2, int totalFreqItems, int[][] pairToRank) {
        //compute it
        Map<Integer, List<Integer>> firstElemToPairRanks = new LinkedHashMap<>(totalFreqItems*2);
        for (Integer[] pair : sortedF2) {
            int elem1 = pair[0];
            List<Integer> pairRanks = firstElemToPairRanks.get(elem1);
            if (pairRanks == null) {
                pairRanks = new ArrayList<>(totalFreqItems);
                firstElemToPairRanks.put(elem1, pairRanks);
            }
            Integer elem2 = pair[1];
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
            int totalFreqItems, List<Integer[]> f2, int[][] pairToRank, int[][] rankToPair,
            int[][] firstElemToPairRanks) {
        BoundedIntPairSet res = new BoundedIntPairSet(totalFreqItems-1, f2.size()-1);
        for (int elem1=0; elem1<totalFreqItems; ++elem1) {
            for (int elem2=elem1+1; elem2<firstElemToPairRanks.length; ++elem2) {
                int[] pairRanks = firstElemToPairRanks[elem2];
                if (pairRanks == null) {
                    continue;
                }
                for (int pairRank : pairRanks) {
                    //need to check whether the triplet (item, pair) have a chance to be frequent:
                    if (couldBeFrequent(elem1, pairRank, pairToRank, rankToPair)) {
                        res.add(elem1, pairRank);
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
        return pairToRank[item1][item2]>=0 && pairToRank[item1][item3] >= 0;
    }
}
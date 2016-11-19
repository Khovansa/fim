package org.openu.fimcmp.apriori;

import org.openu.fimcmp.util.Assert;
import org.openu.fimcmp.util.BoundedIntPairSet;

import java.io.Serializable;
import java.util.*;

/**
 * Auxiliary class to hold the current-size FIs as integer ranks to allow fast next-size itemsets generation.
 */
class CurrSizeFiRanks implements NextSizeItemsetGenHelper, Serializable {
    private final PairRanks currSizeRanks;
    private final BoundedIntPairSet nextSizeCands; //(item, pair) -> is possible

    static PairRanks constructF2Ranks(List<int[]> f2, int totalFreqItems) {
        List<int[]> sortedF2 = getSortedByDecreasingFreq(f2);
        return PairRanks.construct(sortedF2, totalFreqItems, totalFreqItems);
    }

    /**
     * @param fkAsPairs      list of triplets (elem1, elem2, frequency)
     *                       elem1 = frequent item
     *                       elem2 = (k-1)-size itemset represented as rank, where k current itemset size
     * @param totalFreqItems total number of all possible elem1's, i.e. total number of frequent items
     * @param totalFkm1      total number of all possible elem2's, i.e. total number of (k-1)-size FIs
     * @param f2Ranks        ranks for F2
     *
     */
    static CurrSizeFiRanks construct(List<int[]> fkAsPairs, int totalFreqItems, int totalFkm1, PairRanks f2Ranks) {
        Assert.isTrue(!fkAsPairs.isEmpty());
        Assert.isTrue(totalFreqItems > 0);
        Assert.isTrue(totalFkm1 > 0);

        //sort the incoming pairs by descending frequency - it's handy:
        List<int[]> sortedFk = getSortedByDecreasingFreq(fkAsPairs);
        //enumerate the k-FIs, i.e. give them integer ranks:
        PairRanks fkRanks = PairRanks.construct(sortedFk, totalFreqItems, totalFkm1);
        //construct a bit set (item, k-FI as rank) -> whether has chance to be frequent:
        BoundedIntPairSet nextSizeCands = constructNextSizeCands(totalFreqItems, sortedFk, fkRanks, f2Ranks);

        return new CurrSizeFiRanks(fkRanks, nextSizeCands);
    }

    @Override
    public boolean isGoodNextSizeItemset(int item, int currItemsetRank) {
        //Ordering: item < current-itemset[0] < current-itemset[1] < ...:
        return item < currSizeRanks.rankToPair[currItemsetRank][0] && nextSizeCands.contains(item, currItemsetRank);
    }

    int[] getCurrSizeFiAsPairByRank(int rank) {
        return currSizeRanks.rankToPair[rank];
    }

    /**
     * See {@link #construct} for definition of rank1 and rankKm1 (elem1 and elem2)
     */
    int getCurrSizeFiRankByPair(int rank1, int rankKm1) {
        return currSizeRanks.pairToRank[rank1][rankKm1];
    }

    TidsGenHelper constructTidGenHelper(List<int[]> fk, int totalTids) {
        return TidsGenHelper.construct(fk, currSizeRanks, totalTids);
    }

    private static List<int[]> getSortedByDecreasingFreq(List<int[]> currSizeFisAsPairs) {
        ArrayList<int[]> res = new ArrayList<>(currSizeFisAsPairs);
        Collections.sort(res, (o1, o2) -> Integer.compare(o2[2], o1[2]));
        return res;
    }

    /**
     * Assuming the item ranks are [0, totalItems) <br/>
     */
    private static BoundedIntPairSet constructNextSizeCands(
            int totalItems, List<int[]> currSizeFisAsPairs, PairRanks fkRanks, PairRanks f2Ranks) {
        BoundedIntPairSet res = new BoundedIntPairSet(totalItems, currSizeFisAsPairs.size());

        Set<Integer> firstElems = new TreeSet<>();
        for (int[] pair : currSizeFisAsPairs) {
            firstElems.add(pair[0]);
        }

        for (int item1 : firstElems) {
            for (int kFiRank = 0; kFiRank < fkRanks.rankToPair.length; ++kFiRank) {
                int[] kFiAsPair = fkRanks.rankToPair[kFiRank];
                int item2 = kFiAsPair[0];
                int km1Rank = kFiAsPair[1];
                if (item1 < item2 && //only considering cases of item1 < item2
                        couldBeFrequent(item1, item2, km1Rank, fkRanks, f2Ranks)) {
                    res.add(item1, kFiRank);
                }
            }
        }
        return res;
    }

    private static boolean couldBeFrequent(
            int item1, int item2, int km1FiRank, PairRanks fkRanks, PairRanks f2Ranks) {
        return f2Ranks.existsPair(item1, item2) && fkRanks.existsPair(item1, km1FiRank);
    }

    private CurrSizeFiRanks(PairRanks currSizeRanks, BoundedIntPairSet nextSizeCands) {
        this.currSizeRanks = currSizeRanks;
        this.nextSizeCands = nextSizeCands;
    }
}
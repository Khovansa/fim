package org.openu.fimcmp.apriori;

import org.openu.fimcmp.util.Assert;

import java.io.Serializable;
import java.util.*;

/**
 * Auxiliary class to hold the current-size FIs as integer ranks to allow fast next-size itemsets generation.
 */
public class CurrSizeFiRanks implements Serializable {
    private final PairRanks currSizeRanks;
    private final long[][] currSizeFisR1ToRkm1;

    /**
     * @param fkAsPairs      list of triplets (elem1, elem2, frequency)
     *                       elem1 = frequent item
     *                       elem2 = (k-1)-size itemset represented as rank, where k current itemset size
     * @param totalFreqItems total number of all possible elem1's, i.e. total number of frequent items
     * @param totalFkm1      total number of all possible elem2's, i.e. total number of (k-1)-size FIs
     */
    public static CurrSizeFiRanks construct(List<int[]> fkAsPairs, int totalFreqItems, int totalFkm1) {
        Assert.isTrue(!fkAsPairs.isEmpty());
        Assert.isTrue(totalFreqItems > 0);
        Assert.isTrue(totalFkm1 > 0);

        //sort the incoming pairs by descending frequency - it's handy:
        List<int[]> sortedFk = getSortedByDecreasingFreq(fkAsPairs);
        //enumerate the k-FIs, i.e. give them integer ranks:
        PairRanks fkRanks = PairRanks.construct(sortedFk, totalFreqItems, totalFkm1);
        //construct a bit set per item: bs[r<k-1>]=1 <=> rankK exists as a pair (item, r<k-1>):
        long[][] currSizeFisR1ToRkm1 = fkRanks.constructElem1ToElem2BitSet();

        return new CurrSizeFiRanks(fkRanks, currSizeFisR1ToRkm1);
    }

    int[] getCurrSizeFiAsPairByRank(int rank) {
        return currSizeRanks.rankToPair[rank];
    }

    int getTotalCurrSizeRanks() {
        return currSizeRanks.totalRanks();
    }

    int getTotalPrevSizeRanks() {
        return currSizeRanks.totalElems2();
    }

    int getTotalFreqItems() {
        return currSizeRanks.totalElems1();
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

    long[] getPrevRanksForCurrSizeFisAsBitSet(int item) {
        return currSizeFisR1ToRkm1[item];
    }

    int[] getPrevSizeRankToCurrSizeRank(int item) {
        return currSizeRanks.getElem2ToRank(item);
    }

    private static List<int[]> getSortedByDecreasingFreq(List<int[]> currSizeFisAsPairs) {
        ArrayList<int[]> res = new ArrayList<>(currSizeFisAsPairs);
        Collections.sort(res, (o1, o2) -> Integer.compare(o2[2], o1[2]));
        return res;
    }

    private CurrSizeFiRanks(PairRanks currSizeRanks, long[][] currSizeFisR1ToRkm1) {
        this.currSizeRanks = currSizeRanks;
        this.currSizeFisR1ToRkm1 = currSizeFisR1ToRkm1;
    }
}
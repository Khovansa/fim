package org.openu.fimcmp.apriori;

import org.apache.commons.lang.ArrayUtils;
import org.openu.fimcmp.util.Assert;

import java.util.*;

/**
 * Map any FI rank to and from the actual itemset (i.e. list of items)
 */
public class FiRanksToFromItems {
    private final ArrayList<CurrSizeFiRanks> fiRanksKto2;
    private final int maxK;

    FiRanksToFromItems(CurrSizeFiRanks... fiRanksKto2) {
        this(Arrays.asList(fiRanksKto2));
    }

    FiRanksToFromItems(List<CurrSizeFiRanks> fiRanksKto2) {
        this.fiRanksKto2 = new ArrayList<>(fiRanksKto2);
        this.maxK = 1 + fiRanksKto2.size(); //rank mappers start from 2, but k starts from 1
    }

    FiRanksToFromItems toNextSize(CurrSizeFiRanks nextSizeFiRanks, FiRanksToFromItems prev) {
        List<CurrSizeFiRanks> newFiRanksList = new ArrayList<>(1 + prev.fiRanksKto2.size());
        newFiRanksList.add(nextSizeFiRanks);
        newFiRanksList.addAll(prev.fiRanksKto2);
        return new FiRanksToFromItems(newFiRanksList);
    }

    int getMaxK() {
        return maxK;
    }

    int[] getItemsetByMaxRank(int rankK) {
        return getItemsetByRank(rankK, maxK);
    }

    int[] getItemsetByRank(int rankK, int k) {
        Assert.isTrue(k <= maxK && k >= 1);

        List<Integer> res = new ArrayList<>(k);
        int currRank = rankK;
        Iterator<CurrSizeFiRanks> fiRanksIt = fiRanksKto2.listIterator(maxK - k);
        while (fiRanksIt.hasNext()) {
            CurrSizeFiRanks fiRanks = fiRanksIt.next();
            int[] pair = fiRanks.getCurrSizeFiAsPairByRank(currRank);
            //pair matching rankK = (item, rank<i-1>):
            res.add(pair[0]);
            currRank = pair[1]; //(i-1)-FI rank
        }
        res.add(currRank); //should be an item

        Assert.isTrue(res.size() == k);
        return ArrayUtils.toPrimitive(res.toArray(new Integer[k]));
    }

    int getRankByItemset(int[] itemset) {
        Assert.isTrue(itemset.length >= 1);
        Assert.isTrue(itemset.length <= maxK);

        if (itemset.length == 1) {
            return itemset[0];
        }

        final int k = itemset.length;
        int currRank = itemset[k-1]; //starting with r1 i.e. with an item
        int currItemInd = k - 2;
        while (currItemInd >= 0) {
            CurrSizeFiRanks fiRanks = fiRanksKto2.get(fiRanksKto2.size() - currItemInd - 1);
            int currItem = itemset[currItemInd];
            currRank = fiRanks.getCurrSizeFiRankByPair(currItem, currRank);
            Assert.isTrue(currRank >= 0);
            --currItemInd;
        }

        return currRank;
    }

    PairRanks constructRkToRkm1AndR1(int k) {
        Assert.isTrue(k <= maxK && k > 1);

        final CurrSizeFiRanks fiRanksK = fiRanksKto2.get(maxK - k);
        final int totalRanksK = fiRanksK.getTotalCurrSizeRanks();
        int[][] rkToRkm1AndR1 = new int[totalRanksK][];
        for (int rankK=0; rankK < totalRanksK; ++rankK) {
            int[] itemsetK = getItemsetByRank(rankK, k);
            Assert.isTrue(itemsetK.length == k);
            int[] itemsetKm1 = ArrayUtils.subarray(itemsetK, 0, itemsetK.length - 1);
            int rKm1 = getRankByItemset(itemsetKm1);
            int r1 = itemsetK[itemsetK.length - 1];
            rkToRkm1AndR1[rankK] = new int[]{rKm1, r1};
        }

        final int totalRkm1s = fiRanksK.getTotalPrevSizeRanks();
        final int totalR1s = fiRanksK.getTotalFreqItems();
        return PairRanks.constructByRankToPair(rkToRkm1AndR1, totalRkm1s, totalR1s);
    }
}

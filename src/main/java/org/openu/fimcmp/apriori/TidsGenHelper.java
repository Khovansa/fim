package org.openu.fimcmp.apriori;

import org.openu.fimcmp.util.Assert;

import java.util.List;
import java.util.Set;

/**
 * Auxiliary class to help to initialize the TID list per k-itemset (i.e. per its rank). <br/>
 * The TID list per k-itemset rank will be either represented directly or as 'not',
 * i.e. all the TIDs that don't contain this k-itemset.
 */
class TidsGenHelper {
    private final boolean[] rankToIsStoreContainingTids;

    static TidsGenHelper construct(List<int[]> fk, PairRanks rankPairsK, int totalTids) {
        Assert.isTrue(totalTids > 0);

        final int totalRanks = rankPairsK.totalRanks();
        int[] rankToSupport = new int[totalRanks]; //initialized with 0's
        for (int[] itemsetAsPairAndSupport : fk) {
            int rankK = rankPairsK.pairToRank[itemsetAsPairAndSupport[0]][itemsetAsPairAndSupport[1]];
            int support = itemsetAsPairAndSupport[2];
            rankToSupport[rankK] = support;
        }

        //whether to store TIDs that contain the itemset or to store TIDs that don't contain the itemset.
        //The decision is made per k-itemset, i.e. per k-itemset rank:
        boolean[] rankToIsStoreContainingTids = new boolean[totalRanks];
        for (int rank=0; rank<totalRanks; ++rank) {
            rankToIsStoreContainingTids[rank] = (2 * rankToSupport[rank] <= totalTids);
        }

        return new TidsGenHelper(rankToIsStoreContainingTids);
    }

    int totalRanks() {
        return rankToIsStoreContainingTids.length;
    }

    boolean isStoreTidForRank(int rank, Set<Integer> transactionRanks) {
        //store TID if the transaction contains the itemset and we should store the TIDs containing the itemset
        //OR if the transaction does not contain the itemset and we should store the TIDs NOT containing the itemset:
        return transactionRanks.contains(rank) == rankToIsStoreContainingTids[rank];
    }

    private TidsGenHelper(boolean[] rankToIsStoreContainingTids) {
        this.rankToIsStoreContainingTids = rankToIsStoreContainingTids;
    }
}

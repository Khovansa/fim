package org.openu.fimcmp.apriori;

import org.openu.fimcmp.util.Assert;
import org.openu.fimcmp.util.BitArrays;

import java.io.Serializable;
import java.util.BitSet;
import java.util.List;
import java.util.Set;

/**
 * Auxiliary class to help to initialize the TID list per k-itemset (i.e. per its rank). <br/>
 * The TID list per k-itemset rank will be either represented directly or as 'not',
 * i.e. all the TIDs that don't contain this k-itemset.
 */
public class TidsGenHelper implements Serializable {
    private final boolean[] rankToIsStoreContainingTids;
    private final int totalFreqItems;

    static TidsGenHelper construct(List<int[]> fk, PairRanks rankPairsK, int totalTids, int totalFreqItems) {
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

        return new TidsGenHelper(rankToIsStoreContainingTids, totalFreqItems);
    }

    int totalRanks() {
        return rankToIsStoreContainingTids.length;
    }

    public int getTotalFreqItems() {
        return totalFreqItems;
    }

    boolean isStoreTidForRank(int rank, BitSet transactionRanks) {
        //store TID if the transaction contains the itemset and we should store the TIDs containing the itemset
        //OR if the transaction does not contain the itemset and we should store the TIDs NOT containing the itemset:
        return transactionRanks.get(rank) == rankToIsStoreContainingTids[rank];
    }

    boolean isStoreTidForRank(int rank, long[] transactionRanksAsBitset) {
        //store TID if the transaction contains the itemset and we should store the TIDs containing the itemset
        //OR if the transaction does not contain the itemset and we should store the TIDs NOT containing the itemset:
        return BitArrays.get(transactionRanksAsBitset, 0, rank) == rankToIsStoreContainingTids[rank];
    }

    private TidsGenHelper(boolean[] rankToIsStoreContainingTids, int totalFreqItems) {
        this.rankToIsStoreContainingTids = rankToIsStoreContainingTids;
        this.totalFreqItems = totalFreqItems;
    }
}

package org.openu.fimcmp.apriori;

import org.openu.fimcmp.util.Assert;
import org.openu.fimcmp.util.BitArrays;

import java.io.Serializable;
import java.util.List;

/**
 * Auxiliary class to help to initialize the TID list per k-itemset (i.e. per its rank). <br/>
 * The TID list per k-itemset rank will be either represented directly or as 'not',
 * i.e. all the TIDs that don't contain this k-itemset.
 */
public class TidsGenHelper implements Serializable {
    private final boolean[] rankToIsStoreTids;
    private final long[] rankToIsStoreTidsBitSet;
    private final int[] rankToSupportCnt;
    private final int totalTids;

    static TidsGenHelper construct(List<int[]> fk, PairRanks rankPairsK, int totalTids) {
        Assert.isTrue(totalTids > 0);

        int[] rankToSupport = computeRankToSupport(fk, rankPairsK);
        boolean[] rankToIsStoreContainingTids = computeRankToIsStoreContainingTids(rankToSupport, totalTids);

        return new TidsGenHelper(rankToIsStoreContainingTids, rankToSupport, totalTids);
    }

    int totalRanks() {
        return rankToIsStoreTids.length;
    }

    int totalTids() {
        return totalTids;
    }

    /**
     * Sets 1's for ranks that the pair (current TID, rank) should be stored and processed. <br/>
     * The idea is to minimize the number of produced pairs (TID, rank). <br/>
     * The pair should be processed if either <ol>
     * <li>The rank is not very frequent and is contained in the current transaction, OR</li>
     * <li>The rank is very frequent and is not contained in the current transaction</li>
     * </ol>
     *  Rank is 'not very frequent' means that the rank is contained in less than half of transactions. <br/>
     * @param ranksBitSet   bitset of k-ranks contained in the current transaction
     */
    void setToResRanksToBeStoredBitSet(long[] res, int resStartInd, long[] ranksBitSet) {
        Assert.isTrue(rankToIsStoreTidsBitSet.length == ranksBitSet.length);
        System.arraycopy(rankToIsStoreTidsBitSet, 0, res, resStartInd, rankToIsStoreTidsBitSet.length);
        BitArrays.notXor(res, resStartInd, ranksBitSet, 0);
    }

    boolean isStoreContainingTid(int rank) {
        return rankToIsStoreTids[rank];
    }

    int getSupportCount(int rank) {
        return rankToSupportCnt[rank];
    }

    private static int[] computeRankToSupport(List<int[]> fk, PairRanks rankPairsK) {
        final int totalRanks = rankPairsK.totalRanks();
        int[] rankToSupport = new int[totalRanks]; //initialized with 0's
        for (int[] itemsetAsPairAndSupport : fk) {
            int rankK = rankPairsK.pairToRank[itemsetAsPairAndSupport[0]][itemsetAsPairAndSupport[1]];
            int support = itemsetAsPairAndSupport[2];
            rankToSupport[rankK] = support;
        }
        return rankToSupport;
    }

    /**
     * Whether to store TIDs that contain the itemset or to store TIDs that don't contain the itemset. <br/>
     * 'rankToIsStoreContainingTids[rank]=true' means that '1' in a TID bit set should be interpreted
     * as 'this itemset is contained in this transaction'. <br/>
     * The decision is made per k-itemset, i.e. per k-itemset rank. <br/>
     */
    private static boolean[] computeRankToIsStoreContainingTids(int[] rankToSupport, int totalTids) {
        final int totalRanks = rankToSupport.length;
        boolean[] rankToIsStoreContainingTids = new boolean[totalRanks];
        for (int rank=0; rank<totalRanks; ++rank) {
            rankToIsStoreContainingTids[rank] = (2 * rankToSupport[rank] <= totalTids);
        }
        return rankToIsStoreContainingTids;
    }

    private TidsGenHelper(boolean[] rankToIsStoreTids, int[] rankToSupport, int totalTids) {
        this.rankToIsStoreTids = rankToIsStoreTids;
        this.rankToIsStoreTidsBitSet = getRankToIsStoreTidsBitSet(rankToIsStoreTids);
        this.rankToSupportCnt = rankToSupport;
        this.totalTids = totalTids;
    }

    private static long[] getRankToIsStoreTidsBitSet(boolean[] rankToIsStoreTids) {
        final int BIT_SET_START_IND = 0;
        final int totalRanks = rankToIsStoreTids.length;
        long[] res = new long[BitArrays.requiredSize(totalRanks, BIT_SET_START_IND)];
        for (int rank = 0; rank< totalRanks; ++rank) {
            if (rankToIsStoreTids[rank]) {
                BitArrays.set(res, 0, rank);
            }
        }

        //setting the tail as 1's since notXor(1, 0) = 0 - that's what is required:
        int totalBits = BitArrays.totalBitsIn(res, BIT_SET_START_IND);
        for (int nonExistingTailRank=totalRanks; nonExistingTailRank < totalBits; ++nonExistingTailRank) {
            BitArrays.set(res, 0, nonExistingTailRank);
        }

        return res;
    }
}

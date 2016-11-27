package org.openu.fimcmp.apriori;

import org.openu.fimcmp.util.Assert;
import org.openu.fimcmp.util.BitArrays;

import java.io.Serializable;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

/**
 * Auxiliary class to help to initialize the TID list per k-itemset (i.e. per its rank). <br/>
 * The TID list per k-itemset rank will be either represented directly or as 'not',
 * i.e. all the TIDs that don't contain this k-itemset.
 */
public class TidsGenHelper implements Serializable, Rank1Provider {
    private final boolean[] rankToIsStoreTids;
    private final long[] rankToIsStoreTidsBitSet;
    private final PairRanks rankPairsK;
    private final int totalRanks1;
    private final int totalRanksKm1;

    static TidsGenHelper construct(List<int[]> fk, PairRanks rankPairsK, int totalTids) {
        Assert.isTrue(totalTids > 0);

        int[] rankToSupport = computeRankToSupport(fk, rankPairsK);
        boolean[] rankToIsStoreContainingTids = computeRankToIsStoreContainingTids(rankToSupport, totalTids);

        return new TidsGenHelper(rankToIsStoreContainingTids, rankPairsK);
    }

    int totalRanks() {
        return rankToIsStoreTids.length;
    }

    public int getTotalRanks1() {
        return totalRanks1;
    }

    public int getTotalRanksKm1() {
        return totalRanksKm1;
    }

    boolean isStoreTidForRank(int rank, BitSet transactionRanks) {
        //store TID if the transaction contains the itemset and we should store the TIDs containing the itemset
        //OR if the transaction does not contain the itemset and we should store the TIDs NOT containing the itemset:
        return transactionRanks.get(rank) == rankToIsStoreTids[rank];
    }

    boolean isStoreTidForRank(int rank, long[] transactionRanksAsBitset) {
        //store TID if the transaction contains the itemset and we should store the TIDs containing the itemset
        //OR if the transaction does not contain the itemset and we should store the TIDs NOT containing the itemset:
        return BitArrays.get(transactionRanksAsBitset, 0, rank) == rankToIsStoreTids[rank];
    }

    long[] getRanksToBeStoredBitSet(long[] ranksBitSet) {
        Assert.isTrue(rankToIsStoreTidsBitSet.length == ranksBitSet.length);
        long[] res = Arrays.copyOf(rankToIsStoreTidsBitSet, rankToIsStoreTidsBitSet.length);
        BitArrays.notXor(res, 0, ranksBitSet, 0);
        return res;
    }
    void setToResRanksToBeStoredBitSet(long[] res, int resStartInd, long[] ranksBitSet) {
        Assert.isTrue(rankToIsStoreTidsBitSet.length == ranksBitSet.length);
        System.arraycopy(rankToIsStoreTidsBitSet, 0, res, resStartInd, rankToIsStoreTidsBitSet.length);
        BitArrays.notXor(res, resStartInd, ranksBitSet, 0);
    }

    @Override
    public int getRank1(int rankK) {
        return rankPairsK.rankToPair[rankK][0];
    }

    public int getRankKm1(int rankK) {
        return rankPairsK.rankToPair[rankK][1];
    }

    int getRankK(int rank1, int rankKm1) {
        return rankPairsK.pairToRank[rank1][rankKm1];
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

    //Whether to store TIDs that contain the itemset or to store TIDs that don't contain the itemset.
    //The decision is made per k-itemset, i.e. per k-itemset rank:
    private static boolean[] computeRankToIsStoreContainingTids(int[] rankToSupport, int totalTids) {
        final int totalRanks = rankToSupport.length;
        boolean[] rankToIsStoreContainingTids = new boolean[totalRanks];
        for (int rank=0; rank<totalRanks; ++rank) {
            rankToIsStoreContainingTids[rank] = (2 * rankToSupport[rank] <= totalTids);
        }
        return rankToIsStoreContainingTids;
    }

    private TidsGenHelper(boolean[] rankToIsStoreTids, PairRanks rankPairsK) {
        this.rankToIsStoreTids = rankToIsStoreTids;

        this.rankToIsStoreTidsBitSet = getRankToIsStoreTidsBitSet(rankToIsStoreTids);

        this.rankPairsK = rankPairsK;
        this.totalRanks1 = 1 + rankPairsK.computeMaxElem1();
        this.totalRanksKm1 = 1 + rankPairsK.computeMaxElem2();
    }

    private static long[] getRankToIsStoreTidsBitSet(boolean[] rankToIsStoreTids) {
        final int totalRanks = rankToIsStoreTids.length;
        long[] res = new long[BitArrays.requiredSize(totalRanks - 1, 0)];
        for (int rank = 0; rank< totalRanks; ++rank) {
            if (rankToIsStoreTids[rank]) {
                BitArrays.set(res, 0, rank);
            }
        }

        //setting the tail as 1's since notXor(1, 0) = 0 - that's what is required:
        int totalBits = res.length * BitArrays.BITS_PER_WORD;
        for (int nonExistingTailRank=totalRanks; nonExistingTailRank < totalBits; ++nonExistingTailRank) {
            BitArrays.set(res, 0, nonExistingTailRank);
        }

        return res;
    }
}

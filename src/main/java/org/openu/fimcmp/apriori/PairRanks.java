package org.openu.fimcmp.apriori;

import org.openu.fimcmp.util.Assert;
import org.openu.fimcmp.util.BitArrays;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Mapping of pair (int, int) to int rank and vice versa. <br/>
 * The first elements of the pair are supposed to be in the range [0, totalElems1). <br/>
 * The second elements of the pair are supposed to be in the range [0, totalElems2). <br/>
 */
public class PairRanks implements Serializable {
    final int[][] rankToPair;
    final int[][] pairToRank;

    static PairRanks construct(List<int[]> sortedPairs, int totalElems1, int totalElems2) {
        int[][] rankToPair = constructPairRankToPair(sortedPairs);
        int[][] pairToRank = constructPairToRank(rankToPair, totalElems1, totalElems2);
        return new PairRanks(rankToPair, pairToRank);
    }

    boolean existsPair(int elem1, int elem2) {
        return pairToRank[elem1][elem2] >= 0;
    }

    int[] getElem2ToRank(int elem1) {
        return pairToRank[elem1];
    }

    long[][] constructElem1ToElem2BitSet() {
        final int START_IND=0;
        final int totalElems1 = totalElems1();
        final int totalElems2 = totalElems2();

        long[][] res = new long[totalElems1][];
        for (int elem1=0; elem1<totalElems1; ++elem1) {
            long[] elem2Bs = new long[BitArrays.requiredSize(totalElems2, START_IND)];
            for (int elem2=0; elem2 < totalElems2; ++elem2) {
                if (existsPair(elem1, elem2)) {
                    BitArrays.set(elem2Bs, START_IND, elem2);
                }
            }
            res[elem1] = elem2Bs;
        }
        return res;
    }

    int totalRanks() {
        return rankToPair.length;
    }

    int totalElems1() {
        return pairToRank.length;
    }

    int computeMaxElem1() {
        return computeMaxElem(0);
    }

    int totalElems2() {
        Assert.isTrue(pairToRank.length > 0);
        return pairToRank[0].length;
    }

    int computeMaxElem2() {
        return computeMaxElem(1);
    }

    private int computeMaxElem(int firstOrSecond) {
        int res = -1;
        for (int[] pair : rankToPair) {
            res = Math.max(res, pair[firstOrSecond]);
        }
        Assert.isTrue(res >= 0);
        return res;
    }
    private static int[][] constructPairRankToPair(List<int[]> sortedPairs) {
        final int arrSize = sortedPairs.size();
        int[][] res = new int[arrSize][];
        int rank = 0;
        for (int[] pair : sortedPairs) {
            res[rank++] = pair;
        }
        return res;
    }

    private static int[][] constructPairToRank(int[][] rankToPair, int totalElems1, int totalElems2) {
        Assert.isTrue(totalElems1 > 1);
        Assert.isTrue(totalElems2 > 1);

        //initialize the result
        int[][] res = new int[totalElems1][totalElems2];
        for (int[] col : res) {
            Arrays.fill(col, -1);
        }

        //have a map: pair -> rank
        for (int rank = 0; rank < rankToPair.length; ++rank) {
            int[] pair = rankToPair[rank];
            res[pair[0]][pair[1]] = rank;
        }
        return res;
    }

    private PairRanks(int[][] rankToPair, int[][] pairToRank) {
        this.rankToPair = rankToPair;
        this.pairToRank = pairToRank;
    }
}

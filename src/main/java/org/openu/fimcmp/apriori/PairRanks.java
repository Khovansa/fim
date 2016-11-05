package org.openu.fimcmp.apriori;

import org.openu.fimcmp.util.Assert;

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

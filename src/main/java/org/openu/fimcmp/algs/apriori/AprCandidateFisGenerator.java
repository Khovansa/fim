package org.openu.fimcmp.algs.apriori;

import org.openu.fimcmp.util.BitArrays;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Generate candidate itemsets of size i+1 from frequent itemsets of size i. <br/>
 * Necessary for next step Apriori.
 */
class AprCandidateFisGenerator implements Serializable {

    /**
     * Assumes that each transaction is represented as a sorted frequent item ranks (the more frequent ranks first)
     *
     * @return a mapping (frequent item rank, frequent item rank) -> count
     */
    Iterator<int[][]> countCands2_Part(Iterator<int[]> sortedTrIt, int totalFreqItems) {
        int[][] candToCount = new int[totalFreqItems][totalFreqItems];
        while (sortedTrIt.hasNext()) {
            int[] sortedTr = sortedTrIt.next();
            final int trSize = sortedTr.length;
            if (trSize <= 1) {
                continue;
            }

            for (int ii = 0; ii < trSize - 1; ++ii) {
                int item1 = sortedTr[ii];
                int[] col = candToCount[item1];
                for (int jj = ii + 1; jj < trSize; ++jj) {
                    int item2 = sortedTr[jj];
                    ++col[item2];
                }
            }
        }

        return Collections.singletonList(candToCount).iterator();
    }

    /**
     * Assumes that each transaction is represented as a pair (sorted frequent item ranks, bitset of (k-1)-FIs ranks).
     *
     * @return a mapping (frequent item rank, (k-1)-FI rank) -> count
     */
    Iterator<int[][]> countCandsK_Part(
            Iterator<Tuple2<int[], long[]>> f1AndFkm1BitSetIt,
            int km1, NextSizeItemsetGenHelper genHelper) {
        final int f1Size = genHelper.getTotalFreqItems();
        final int fKm1Size = genHelper.getTotalCurrSizeRanks();
        int[][] candToCount = new int[f1Size][fKm1Size]; //initialized to 0's
        while (f1AndFkm1BitSetIt.hasNext()) {
            Tuple2<int[], long[]> f1AndFkm1BitSet = f1AndFkm1BitSetIt.next();
            int[] f1 = f1AndFkm1BitSet._1;
            if (f1.length <= km1) {
                continue;
            }
            long[] fKm1BitSet = f1AndFkm1BitSet._2;
            if (BitArrays.isZerosOnly(fKm1BitSet, 0)) {
                continue;
            }

            final int resColumnsSize = f1.length - km1;
            for (int ii = 0; ii < resColumnsSize; ++ii) {
                int item = f1[ii];
                long[] hasChanceForNextRkm1Bs = genHelper.getCurrRanksForNextSizeCandsBitSet(item);
                long[] rKm1ForCandKsBitSet = BitArrays.andReturn(fKm1BitSet, hasChanceForNextRkm1Bs, 0, fKm1BitSet.length);
                int[] rKm1ForCandKs = BitArrays.asNumbers(rKm1ForCandKsBitSet, 0);
                int[] itemCol = candToCount[item];
                for (int rKm1 : rKm1ForCandKs) {
                    ++itemCol[rKm1];
                }
            }
        }

        return Collections.singletonList(candToCount).iterator();
    }

    int[][] mergeCounts_Part(int[][] cnt1, int[][] cnt2) {
        if (cnt2.length == 0) {
            return cnt1;
        }
        if (cnt1.length == 0) {
            return arrCopy2D(cnt2);
        }

        for (int ii = 0; ii < cnt1.length; ++ii) {
            int[] col1 = cnt1[ii];
            int[] col2 = cnt2[ii];
            for (int jj = 0; jj < col1.length; ++jj) {
                col1[jj] += col2[jj];
            }
        }
        return cnt1;
    }

    private int[][] arrCopy2D(int[][] src) {
        int[][] res = new int[src.length][];
        for (int ii = 0; ii < res.length; ++ii) {
            res[ii] = Arrays.copyOf(src[ii], src[ii].length);
        }
        return res;
    }

    /**
     * @return pair (sorted frequent item ranks, bit array of 2-FI ranks). <br/>
     * This method is intended to be applied per transaction.
     */
    Tuple2<int[], long[]> toSortedRanks1AndBitArrayOfRanks2(int[] sortedTr, CurrSizeFiRanks f2RanksHelper) {
        long[] ranks2 = computeBitArrayOfRanks2(sortedTr, f2RanksHelper);
        return new Tuple2<>(sortedTr, ranks2);
    }

    /**
     * @return pair (sorted frequent item ranks, bit array of k-FI ranks). <br/>
     * This method is intended to be applied per transaction.
     */
    Tuple2<int[], long[]> toSortedRanks1AndBitArrayOfRanksK(
            int[] sortedTr, long[] sortedRanksKm1, CurrSizeFiRanks fkRanksHelper) {
        long[] ranksK = computeBitArrayOfRanksK(sortedTr, sortedRanksKm1, fkRanksHelper);
        return new Tuple2<>(sortedTr, ranksK);
    }

    /**
     * Convert the column to pairs and filter out infrequent ones.
     *
     * @param col col[0]=elem1, the rest is: col[elem2_rank + 1] = count
     */
    List<int[]> fkColToPairs(int[] col, long minSuppCount) {
        if (col.length <= 1) {
            return Collections.emptyList();
        }

        List<int[]> res = new ArrayList<>(col.length - 1);
        int elem1 = col[0];
        for (int ii = 1; ii < col.length; ++ii) {
            final int count = col[ii];
            if (count >= minSuppCount) {
                int elem2 = ii - 1;
                res.add(new int[]{elem1, elem2, count});
            }
        }

        return res;
    }


    private long[] computeBitArrayOfRanks2(int[] sortedTr, CurrSizeFiRanks f2RanksHelper) {
        final int START_IND = 0;
        long[] resRanks2 = new long[BitArrays.requiredSize(f2RanksHelper.getTotalCurrSizeRanks(), START_IND)];
        final int arrSize = sortedTr.length;
        for (int ii = 0; ii < arrSize; ++ii) {
            int elem1 = sortedTr[ii];
            int[] itemToR2 = f2RanksHelper.getPrevSizeRankToCurrSizeRank(elem1);
            for (int jj = ii + 1; jj < arrSize; ++jj) {
                int elem2 = sortedTr[jj];
                int rank2 = itemToR2[elem2];
                if (rank2 >= 0) {
                    BitArrays.set(resRanks2, START_IND, rank2);
                }
            }
        }

        return resRanks2;
    }

    private long[] computeBitArrayOfRanksK(int[] sortedTr, long[] sortedRanksKm1Bs, CurrSizeFiRanks fkRanksHelper) {
        final int START_IND = 0;
        long[] resRanksK = new long[BitArrays.requiredSize(fkRanksHelper.getTotalCurrSizeRanks(), START_IND)];

        for (int r1 : sortedTr) {
            long[] possibleRkm1sBs = fkRanksHelper.getPrevRanksForCurrSizeFisAsBitSet(r1);
            long[] actRkm1sBs = BitArrays.andReturn(sortedRanksKm1Bs, possibleRkm1sBs, START_IND, sortedRanksKm1Bs.length);
            final int[] actRkm1s = BitArrays.asNumbers(actRkm1sBs, START_IND);
            final int[] rKm1ToRk = fkRanksHelper.getPrevSizeRankToCurrSizeRank(r1);
            for (int rKm1 : actRkm1s) {
                int rankK = rKm1ToRk[rKm1];
                BitArrays.set(resRanksK, START_IND, rankK);
            }
        }

        return resRanksK;
    }
}

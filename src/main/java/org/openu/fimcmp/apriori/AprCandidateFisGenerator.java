package org.openu.fimcmp.apriori;

import org.openu.fimcmp.util.BitArrays;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Generate candidate itemsets of size i+1 from frequent itemsets of size i. <br/>
 * Necessary for next step Apriori.
 */
public class AprCandidateFisGenerator implements Serializable {
    private static final int[][] EMPTY_COLS = {};

    int[][] genTransactionC2s_Direct(int[] sortedTr) {
        final int trSize = sortedTr.length;
        if (trSize <= 1) {
            return EMPTY_COLS;
        }

        int[][] res = new int[trSize - 1][];
        for (int ii = 0; ii < trSize - 1; ++ii) {
            final int secondElemsCnt = trSize - 1 - ii;
            int[] resCol = res[ii] = new int[secondElemsCnt + 1];
            resCol[0] = sortedTr[ii]; //the first element of the pair
            //Adding the second elements of the pairs (whose first element is sortedTr[ii]):
            System.arraycopy(sortedTr, ii+1, resCol, 1, secondElemsCnt);
        }

        return res;
    }

    int[][] genNextSizeCands_ByItems_BitSet_Direct(
            int currItemsetSize, Tuple2<int[], long[]> itemsAndCurrItemsets,
            NextSizeItemsetGenHelper genHelper) {
        int[] sortedTr = itemsAndCurrItemsets._1;
        final int trSize = sortedTr.length;
        if (trSize <= currItemsetSize) {
            return EMPTY_COLS;
        }
        long[] currItemsetsBitSet = itemsAndCurrItemsets._2;
        int[] currItemsets = BitArrays.asNumbers(currItemsetsBitSet, 0);
        if (currItemsets.length == 0) {
            return EMPTY_COLS;
        }

        final int resColumnsSize = trSize - currItemsetSize;
        int[][] res = new int[resColumnsSize][];
        for (int ii = 0; ii < resColumnsSize; ++ii) {
            int item = sortedTr[ii];
            res[ii] = genNextSizeCandsForItem_Direct(item, currItemsets, genHelper);
        }

        return res;
    }

    private int[] genNextSizeCandsForItem_Direct(
            int item, int[] currItemsetRanks, NextSizeItemsetGenHelper genHelper) {

        int totalRanksK = genHelper.getTotalCurrSizeRanks();
        long[] fkBitSet = genHelper.getFkBitSet(item);

        int[] res = new int[totalRanksK + 1];
        int resInd = 0;
        res[resInd++] = item;
        for (int itemsetRank : currItemsetRanks) {
            if (BitArrays.get(fkBitSet, 0, itemsetRank)) {
                res[resInd++] = itemsetRank;
            }
        }

        return Arrays.copyOf(res, resInd);
    }

    Tuple2<int[], long[]> toSortedRanks1And2_BitSet_Tmp(int[] sortedTr, CurrSizeFiRanks f2RanksHelper) {
        long[] ranks2 = computeSortedRanks2_BitSet_Tmp(sortedTr, f2RanksHelper);
        return new Tuple2<>(sortedTr, ranks2);
    }

    Tuple2<int[], long[]> toSortedRanks1AndK_BitSet(
            int[] sortedTr, long[] sortedRanksKm1, CurrSizeFiRanks fkRanksHelper) {
        long[] ranksK = computeSortedRanksK_BitSet(sortedTr, sortedRanksKm1, fkRanksHelper);
        return new Tuple2<>(sortedTr, ranksK);
    }

    long[] getRankToTidNew2D_AllAtOnce_BitSet2(long[] kRanksBs, long tid, TidsGenHelper tidsGenHelper) {
        long[] res = new long[kRanksBs.length + 1];
        res[0] = tid;
        tidsGenHelper.setToResRanksToBeStoredBitSet(res, 1, kRanksBs);
        return res;
    }

    int[] mergeElem_Direct(int totalCands, int[] col, int[] elem) {
        if (elem.length <= 1) {
            return col;
        }
        if (col.length == 0) {
            col = new int[totalCands + 1];
            col[0] = elem[0];
        }
        for (int ii=1; ii<elem.length; ++ii) {
            int rank = elem[ii];
            ++col[rank+1];
        }
        return col;
    }

    int[] mergeColumns_Direct(int[] col1, int[] col2) {
        if (col2.length == 0) {
            return col1;
        }
        if (col1.length == 0) {
            return Arrays.copyOf(col2, col2.length);
        }
        for (int ii=1; ii<col2.length; ++ii) {
            col1[ii] += col2[ii];
        }
        return col1;
    }

    List<int[]> fkColToPairs_Direct(int[] col, long minSuppCount) {
        if (col.length <= 1) {
            return Collections.emptyList();
        }

        int resLen = (col.length - 1);
        List<int[]> res = new ArrayList<>(resLen);
        int item1 = col[0];
        for (int ii = 1; ii < col.length; ++ii) {
            if (col[ii] >= minSuppCount) {
                res.add(new int[]{item1, ii-1, col[ii]});
            }
        }
        return res;
    }


    private long[] computeSortedRanks2_BitSet_Tmp(int[] sortedTr, CurrSizeFiRanks f2RanksHelper) {
        final int START_IND = 0;
        long[] resRanks2 = new long[BitArrays.requiredSize(f2RanksHelper.getTotalCurrSizeRanks()-1, START_IND)];
        final int arrSize = sortedTr.length;
        for (int ii = 0; ii < arrSize; ++ii) {
            int elem1 = sortedTr[ii];
            for (int jj = ii + 1; jj < arrSize; ++jj) {
                int elem2 = sortedTr[jj];
                int rank2 = f2RanksHelper.getCurrSizeFiRankByPair(elem1, elem2);
                if (rank2 >= 0) {
                    BitArrays.set(resRanks2, START_IND, rank2);
                }
            }
        }

        return resRanks2;
    }

    private long[] computeSortedRanksK_BitSet(int[] sortedTr, long[] sortedRanksKm1Bs, CurrSizeFiRanks fkRanksHelper) {
        final int START_IND = 0;
        long[] resRanksK = new long[BitArrays.requiredSize(fkRanksHelper.getTotalCurrSizeRanks()-1, START_IND)];

        int[] wordNums = new int[BitArrays.BITS_PER_WORD];  //tmp buffer to hold the current word's numbers
        for (int wordInd=START_IND; wordInd<sortedRanksKm1Bs.length; ++wordInd) {
            long word = sortedRanksKm1Bs[wordInd];
            if (word == 0) {
                continue;
            }
            int resInd = BitArrays.getWordBitsAsNumbersToArr(wordNums, word, START_IND, wordInd);
            for (int numInd=0; numInd<resInd; ++numInd) {
                int rankKm1 = wordNums[numInd];
                for (int elem1 : sortedTr) {
                    int rankK = fkRanksHelper.getCurrSizeFiRankByPair(elem1, rankKm1);
                    if (rankK >= 0) {
                        BitArrays.set(resRanksK, START_IND, rankK);
                    }
                }
            }
        }

        return resRanksK;
    }
}

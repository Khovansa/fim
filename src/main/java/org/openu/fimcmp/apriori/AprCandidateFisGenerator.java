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

    int[][] genCandsOfSize2(int[] sortedTr) {
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
            System.arraycopy(sortedTr, ii + 1, resCol, 1, secondElemsCnt);
        }

        return res;
    }

    int[][] genCandsOfNextSize(
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
            res[ii] = genCandsOfNextSizeForItem(item, currItemsets, genHelper);
        }

        return res;
    }

    private int[] genCandsOfNextSizeForItem(
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

    Iterator<int[][]> countNextSizeCands_Part(
            Iterator<Tuple2<int[], long[]>> itemsAndCurrItemsetsIt,
            int currItemsetSize, NextSizeItemsetGenHelper genHelper) {
        int totalRanksK = genHelper.getTotalCurrSizeRanks();
        int f1Size = genHelper.getTotalFreqItems();
        int[][] candToCount = new int[f1Size][totalRanksK]; //initialized to 0's
        while (itemsAndCurrItemsetsIt.hasNext()) {
            Tuple2<int[], long[]> itemsAndCurrItemsets = itemsAndCurrItemsetsIt.next();
            int[] sortedTr = itemsAndCurrItemsets._1;
            final int trSize = sortedTr.length;
            if (trSize <= currItemsetSize) {
                continue;
            }
            long[] currItemsetsBitSet = itemsAndCurrItemsets._2;
            if (BitArrays.isZerosOnly(currItemsetsBitSet, 0)) {
                continue;
            }

            final int resColumnsSize = trSize - currItemsetSize;
            for (int ii = 0; ii < resColumnsSize; ++ii) {
                int item = sortedTr[ii];
                long[] fkBitSet = genHelper.getFkBitSet(item);
                long[] matchingItemsetsKBs = BitArrays.andReturn(fkBitSet, currItemsetsBitSet, 0, fkBitSet.length);
                int[] matchingItemsetsK = BitArrays.asNumbers(matchingItemsetsKBs, 0);
                int[] itemCol = candToCount[item];
                for (int itemsetRank : matchingItemsetsK) {
                    ++itemCol[itemsetRank];
                }
            }
        }

        return Collections.singletonList(candToCount).iterator();
    }

    Tuple2<int[], long[]> toSortedRanks1And2(int[] sortedTr, CurrSizeFiRanks f2RanksHelper) {
        long[] ranks2 = computeSortedRanks2(sortedTr, f2RanksHelper);
        return new Tuple2<>(sortedTr, ranks2);
    }

    Tuple2<int[], long[]> toSortedRanks1AndK(
            int[] sortedTr, long[] sortedRanksKm1, CurrSizeFiRanks fkRanksHelper) {
        long[] ranksK = computeSortedRanksK(sortedTr, sortedRanksKm1, fkRanksHelper);
        return new Tuple2<>(sortedTr, ranksK);
    }

    /**
     * @return res[0]=tid, the rest is a bitset of k-ranks with 1's for cases
     * when a pair (TID, rankK) should be stored and processed. <br/>
     *
     * See {@link TidsGenHelper#setToResRanksToBeStoredBitSet} for details.
     */
    long[] getTidAndRanksToBeStored(long[] kRanksBs, long tid, TidsGenHelper tidsGenHelper) {
        long[] res = new long[kRanksBs.length + 1];
        res[0] = tid;
        tidsGenHelper.setToResRanksToBeStoredBitSet(res, 1, kRanksBs);
        return res;
    }

    /**
     * Add 1 to each element in 'col' contained in 'elem'.
     *
     * @param col  col[0]=elem1, the rest is: col[elem2_rank + 1] = count
     * @param elem elem[0]=elem1, the rest is a list of elem2 ranks
     */
    int[] mergeCountingElem(int totalCands, int[] col, int[] elem) {
        if (elem.length <= 1) {
            return col;
        }

        if (col.length == 0) {
            col = new int[totalCands + 1];
            col[0] = elem[0];
        }

        for (int ii = 1; ii < elem.length; ++ii) {
            int rank = elem[ii];
            ++col[rank + 1];
        }

        return col;
    }

    /**
     * Sum the counters of each element. <br/>
     * See {@link #mergeCountingElem} for the structure of each column
     */
    int[] mergeCountingColumns(int[] col1, int[] col2) {
        if (col2.length == 0) {
            return col1;
        }
        if (col1.length == 0) {
            return Arrays.copyOf(col2, col2.length);
        }
        for (int ii = 1; ii < col2.length; ++ii) {
            col1[ii] += col2[ii];
        }
        return col1;
    }

    /**
     * Convert the column to pairs and filter out infrequent ones.
     *
     * @param col see {@link #mergeCountingElem} for the structure
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


    private long[] computeSortedRanks2(int[] sortedTr, CurrSizeFiRanks f2RanksHelper) {
        final int START_IND = 0;
        long[] resRanks2 = new long[BitArrays.requiredSize(f2RanksHelper.getTotalCurrSizeRanks(), START_IND)];
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

    private long[] computeSortedRanksK(int[] sortedTr, long[] sortedRanksKm1Bs, CurrSizeFiRanks fkRanksHelper) {
        final int START_IND = 0;
        long[] resRanksK = new long[BitArrays.requiredSize(fkRanksHelper.getTotalCurrSizeRanks(), START_IND)];

        //iterating over sortedRanksKm1Bs:
        int[] wordNums = BitArrays.newBufForWordNumbers();  //tmp buffer to hold the current word's numbers
        for (int wordInd = START_IND; wordInd < sortedRanksKm1Bs.length; ++wordInd) {
            long word = sortedRanksKm1Bs[wordInd];
            if (word == 0) {
                continue;
            }
            int resInd = BitArrays.getWordBitsAsNumbersToArr(wordNums, word, START_IND, wordInd);
            for (int numInd = 0; numInd < resInd; ++numInd) {
                int rankKm1 = wordNums[numInd];
                //iterating over sortedTr:
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

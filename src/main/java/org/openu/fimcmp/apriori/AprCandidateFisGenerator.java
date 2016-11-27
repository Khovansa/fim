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
    private static final int[] EMPTY_COL = {};
    private static final int[] EMPTY_COL_0 = {0};
    private static final int[][] EMPTY_COLS = {};

    /**
     * Pairs {(0, 3), (0, 6), (0, 9), (3, 6), (3, 9), (6, 9)} will be held as followed: <br/>
     * res[0] = [0, 3, 1, 6, 1, 9, 1] <br/>
     * res[1] = [3, 1, 6, 1, 9, 1] <br/>
     * res[2] = [6, 9, 1] <br/>
     * That is, the first element of each column is the first element of the pair. <br/>
     * The rest of the elements in the column are the second elements followed by their counters. <br/>
     * The counters produced by this function are always 1's, but a later reduce operation will sum them. <br/>
     * E.g. [3, 6, 1, 9, 1] stands for {((3, 6), 1), ((3, 9), 1)}.
     */
    //TODO: add partitioner
    int[][] genTransactionC2s(int[] sortedTr) {
        final int trSize = sortedTr.length;
        if (trSize <= 1) {
            return EMPTY_COLS;
        }

        int[][] res = new int[trSize - 1][];
        for (int ii = 0; ii < trSize - 1; ++ii) {
            int[] resCol = res[ii] = new int[2 * (trSize - ii - 1) + 1];
            int resColInd = 0;
            resCol[resColInd++] = sortedTr[ii]; //the first element of the pair
            //Adding the second elements of the pairs (whose first element is sortedTr[ii]):
            for (int jj = ii + 1; jj < trSize; ++jj) {
                resCol[resColInd++] = sortedTr[jj];
                resCol[resColInd++] = 1;
            }
        }

        return res;
    }

    /**
     * Return the same structure as {@link #genTransactionC2s}, but for triplets and potentially larger itemsets. <br/>
     * Store a pair rank (integer) instead of a pair as a second element. <br/>
     * In other words, triplets are represented as pairs (elem1, pairRank) and then
     * group these pairs as described in {@link #genTransactionC2s}.
     */
    //TODO: add partitioner
    int[][] genNextSizeCands_ByItems(
            int currItemsetSize, Tuple2<int[], int[]> itemsAndCurrItemsets,
            NextSizeItemsetGenHelper genHelper) {
        int[] sortedTr = itemsAndCurrItemsets._1;
        int[] currItemsets = itemsAndCurrItemsets._2;
        final int trSize = sortedTr.length;
        if (trSize <= currItemsetSize || currItemsets.length == 0) {
            return EMPTY_COLS;
        }

        final int resColumnsSize = trSize - currItemsetSize;
        int[][] res = new int[resColumnsSize][];
        for (int ii = 0; ii < resColumnsSize; ++ii) {
            int item = sortedTr[ii];
            res[ii] = genNextSizeCandsForItem(item, currItemsets, genHelper);
        }

        return res;
    }
    int[][] genNextSizeCands_ByItems_BitSet(
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
            res[ii] = genNextSizeCandsForItem(item, currItemsets, genHelper);
        }

        return res;
    }

    private int[] genNextSizeCandsForItem(
            int item, int[] currItemsetRanks, NextSizeItemsetGenHelper genHelper) {

        List<Integer> filteredItemsetRanks = getFilteredItemsetRanksForItem(item, currItemsetRanks, genHelper);

        return createColumn(item, filteredItemsetRanks);
    }

    private List<Integer> getFilteredItemsetRanksForItem(
            int item, int[] currItemsetRanks, NextSizeItemsetGenHelper genHelper) {
        List<Integer> filteredItemsetRanks = new ArrayList<>(currItemsetRanks.length);
        for (int itemsetRank : currItemsetRanks) {
            if (genHelper.isGoodNextSizeItemset(item, itemsetRank)) {
                filteredItemsetRanks.add(itemsetRank);
            }
        }
        return filteredItemsetRanks;
    }

    /**
     * See {@link #genTransactionC2s(int[])} for the column structure
     */
    private int[] createColumn(int elem1, List<Integer> elem2s) {
        int elem2sCnt = elem2s.size();
        if (elem2sCnt == 0) {
            //slight violation for efficiency - it does not matter what is the first element if there are no pairs:
            return EMPTY_COL_0;
        }

        int[] resCol = new int[2 * elem2sCnt + 1];
        int resColInd = 0;
        resCol[resColInd++] = elem1; //the first element of the new pair (i.e. triplet)
        //Adding the second elements of the pairs (whose first element is 'item'):
        for (int elem2 : elem2s) {
            resCol[resColInd++] = elem2;
            resCol[resColInd++] = 1;
        }
        return resCol;
    }

    Tuple2<int[], int[]> toSortedRanks1And2(int[] sortedTr, CurrSizeFiRanks f2RanksHelper) {
        int[] ranks2 = computeSortedRanks2(sortedTr, f2RanksHelper);
        return new Tuple2<>(sortedTr, ranks2);
    }
    Tuple2<int[], long[]> toSortedRanks1And2_BitSet_Tmp(int[] sortedTr, CurrSizeFiRanks f2RanksHelper) {
        long[] ranks2 = computeSortedRanks2_BitSet_Tmp(sortedTr, f2RanksHelper);
        return new Tuple2<>(sortedTr, ranks2);
    }

    Tuple2<int[], int[]> toSortedRanks1AndK(int[] sortedTr, int[] sortedRanksKm1, CurrSizeFiRanks fkRanksHelper) {
        int[] ranksK = computeSortedRanksK(sortedTr, sortedRanksKm1, fkRanksHelper);
        return new Tuple2<>(sortedTr, ranksK);
    }

    Tuple2<int[], long[]> toSortedRanks1AndK_BitSet(
            int[] sortedTr, long[] sortedRanksKm1, CurrSizeFiRanks fkRanksHelper) {
        long[] ranksK = computeSortedRanksK_BitSet(sortedTr, sortedRanksKm1, fkRanksHelper);
        return new Tuple2<>(sortedTr, ranksK);
    }

    /**
     * Return array of [rank, tid] for all ranks that require this
     * (see {@link TidsGenHelper#isStoreTidForRank} for details. <br/>
     */
    long[][] getRankToTid(int[] transactionRanksK, long tid, TidsGenHelper tidsGenHelper) {
        final int totalRanks = tidsGenHelper.totalRanks();
        BitSet ranksSet = new BitSet(totalRanks);
        for (int rank : transactionRanksK) {
            ranksSet.set(rank);
        }

        //count the result:
        int resCnt = 0;
        for (int rank = 0; rank < totalRanks; ++rank) {
            if (tidsGenHelper.isStoreTidForRank(rank, ranksSet)/* || ranksSet.get(rank)*/) {
                ++resCnt;
            }
        }

        //create the result:
        long[][] res = new long[resCnt][];
        int resInd = 0;
        for (int rank = 0; rank < totalRanks; ++rank) {
            if (tidsGenHelper.isStoreTidForRank(rank, ranksSet)) {
                res[resInd++] = new long[]{rank, tid};
            }
//            else if (ranksSet.get(rank)) {
//                res[resInd++] = new long[]{rank, -1}; //need to have it for the case this rank is present in all transactions
//            }
        }
        return res;
    }
    long[] getRankToTidNew(int[] transactionRanksK, long tid, TidsGenHelper tidsGenHelper) {
        final int START_IND = 1;
        final int totalRanks = tidsGenHelper.totalRanks();
        long[] res = new long[BitArrays.requiredSize(totalRanks - 1, START_IND)];
        res[0] = tid;
//        BitArrays.setAll(res, 1, transactionRanksK);

        long[] ranksSet = new long[BitArrays.requiredSize(totalRanks - 1, 0)];
        for (int rank : transactionRanksK) {
            BitArrays.set(ranksSet, 0, rank);
        }

        for (int rank = 0; rank < totalRanks; ++rank) {
            if (tidsGenHelper.isStoreTidForRank(rank, ranksSet)) {
                BitArrays.set(res, START_IND, rank);
            }
        }
        return res;
    }
    long[][] getRankToTidNew2D_AllAtOnce(int[] transactionRanksK, long tid, TidsGenHelper tidsGenHelper) {
        final int START_IND = 2;
        final int totalR1s = tidsGenHelper.getTotalRanks1();
        final int totalRanksKm1 = tidsGenHelper.getTotalRanksKm1();
        long[][] res = new long[totalR1s][];

        final int totalRanks = tidsGenHelper.totalRanks();
        long[] ranksSet = new long[BitArrays.requiredSize(totalRanks, 0)];
        for (int rank : transactionRanksK) {
            BitArrays.set(ranksSet, 0, rank);
        }

        for (int rankK = 0; rankK < totalRanks; ++rankK) {
            if (!tidsGenHelper.isStoreTidForRank(rankK, ranksSet)) {
                continue;
            }
            int rank1 = tidsGenHelper.getRank1(rankK);
            int rankKm1 = tidsGenHelper.getRankKm1(rankK);
            long[] resCol = res[rank1];
            if (resCol == null) {
                resCol = res[rank1] = new long[BitArrays.requiredSize(totalRanksKm1, START_IND)];
                resCol[0] = rank1;
                resCol[1] = tid;
            }
            BitArrays.set(resCol, START_IND, rankKm1);
        }

        return res;
    }

    long[][] getRankToTidNew2D_AllAtOnce_BitSet(long[] kRanksBs, long tid, TidsGenHelper tidsGenHelper) {
        final int RES_START_IND = 2;
        final int INPUT_START_IND = 0;
        final int totalR1s = tidsGenHelper.getTotalRanks1();
        final int totalRanksKm1 = tidsGenHelper.getTotalRanksKm1();
        final int resArrSize1D = BitArrays.requiredSize(totalRanksKm1, RES_START_IND);
        long[][] res = new long[totalR1s][];

        long[] ranksToStoreBitSet = tidsGenHelper.getRanksToBeStoredBitSet(kRanksBs);
        //iterate over 'ranksToStoreBitSet' and fill res[rank1's]=bit set of matching rank(k-1)'s:
        int[] wordNums = new int[BitArrays.BITS_PER_WORD];  //tmp buffer to hold the current word's numbers
        for (int wordInd = INPUT_START_IND; wordInd < ranksToStoreBitSet.length; ++wordInd) {
            long word = ranksToStoreBitSet[wordInd];
            if (word == 0) {
                continue;
            }
            int resInd = BitArrays.getWordBitsAsNumbersToArr(wordNums, word, INPUT_START_IND, wordInd);
            for (int numInd=0; numInd<resInd; ++numInd) {
                int rankK = wordNums[numInd];
                int rank1 = tidsGenHelper.getRank1(rankK);
                int rankKm1 = tidsGenHelper.getRankKm1(rankK);
                long[] resCol = res[rank1];
                if (resCol == null) {
                    resCol = res[rank1] = new long[resArrSize1D];
                    resCol[0] = rank1;
                    resCol[1] = tid;
                }
                BitArrays.set(resCol, RES_START_IND, rankKm1);
            }
        }

        return res;
    }
    long[] getRankToTidNew2D_AllAtOnce_BitSet2(long[] kRanksBs, long tid, TidsGenHelper tidsGenHelper) {
        long[] res = new long[kRanksBs.length + 1];
        res[0] = tid;
        tidsGenHelper.setToResRanksToBeStoredBitSet(res, 1, kRanksBs);
        return res;
    }

    /**
     * See {@link #genTransactionC2s} for columns structure. <br/>
     * E.g. {0, 3, 1, 6, 1, 9, 1}. <br/>
     * That is, after the first item, we use a kind of linked list (2nd item, count),
     * i.e. a sparse representation of a map: item -> count.
     */
    int[] mergeColumns(int[] col1, int[] col2) {
        if (col1.length <= 1) {
            return Arrays.copyOf(col2, col2.length);
        }
        if (col2.length <= 1) {
            return col1;
        }

        int diffItemsCount = getDifferentItemsCountInColumns(col1, col2);
        int resLength = 1 + 2 * diffItemsCount;
        if (resLength > col1.length) {
            int[] res = new int[resLength];
            mergeColumnsToRes(res, col1, col2);
            return res;
        } else {
            mergeColumnsToCol1(col1, col2);
            return col1;
        }
    }

    /**
     * The TID-list column structure is: [rank, tid1, tid2, ... tidN]
     */
    long[] mergeTids(long[] col1, long[] col2) {
        if (col1.length <= 1 || col1[1] < 0) {
            return Arrays.copyOf(col2, col2.length);
        }
        if (col2.length <= 1 || col2[1] < 0) {
            return col1;
        }

        int diffTidsCount = getDifferentTidsCount(col1, col2);
        int resLength = 1 + diffTidsCount;
        //check whether col1 or col2 already contain all the TIDs from both columns:
        if (resLength == col1.length) {
            return col1;
        }
        if (resLength == col2.length) {
            return Arrays.copyOf(col2, col2.length);
        }

        //need to create a new array:
        long[] res = new long[resLength];
        mergeTidListsToRes(res, col1, col2);
        return res;
    }

    /**
     * Filter columns by min support. <br/>
     * See {@link #genTransactionC2s} for the column structure. <br/>
     * E.g. {0, 3, 1, 6, 1, 9, 1}. <br/>
     */
    int[] getColumnsFilteredByMinSupport(int[] col, long minSuppCount) {
        int goodElemCnt = 0;
        for (int ii = 2; ii < col.length; ii += 2) {
            if (col[ii] >= minSuppCount) {
                ++goodElemCnt;
            }
        }

        int resLen = 1 + 2 * goodElemCnt;
        if (resLen == col.length) {
            return col; //all pairs are frequent
        }
        if (goodElemCnt == 0) {
            //slight violation of the column structure for efficiency: it had to be {col[0]}:
            return EMPTY_COL;
        }

        int[] res = new int[resLen];
        int resInd = 0;
        res[resInd++] = col[0];
        for (int ii = 2; ii < col.length; ii += 2) {
            if (col[ii] >= minSuppCount) {
                res[resInd++] = col[ii - 1];  //elem
                res[resInd++] = col[ii];    //count
            }
        }
        return res;
    }

    List<int[]> fkColToPairs(int[] col) {
        if (col.length <= 1) {
            return Collections.emptyList();
        }

        int resLen = (col.length - 1) / 2;
        List<int[]> res = new ArrayList<>(resLen);
        int item1 = col[0];
        for (int ii = 1; ii < col.length; ii += 2) {
            res.add(new int[]{item1, col[ii], col[ii + 1]});
        }
        return res;
    }

    private int getDifferentItemsCountInColumns(int[] col1, int[] col2) {
        int i1 = 1, i2 = 1; //1 since skipping over the first element of the pair
        int res = 0;
        while (i1 < col1.length && i2 < col2.length) {
            int cmp = Integer.compare(col1[i1], col2[i2]);
            ++res;
            if (cmp <= 0) {
                i1 += 2; //2 since skipping over the item counters
            }
            if (cmp >= 0) {
                i2 += 2;
            }
        }

        res += ((col1.length - i1) + (col2.length - i2)) / 2; //length of tails, skipping the counters
        return res;
    }

    private int getDifferentTidsCount(long[] col1, long[] col2) {
        int i1 = 1, i2 = 1; //1 since skipping over the 'rank'
        int res = 0;
        while (i1 < col1.length && i2 < col2.length) {
            int cmp = Long.compare(col1[i1], col2[i2]);
            ++res;
            if (cmp <= 0) {
                ++i1;
            }
            if (cmp >= 0) {
                ++i2;
            }
        }

        res += ((col1.length - i1) + (col2.length - i2)); //length of the tails
        return res;
    }

    private void mergeColumnsToRes(int[] res, int[] col1, int[] col2) {
        res[0] = col1[0]; //the first elem of the pair
        int i1 = 1, i2 = 1, ir = 1;
        while (i1 < col1.length && i2 < col2.length) {
            int cmp = Integer.compare(col1[i1], col2[i2]);
            if (cmp < 0) {
                res[ir++] = col1[i1++]; //elem
                res[ir++] = col1[i1++]; //counter
            } else if (cmp > 0) {
                res[ir++] = col2[i2++]; //elem
                res[ir++] = col2[i2++]; //counter
            } else {
                res[ir++] = col1[i1++]; //elem
                ++i2;
                res[ir++] = col1[i1++] + col2[i2++]; //counter
            }
        }

        //tails:
        if (i1 < col1.length) {
            System.arraycopy(col1, i1, res, ir, col1.length - i1);
        } else if (i2 < col2.length) {
            System.arraycopy(col2, i2, res, ir, col2.length - i2);
        }
    }

    private void mergeTidListsToRes(long[] res, long[] col1, long[] col2) {
        res[0] = col1[0]; //the rank
        int i1 = 1, i2 = 1, ir = 1;
        while (i1 < col1.length && i2 < col2.length) {
            int cmp = Long.compare(col1[i1], col2[i2]);
            if (cmp < 0) {
                res[ir++] = col1[i1++];
            } else if (cmp > 0) {
                res[ir++] = col2[i2++];
            } else {
                res[ir++] = col1[i1++];
                ++i2;
            }
        }

        //tails:
        if (i1 < col1.length) {
            System.arraycopy(col1, i1, res, ir, col1.length - i1);
        } else if (i2 < col2.length) {
            System.arraycopy(col2, i2, res, ir, col2.length - i2);
        }
    }


    //Assumes col2 elements set is a subset of col1's elements
    private void mergeColumnsToCol1(int[] col1, int[] col2) {
        int i1 = 1, i2 = 1;
        while (i1 < col1.length && i2 < col2.length) {
            if (col1[i1] == col2[i2]) {
                col1[i1 + 1] += col2[i2 + 1]; //sum the counts
                //move both to the next element:
                i1 += 2;
                i2 += 2;
            } else {
                i1 += 2; //col2 has no such element => this element's count is unchanged
            }
        }
    }

    private int[] computeSortedRanks2(int[] sortedTr, CurrSizeFiRanks f2RanksHelper) {
        final int arrSize = sortedTr.length;
        int[] ranks = new int[arrSize * (arrSize - 1) / 2];
        int resInd = 0;
        for (int ii = 0; ii < arrSize; ++ii) {
            int elem1 = sortedTr[ii];
            for (int jj = ii + 1; jj < sortedTr.length; ++jj) {
                int elem2 = sortedTr[jj];
                int rank = f2RanksHelper.getCurrSizeFiRankByPair(elem1, elem2);
                if (rank >= 0) {
                    ranks[resInd++] = rank;
                }
            }
        }

        Arrays.sort(ranks, 0, resInd);
        return Arrays.copyOf(ranks, resInd);
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

    private int[] computeSortedRanksK(int[] sortedTr, int[] sortedRanksKm1, CurrSizeFiRanks fkRanksHelper) {
        final int elem1Cnt = sortedTr.length;
        final int elem2Cnt = sortedRanksKm1.length;
        int[] resRanksK = new int[elem1Cnt * elem2Cnt];
        int resInd = 0;
        for (int elem1 : sortedTr) {
            for (int rankKm1 : sortedRanksKm1) {
                int rankK = fkRanksHelper.getCurrSizeFiRankByPair(elem1, rankKm1);
                if (rankK >= 0) {
                    resRanksK[resInd++] = rankK;
                }
            }
        }

        Arrays.sort(resRanksK, 0, resInd);
        return Arrays.copyOf(resRanksK, resInd);
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

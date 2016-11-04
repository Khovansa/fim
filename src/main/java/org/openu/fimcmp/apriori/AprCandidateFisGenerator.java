package org.openu.fimcmp.apriori;

import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Generate candidate itemsets of size i+1 from frequent itemsets of size i. <br/>
 * Necessary for next step Apriori.
 */
public class AprCandidateFisGenerator implements Serializable {
    private static final Integer[] EMPTY_COL = {};
    private static final Integer[] EMPTY_COL_0 = {0};
    private static final Integer[][] EMPTY_COLS = {};

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
    Integer[][] genTransactionC2s(Integer[] sortedTr) {
        final int trSize = sortedTr.length;
        if (trSize <= 1) {
            return EMPTY_COLS;
        }

        Integer[][] res = new Integer[trSize - 1][];
        for (int ii = 0; ii < trSize - 1; ++ii) {
            Integer[] resCol = res[ii] = new Integer[2 * (trSize - ii - 1) + 1];
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
    Integer[][] genNextSizeCands_ByItems(
            int currItemsetSize, Tuple2<Integer[], Integer[]> itemsAndCurrItemsets,
            NextSizeItemsetGenHelper genHelper) {
        Integer[] sortedTr = itemsAndCurrItemsets._1;
        Integer[] currItemsets = itemsAndCurrItemsets._2;
        final int trSize = sortedTr.length;
        if (trSize <= currItemsetSize || currItemsets.length == 0) {
            return EMPTY_COLS;
        }

        final int resColumnsSize = trSize - currItemsetSize;
        Integer[][] res = new Integer[resColumnsSize][];
        for (int ii = 0; ii < resColumnsSize; ++ii) {
            Integer item = sortedTr[ii];
            res[ii] = genNextSizeCandsForItem(item, currItemsets, genHelper);
        }

        return res;
    }

    private Integer[] genNextSizeCandsForItem(
            Integer item, Integer[] currItemsetRanks, NextSizeItemsetGenHelper genHelper) {

        List<Integer> filteredItemsetRanks = getFilteredItemsetRanksForItem(item, currItemsetRanks, genHelper);

        return createColumn(item, filteredItemsetRanks);
    }

    private List<Integer> getFilteredItemsetRanksForItem(
            Integer item, Integer[] currItemsetRanks, NextSizeItemsetGenHelper genHelper) {
        List<Integer> filteredItemsetRanks = new ArrayList<>(currItemsetRanks.length);
        for (Integer itemsetRank : currItemsetRanks) {
            if (genHelper.isGoodNextSizeItemset(item, itemsetRank)) {
                filteredItemsetRanks.add(itemsetRank);
            }
        }
        return filteredItemsetRanks;
    }

    /**
     * See {@link #genTransactionC2s(Integer[])} for the column structure
     */
    private Integer[] createColumn(Integer elem1, List<Integer> elem2s) {
        int elem2sCnt = elem2s.size();
        if (elem2sCnt == 0) {
            //slight violation for efficiency - it does not matter what is the first element if there are no pairs:
            return EMPTY_COL_0;
        }

        Integer[] resCol = new Integer[2 * elem2sCnt + 1];
        int resColInd = 0;
        resCol[resColInd++] = elem1; //the first element of the new pair (i.e. triplet)
        //Adding the second elements of the pairs (whose first element is 'item'):
        for (Integer elem2 : elem2s) {
            resCol[resColInd++] = elem2;
            resCol[resColInd++] = 1;
        }
        return resCol;
    }

    Tuple2<Integer[], Integer[]> toSortedRanks1And2(Integer[] sortedTr, PreprocessedF2 preprocessedF2) {
        Integer[] ranks2 = computeSortedRanks2(sortedTr, preprocessedF2);
        return new Tuple2<>(sortedTr, ranks2);
    }

    /**
     * See {@link #genTransactionC2s} for columns structure. <br/>
     * E.g. {0, 3, 1, 6, 1, 9, 1}. <br/>
     * That is, after the first item, we use a kind of linked list (2nd item, count),
     * i.e. a sparse representation of a map: item -> count.
     */
    Integer[] mergeC2Columns(Integer[] col1, Integer[] col2) {
        if (col1.length <= 1) {
            return Arrays.copyOf(col2, col2.length);
        }
        if (col2.length <= 1) {
            return col1;
        }

        int diffItemsCount = getDifferentItemsCountForC2s(col1, col2);
        int resLength = 1 + 2 * diffItemsCount;
        if (resLength > col1.length) {
            Integer[] res = new Integer[resLength];
            mergeC2ColumnsToRes(res, col1, col2);
            return res;
        } else {
            mergeC2ColumnsToCol1(col1, col2);
            return col1;
        }
    }

    /**
     * Filter C2's by min support. <br/>
     * See {@link #genTransactionC2s} for the column structure. <br/>
     * E.g. {0, 3, 1, 6, 1, 9, 1}. <br/>
     */
    Integer[] getC2sFilteredByMinSupport(Integer[] col, long minSuppCount) {
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

        Integer[] res = new Integer[resLen];
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

    List<Integer[]> f2ColToPairs(Integer[] col) {
        if (col.length <= 1) {
            return Collections.emptyList();
        }

        int resLen = (col.length - 1) / 2;
        List<Integer[]> res = new ArrayList<>(resLen);
        Integer item1 = col[0];
        for (int ii = 1; ii < col.length; ii += 2) {
            res.add(new Integer[]{item1, col[ii], col[ii + 1]});
        }
        return res;
    }

    private int getDifferentItemsCountForC2s(Integer[] col1, Integer[] col2) {
        if (col1.length <= 1) {
            return col2.length;
        }

        int i1 = 1, i2 = 1; //1 since skipping over the first element of the pair
        int res = 0;
        while (i1 < col1.length && i2 < col2.length) {
            int cmp = col1[i1].compareTo(col2[i2]);
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

    private void mergeC2ColumnsToRes(Integer[] res, Integer[] col1, Integer[] col2) {
        res[0] = (col1.length > 0) ? col1[0] : col2[0];
        int i1 = 1, i2 = 1, ir = 1;
        while (i1 < col1.length && i2 < col2.length) {
            int cmp = col1[i1].compareTo(col2[i2]);
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
        while (i1 < col1.length) {
            res[ir++] = col1[i1++]; //elem
            res[ir++] = col1[i1++]; //counter
        }
        while (i2 < col2.length) {
            res[ir++] = col2[i2++]; //elem
            res[ir++] = col2[i2++]; //counter
        }
    }

    //Assumes col2 elements set is a subset of col1's elements
    private void mergeC2ColumnsToCol1(Integer[] col1, Integer[] col2) {
        int i1 = 1, i2 = 1;
        while (i1 < col1.length && i2 < col2.length) {
            if (col1[i1].equals(col2[i2])) {
                col1[i1 + 1] += col2[i2 + 1]; //sum the counts
                //move both to the next element:
                i1 += 2;
                i2 += 2;
            } else {
                i1 += 2; //col2 has no such element => this element's count is unchanged
            }
        }
    }

    private Integer[] computeSortedRanks2(Integer[] sortedTr, PreprocessedF2 preprocessedF2) {
        final int arrSize = sortedTr.length - 1;
        List<Integer> ranks = new ArrayList<>(arrSize * (arrSize - 1) / 2);
        //OPTIMIZATION: skipping the 1st element - the pairs are expected to be the new 2nd elem:
        for (int ii = 1; ii < arrSize; ++ii) {
            Integer elem1 = sortedTr[ii];
            for (int jj = ii + 1; jj < sortedTr.length; ++jj) {
                Integer elem2 = sortedTr[jj];
                int rank = preprocessedF2.getPairRank(elem1, elem2);
                if (rank >= 0) {
                    ranks.add(rank);
                }
            }
        }

        ranks.sort(null);
        return ranks.toArray(new Integer[ranks.size()]);
    }
}

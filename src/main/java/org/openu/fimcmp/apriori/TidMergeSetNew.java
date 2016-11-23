package org.openu.fimcmp.apriori;

import org.apache.commons.lang.NotImplementedException;
import org.openu.fimcmp.util.Assert;
import org.openu.fimcmp.util.BitArrays;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Set of TIDs optimized for merge operations. <br/>
 * Holds everything in long[]. <br/>
 * To allow working with RDD&lt;long[]> rather than with RDD&lt;TidMergeSet>,
 * has all its operations as static, taking long[] as its first argument. <br/>
 * <b>WARNING: assumes all the TIDs are in the range [0, total), see RDD.zipWithIndex()</b><br/>
 * <p>
 * <pre>
 * The array structure is: {rank, size, firstElem, lastElem, (bitset of TIDs)}.
 * Bitset of TIDs:
 * - The size is 2 x total / 64
 * - Each even element holds a bitset for the 64 TIDs in the range [ii, ii+63], where ii = elem index / 2
 * - Each odd element holds a pointer to the next element
 * This way we
 * (1) Avoid re-allocations
 * (2) merge(s1, s2) only takes O(s2.size / 64) operations,
 *     e.g. if it holds only 2 elements it will take about 2 operations
 * </pre>
 */
public class TidMergeSetNew implements Serializable {
    private static final int RANK_IND = 0;
    private static final int SIZE_IND = 1;
    private static final int MIN_ELEM_IND = 2;
    private static final int MAX_ELEM_IND = 3;
    private static final int FIRST_ELEM_IND = 4;


    static long[][] mergeElem2D(
            long[][] rankKm1ToTidSet,
            Tuple3<Integer, Integer, Long> rank1RankKAndTid,
            long totalTids,
            TidsGenHelper tidsGenHelper) {

        if (rankKm1ToTidSet.length == 0) {
            rankKm1ToTidSet = new long[tidsGenHelper.getTotalRanksKm1()][];
        }

        final int rankK = rank1RankKAndTid._2();
        final long tid = rank1RankKAndTid._3();
        final int rankKm1 = tidsGenHelper.getRankKm1(rankK);
        long[] tidSet = rankKm1ToTidSet[rankKm1];
        rankKm1ToTidSet[rankKm1] = mergeElemFast(tidSet, rankK, tid, totalTids);

        return rankKm1ToTidSet;
    }
    static long[][] mergeElem2D_AllAtOnce(
            long[][] rankKm1ToTidSet,
            long[] elem_r1TidRkm1BitSet,
            long totalTids,
            TidsGenHelper tidsGenHelper) {

        if (rankKm1ToTidSet.length == 0) {
            rankKm1ToTidSet = new long[tidsGenHelper.getTotalRanksKm1()][];
        }

        final int START_IND = 2;
        final int rank1 = (int)elem_r1TidRkm1BitSet[0];
        final long tid = elem_r1TidRkm1BitSet[1];

        int[] wordNums = new int[BitArrays.BITS_PER_WORD];  //tmp buffer to hold the current word's numbers
        for (int wordInd=START_IND; wordInd<elem_r1TidRkm1BitSet.length; ++wordInd) {
            long word = elem_r1TidRkm1BitSet[wordInd];
            if (word != 0) {
                int resInd = BitArrays.getWordBitsAsNumbersToArr(wordNums, word, START_IND, wordInd);
                for (int numInd=0; numInd<resInd; ++numInd) {
                    int rankKm1 = wordNums[numInd];
                    int rankK = tidsGenHelper.getRankK(rank1, rankKm1);
                    long[] tidSet = rankKm1ToTidSet[rankKm1];
                    rankKm1ToTidSet[rankKm1] = mergeElemFast(tidSet, rankK, tid, totalTids);
                }
            }
        }
        return rankKm1ToTidSet;
    }

    static long[][] mergeSets2D(long[][] s1, long[][] s2) {
        throw new NotImplementedException();
    }


    /**
     * <pre>
     * Cases:
     * - {}: initial empty set, need to be replaced at least by {rank, ...} - created by Spark
     * - {rank}: 'normal' empty set that includes rank - created by us
     * - {rank, -1}: single element (itemset rank);
     *   the case when the itemset is present in a transaction, but we only aggregate those that are not present
     * - {rank, TID}: single element (itemset rank) - normal case when we want to aggregate the TID for this itemset
     * - {rank, size, first_elem_ind, last_elem_ind, bitset, ...} - the normal set
     * </pre>
     */
    static long[] mergeElem(long[] tidSet, long[] rankAndTid, long totalTids) {
        final int rank = (int) rankAndTid[0];
        final long tid = rankAndTid[1];
        return mergeElem(tidSet, rank, tid, totalTids);
    }

    static long[] mergeElem(long[] tidSet, Tuple2<Integer, Long> rankAndTid, long totalTids) {
        final int rank = rankAndTid._1;
        final long tid = rankAndTid._2;
        return mergeElem(tidSet, rank, tid, totalTids);
    }

    static long[] mergeElem(long[] tidSet, int rank, long tid, long totalTids) {
        if (tidSet == null || tidSet.length <= 1) {
            return newSetWithElem(rank, tid, totalTids);
        } else {
            addElemToExistingSet(tidSet, tid);
            return tidSet;
        }
    }

    static long[] mergeElemFast(long[] tidSet, int rank, long tid, long totalTids) {
        if (tidSet != null && tidSet.length > 1) {
            BitArrays.set(tidSet, FIRST_ELEM_IND, (int) tid); //requires to set min, max and size later
            return tidSet;
        } else {
            return newSetWithElem(rank, tid, totalTids);
        }
    }

    static long[] withMetadata(long[] tidSet) {
        long[] res = Arrays.copyOf(tidSet, tidSet.length);
        setMetadata(res);
        return res;
    }

    private static void setMetadata(long[] res) {
        res[MIN_ELEM_IND] = BitArrays.min(res, FIRST_ELEM_IND);
        res[MAX_ELEM_IND] = BitArrays.max(res, FIRST_ELEM_IND);
        res[SIZE_IND] = BitArrays.cardinality(res, FIRST_ELEM_IND);
    }

    /**
     * Assuming the two sets' ranges do not intersect
     */
    static long[] mergeSets(long[] s1, long[] s2) {
        if (s2.length <= 1) {
            return s1;
        }
        if (s1.length <= 1) {
            return copyOf(s2);
        }

        if (s1[SIZE_IND] <= 1) {
            setMetadata(s1);
        }
        if (s2[SIZE_IND] <= 1) {
            setMetadata(s2);
        }
        long[] lowerSet = (s1[MIN_ELEM_IND] < s2[MIN_ELEM_IND]) ? s1 : s2;
        long[] higherSet = (s1[MIN_ELEM_IND] < s2[MIN_ELEM_IND]) ? s2 : s1;
        Assert.isTrue(lowerSet[MAX_ELEM_IND] < higherSet[MIN_ELEM_IND]);

        long[] res = new long[BitArrays.requiredSize((int) higherSet[MAX_ELEM_IND], FIRST_ELEM_IND)];
        res[RANK_IND] = lowerSet[RANK_IND];
        res[SIZE_IND] = lowerSet[SIZE_IND] + higherSet[SIZE_IND];
        res[MIN_ELEM_IND] = lowerSet[MIN_ELEM_IND];
        res[MAX_ELEM_IND] = higherSet[MAX_ELEM_IND];

        //first set
        System.arraycopy(lowerSet, FIRST_ELEM_IND, res, FIRST_ELEM_IND, (lowerSet.length - FIRST_ELEM_IND));
        //second set
        int destInd = BitArrays.wordIndex((int) higherSet[MIN_ELEM_IND], FIRST_ELEM_IND);
        System.arraycopy(higherSet, FIRST_ELEM_IND, res, destInd, (higherSet.length - FIRST_ELEM_IND));

        return res;
    }

    static int count(long[] tidSet) {
        if (tidSet.length <= 1) {
            return 0;
        } else {
            return BitArrays.cardinality(tidSet, FIRST_ELEM_IND);
        }
    }

    static long[] describeAsList(long[] tidSet) {
        long[] res = new long[4];
        res[0] = tidSet[RANK_IND];
        if (tidSet.length > 1) {
            res[1] = tidSet[SIZE_IND];
            res[2] = tidSet[MIN_ELEM_IND];
            res[3] = tidSet[MAX_ELEM_IND];
//            res.add((long)count(tidSet));
        }

        return res;
    }


    private static long[] copyOf(long[] s2) {
        return Arrays.copyOf(s2, s2.length);
    }

    private static long[] newSetWithElem(int rank, long tid, long totalTids) {
        long[] res = new long[BitArrays.requiredSize((int) totalTids - 1, FIRST_ELEM_IND)];
        res[RANK_IND] = rank;
        res[SIZE_IND] = 1;
        int tidAsInt = (int) tid;
        res[MIN_ELEM_IND] = res[MAX_ELEM_IND] = tidAsInt;
        BitArrays.set(res, FIRST_ELEM_IND, tidAsInt);
        return res;
    }

    private static void addElemToExistingSet(long[] tidSet, long tid) {
        int tidAsInt = (int) tid;
        tidSet[MIN_ELEM_IND] = Math.min(tidSet[MIN_ELEM_IND], tidAsInt);
        tidSet[MAX_ELEM_IND] = Math.max(tidSet[MAX_ELEM_IND], tidAsInt);
        if (!BitArrays.get(tidSet, FIRST_ELEM_IND, tidAsInt)) {
            ++tidSet[SIZE_IND];
            BitArrays.set(tidSet, FIRST_ELEM_IND, tidAsInt);
        }
    }
}

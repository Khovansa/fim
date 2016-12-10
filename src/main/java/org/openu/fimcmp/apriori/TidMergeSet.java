package org.openu.fimcmp.apriori;

import org.apache.commons.lang.NotImplementedException;
import org.openu.fimcmp.util.Assert;
import org.openu.fimcmp.util.BitArrays;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

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
public class TidMergeSet implements Serializable {
    private static final int RANK_IND = 0;
    private static final int SIZE_IND = 1;
    private static final int MIN_ELEM_IND = 2;
    private static final int MAX_ELEM_IND = 3;
    private static final int BITSET_START_IND = 4;

    /**
     * Return an iterator over a single long[][] array: rankK -> 'tid-set'. <br/>
     * The 'tid-set' is a bitset of TIDs prefixed with some metadata.
     */
    static Iterator<long[][]> processPartition(
            Iterator<Tuple2<long[], Long>> kRanksBsAndTidIt, TidsGenHelper tidsGenHelper, long totalTids) {
        long[][] rankKToTidSet = new long[tidsGenHelper.totalRanks()][];
        long minTid = Long.MAX_VALUE;
        long maxTid = Long.MIN_VALUE;
        boolean hasElems = false;
        while(kRanksBsAndTidIt.hasNext()) {
            hasElems = true;
            Tuple2<long[], Long> kRanksBsAndTid = kRanksBsAndTidIt.next();
            long[] kRanksBs = kRanksBsAndTid._1;

            long[] kRanksToBeStoredBs = new long[kRanksBs.length];
            tidsGenHelper.setToResRanksToBeStoredBitSet(kRanksToBeStoredBs, 0, kRanksBs);
            int[] kRanksToBeStored = BitArrays.asNumbers(kRanksToBeStoredBs, 0);

            final long tid = kRanksBsAndTid._2;
            minTid = Math.min(minTid, tid);
            maxTid = Math.max(maxTid, tid);
            for (int rankK : kRanksToBeStored) {
                long[] tidSet = rankKToTidSet[rankK];
                if (tidSet != null) {
                    BitArrays.set(tidSet, BITSET_START_IND, (int) tid); //requires to set min, max and size later
                } else {
                    rankKToTidSet[rankK] = newSetWithElem(rankK, tid, totalTids);
                }
            }
        }

        if (hasElems) {
            int startInd = indexOfTid(minTid);
            int endIndExc = 1 + indexOfTid(maxTid);
            for (long[] tidSet : rankKToTidSet) {
                setMetadata(tidSet, startInd, endIndExc);
            }
        }

        return Collections.singletonList(rankKToTidSet).iterator();
    }

    static long[][] mergePartitions(long[][] part1, long[][] part2, TidsGenHelper tidsGenHelper) {
        if (part2.length == 0) {
            return part1;
        }
        final int totalRanks = tidsGenHelper.totalRanks();
        if (part1.length == 0) {
            part1 = new long[totalRanks][];
        }

        for (int rankK = 0; rankK < totalRanks; ++rankK) {
            part1[rankK] = mergeTidSets(part1[rankK], part2[rankK]);
        }
        return part1;
    }

    static long[][] mergeElem(
            long[][] rankKToTidSet,
            long[] elem_tidRkBitSet,
            long totalTids,
            TidsGenHelper tidsGenHelper) {

        if (rankKToTidSet.length == 0) {
            rankKToTidSet = new long[tidsGenHelper.totalRanks()][];
        }

        final int START_IND = 1;
        final long tid = elem_tidRkBitSet[0];

        int[] wordNums = BitArrays.newBufForWordNumbers();  //tmp buffer to hold the current word's numbers
        for (int wordInd=START_IND; wordInd<elem_tidRkBitSet.length; ++wordInd) {
            long word = elem_tidRkBitSet[wordInd];
            if (word == 0) {
                continue;
            }
            int resInd = BitArrays.getWordBitsAsNumbersToArr(wordNums, word, START_IND, wordInd);
            for (int numInd=0; numInd<resInd; ++numInd) {
                int rankK = wordNums[numInd];
                long[] tidSet = rankKToTidSet[rankK];
                rankKToTidSet[rankK] = addTid(tidSet, rankK, tid, totalTids);
            }
        }
        return rankKToTidSet;
    }

    static long[][] mergeSets(long[][] s1, long[][] s2) {
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
    private static long[] addTid(long[] tidSet, int rank, long tid, long totalTids) {
        if (tidSet != null && tidSet.length > 1) {
            BitArrays.set(tidSet, BITSET_START_IND, (int) tid); //requires to set min, max and size later
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
        res[MIN_ELEM_IND] = BitArrays.min(res, BITSET_START_IND);
        res[MAX_ELEM_IND] = BitArrays.max(res, BITSET_START_IND);
        res[SIZE_IND] = BitArrays.cardinality(res, BITSET_START_IND);
    }

    private static void setMetadata(long[] tidSet, int startInd, int endIndExc) {
        if (tidSet == null) {
            return;
        }

        tidSet[MIN_ELEM_IND] = BitArrays.min(tidSet, startInd, endIndExc);
        tidSet[MAX_ELEM_IND] = BitArrays.max(tidSet, startInd, endIndExc);
        tidSet[SIZE_IND] = BitArrays.cardinality(tidSet, startInd, endIndExc);
    }

    /**
     * Assuming the two sets' ranges do not intersect
     */
    static long[] mergeTidSets(long[] s1AndRes, long[] s2) {
        if (s2 == null || s2.length <= 1) {
            return s1AndRes;
        }
        if (s1AndRes == null || s1AndRes.length <= 1) {
            return copyOf(s2);
        }

        final boolean s1IsLower = s1AndRes[MIN_ELEM_IND] < s2[MIN_ELEM_IND];
        long[] lowerSet = s1IsLower ? s1AndRes : s2;
        long[] higherSet = s1IsLower ? s2 : s1AndRes;
        Assert.isTrue(lowerSet[MAX_ELEM_IND] < higherSet[MIN_ELEM_IND]);

        s1AndRes[RANK_IND] = lowerSet[RANK_IND];
        s1AndRes[SIZE_IND] = lowerSet[SIZE_IND] + higherSet[SIZE_IND];
        s1AndRes[MIN_ELEM_IND] = lowerSet[MIN_ELEM_IND];
        s1AndRes[MAX_ELEM_IND] = higherSet[MAX_ELEM_IND];

        if (s1IsLower) {
            //copy the higher set
            int startInd = indexOfTid(higherSet[MIN_ELEM_IND]);
            int endIndExc = 1 + indexOfTid(higherSet[MAX_ELEM_IND]);
            int copyLen = endIndExc - startInd;
            System.arraycopy(higherSet, startInd, s1AndRes, startInd, copyLen);
        } else {
            //copy the lower set
            int startInd = indexOfTid(lowerSet[MIN_ELEM_IND]);
            int endIndExc = 1 + indexOfTid(lowerSet[MAX_ELEM_IND]);
            int copyLen = endIndExc - startInd;
            System.arraycopy(lowerSet, startInd, s1AndRes, startInd, copyLen);
        }

        return s1AndRes;
    }

    private static int indexOfTid(long tid) {
        return BitArrays.wordIndex((int)tid, BITSET_START_IND);
    }

    static int count(long[] tidSet) {
        if (tidSet.length <= 1) {
            return 0;
        } else {
            return BitArrays.cardinality(tidSet, BITSET_START_IND);
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
        long[] res = new long[BitArrays.requiredSize((int) totalTids, BITSET_START_IND)];
        res[RANK_IND] = rank;
        res[SIZE_IND] = 1;
        int tidAsInt = (int) tid;
        res[MIN_ELEM_IND] = res[MAX_ELEM_IND] = tidAsInt;
        BitArrays.set(res, BITSET_START_IND, tidAsInt);
        return res;
    }
}

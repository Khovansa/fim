package org.openu.fimcmp.apriori;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Set of TIDs optimized for merge operations. <br/>
 * Holds everything in long[]. <br/>
 * To allow working with RDD&lt;long[]> rather than with RDD&lt;TidMergeSet>,
 * has all its operations as static, taking long[] as its first argument. <br/>
 * <b>WARNING: assumes all the TIDs are in the range [0, total), see RDD.zipWithIndex()</b><br/>
 *
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
 *
 */
public class TidMergeSet implements Serializable {
    private static final int LONG_BITSETS_PER_PTR = 1; //we could have > 1 long bit sets per pointer

    // We split the TID bits to {index, remainder}.
    // 'index' determines the index in the long array,
    // while the remainder determines the bit to raise in the bitset hold by a long.
    // E.g. 1 long is 64=2^6 bits, meaning the number of the remainder bits is 6.
    private static final int REMAINDER_BITS_CNT = 5 + LONG_BITSETS_PER_PTR;
    private static final long INDEX_MASK = (-1L << REMAINDER_BITS_CNT); //1..1000000
    private static final long REMAINDER_MASK = (2L << REMAINDER_BITS_CNT)-1; //0..0111111

    private static final int RANK_IND = 0;
    private static final int SIZE_IND = 1;
    private static final int FIRST_ELEM_IND = 2;
    private static final int LAST_ELEM_IND = 3;
    private static final int AUXILIARY_FIELDS_CNT = 4; //rank, size, first, last

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
    static long[] merge(long[] s1, long[] s2, long totalTids) {
        //TODO
        return new long[]{};
    }

    static long[] mergeElem(long[] tidSet, long[] rankAndTid, long totalTids) {
        final int rank = (int)rankAndTid[0];
        final long tid = rankAndTid[1];

        if (tidSet.length <= 1) {
            return newSetWithElem(rank, tid, totalTids);
        } else {
            addElemToExistingSet(tidSet, tid);
            return tidSet;
        }
    }

    static long[] mergeSets(long[] s1, long[] s2) {
        if (s2.length <= 1) {
            return s1;
        }
        if (s1.length <= 1) {
            return copyOf(s2);
        }

        int currElemInd = (int)s2[FIRST_ELEM_IND];
        do {
            addBitsetToExistingSet(s1, currElemInd, s2[currElemInd]);
            currElemInd = (int)s2[currElemInd + LONG_BITSETS_PER_PTR];
        } while(currElemInd !=  0);
        return s1;
    }

    static List<Long> toNormalListOfTids(long[] tidSet, int maxTidsInRes) {
        if (tidSet.length == 0) {
            return Collections.emptyList();
        }
        if (tidSet.length == 1) {
            return Collections.singletonList(tidSet[0]);
        }

        ArrayList<Long> res = new ArrayList<>((int)tidSet[SIZE_IND] * 64);
        int currElemInd = (int)tidSet[FIRST_ELEM_IND];
        //reserve space for auxiliary stuff, '-1' will not move during the sorting:
        res.addAll(Arrays.asList(-1L, -1L, -1L));
        do {
            long bitSet = tidSet[currElemInd];
            addAsNormalTids(res, currElemInd, bitSet);
            currElemInd = (int)tidSet[currElemInd + LONG_BITSETS_PER_PTR];
        } while(currElemInd != 0);

//        res.sort(null);
        int actSize = res.size();
        res = new ArrayList<>(res.subList(0, 3 + Math.min(res.size(), maxTidsInRes)));
        res.set(0, tidSet[0]);
        res.set(1, (long) actSize);
        res.set(2, tidSet[1]);
        return res;
    }

    static int count(long[] tidSet) {
        if (tidSet.length <= 1) {
            return 0;
        }

        int res = 0;
        int currElemInd = (int)tidSet[FIRST_ELEM_IND];
        do {
            long bitSet = tidSet[currElemInd];
            List<Long> tmpRes = new ArrayList<>(64);
            addAsNormalTids(tmpRes, currElemInd, bitSet);
            res += tmpRes.size();
            currElemInd = (int)tidSet[currElemInd + LONG_BITSETS_PER_PTR];
        } while(currElemInd != 0);
        return res;
    }
    private static long[] copyOf(long[] s2) {
        long[] res = new long[s2.length];
        System.arraycopy(s2, 0, res, 0, AUXILIARY_FIELDS_CNT);

        //only go through the existing elements, not the entire array
        int currElemInd = (int)s2[FIRST_ELEM_IND];
        do {
            System.arraycopy(s2, currElemInd, res, currElemInd, LONG_BITSETS_PER_PTR+1);
            currElemInd = (int)s2[currElemInd + LONG_BITSETS_PER_PTR];
        } while(currElemInd !=  0);
        return res;
    }

    private static long[] newSetWithElem(int rank, long tid, long totalTids) {
        if (tid < 0) {
            //the case when the itemset (rank) is present in this transaction (TID),
            // but we only aggregate those that are not present
            return new long[]{rank};  //'zero' TID set for this rank
        }

        long[] res = new long[getArrayLen(totalTids)];
        res[RANK_IND] = rank;
        res[SIZE_IND] = 1;
        int index = getIndex(tid);
        res[FIRST_ELEM_IND] = res[LAST_ELEM_IND] = index;
        res[index] = getRemainderAsBit(tid);
        return res;
    }

    private static void addElemToExistingSet(long[] tidSet, long tid) {
        if (tid < 0) {
            //the case when the itemset (rank) is present in this transaction (TID),
            // but we only aggregate those that are not present
            return; //nothing to do
        }

        int newIndex = getIndex(tid);
        long bitSet = getRemainderAsBit(tid);
        addBitsetToExistingSet(tidSet, newIndex, bitSet);
    }

    private static void addBitsetToExistingSet(long[] tidSet, int newIndex, long bitSet) {
        if (tidSet[newIndex] != 0) {
            tidSet[newIndex] = tidSet[newIndex] | bitSet;
        } else {
            //a new element
            tidSet[newIndex] = bitSet;
            ++tidSet[SIZE_IND];
            int lastElemPtr = (int) tidSet[LAST_ELEM_IND] + LONG_BITSETS_PER_PTR;
            tidSet[LAST_ELEM_IND] = tidSet[lastElemPtr] = newIndex;
        }
    }

    private static void addAsNormalTids(List<Long> res, int currElemInd, long bitSet) {
        int indexInPureBitSet = (currElemInd - AUXILIARY_FIELDS_CNT) / (LONG_BITSETS_PER_PTR + 1);
        long base = (indexInPureBitSet << REMAINDER_BITS_CNT);

        long currBitSet = bitSet;
        int currBitInd = 0;
        while (currBitSet != 0) {
            int bit = (int)(currBitSet & 1);
            if (bit != 0) {
                res.add(base + currBitInd);
            }
            ++currBitInd;
            //shift and make sure the MSB=0 after the shift (not the case for negative numbers):
            currBitSet = (currBitSet >> 1) & 0x7fffffffffffffffL;
        }
    }

    //TODO: if LONG_BITSETS_PER_PTR > 1, need to return 2 numbers
    private static int getIndex(long tid) {
        int indexInPureBitSet = (int) ((tid & INDEX_MASK) >> REMAINDER_BITS_CNT);
        return AUXILIARY_FIELDS_CNT + (LONG_BITSETS_PER_PTR + 1) * indexInPureBitSet;
    }

    private static long getRemainderAsBit(long tid) {
        int remainder = (int)(tid & REMAINDER_MASK);
        return 1L<<remainder;
    }

    private static int getArrayLen(long totalTids) {
        int longBitSetCnt = (int)Math.ceil(1.0 * totalTids / 64);
        int ptrCnt = (int)Math.ceil(1.0 * longBitSetCnt / LONG_BITSETS_PER_PTR);
        return AUXILIARY_FIELDS_CNT + longBitSetCnt + ptrCnt;
    }

}

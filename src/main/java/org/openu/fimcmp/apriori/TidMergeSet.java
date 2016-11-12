package org.openu.fimcmp.apriori;

import java.io.Serializable;

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

    static long getIndex(long tid) {
        return (tid & INDEX_MASK) >> REMAINDER_BITS_CNT;
    }

    static int getRemainder(long tid) {
        return (int)(tid & REMAINDER_MASK);
    }

    static long getRemainderAsBit(long tid) {
        return 1<<(getRemainder(tid)-1);
    }
}

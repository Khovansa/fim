package org.openu.fimcmp.apriori;

import org.openu.fimcmp.util.BitArrays;
import scala.Tuple2;

import java.util.Iterator;

/**
 * Iterate over long[] representing the following: <br/>
 * long[0] = TID <br/>
 * long[1..end) = bitset of integer ranks
 */
public class IteratorOverTidAndRanksBitset implements Iterator<Tuple2<Integer, Long>> {
    static final int START_IND = 1; //one place for TID
    private static final int BITS_PER_WORD = 64;
    private static final int NO_NEXT_NUM_IND = -2;
    private static final int HEAD_NUM_IND = 0; //the first index
    private static final int NUMS_END_VALUE = -1;

    private final long[] words;
    private final int[] nums;
    private int nextWordInd;
    private int nextNumInd;

    public IteratorOverTidAndRanksBitset(long[] tidAndRanksBitset) {
        this.words = tidAndRanksBitset;
        this.nums = new int[BITS_PER_WORD + 1];
        this.nextWordInd = START_IND - 1; //just before the start
        moveWordIndexToNextNonZeroWord();
    }

    @Override
    public boolean hasNext() {
        return hasNextNum() || nextWordInd < words.length;
    }

    @Override
    public Tuple2<Integer, Long> next() {
        return new Tuple2<>(nextRank(), words[0]);
    }

    private int nextRank() {
        //compute the result:
        if (!hasNextNum()) { //need to go to the next word
            recomputeNums(); //convert the next word to numbers
        }
        int res = nums[nextNumInd++];

        if (atNumsEnd()) { //no next value in 'nums'
            moveWordIndexToNextNonZeroWord(); //move 'nextWordInd' to the next non-zero word
        }

        return res;
    }

    private boolean hasNextNum() {
        return nextNumInd != NO_NEXT_NUM_IND && !atNumsEnd();
    }

    private void recomputeNums() {
        int base = BitArrays.base(nextWordInd, START_IND);
        int endNumInd = BitArrays.getWordBitsAsNumbers(nums, 0, base, words[nextWordInd]);
        nums[endNumInd] = NUMS_END_VALUE;
        nextNumInd = HEAD_NUM_IND;
    }

    private boolean atNumsEnd() {
        return nums[nextNumInd] == NUMS_END_VALUE;
    }

    //move 'nextWordInd' to point to the next non-zero word
    private void moveWordIndexToNextNonZeroWord() {
        nextNumInd = NO_NEXT_NUM_IND;

        ++nextWordInd;
        while (nextWordInd < words.length && words[nextWordInd] == 0) {
            ++nextWordInd;
        }
    }
}

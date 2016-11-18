package org.openu.fimcmp.apriori;

import org.openu.fimcmp.util.BitArrays;
import scala.Tuple2;

import java.util.Iterator;

/**
 * Iterate over long[] representing the following: <br/>
 * long[0] = TID <br/>
 * long[1..end) = bitset of integer ranks
 */
public class IteratorOverTidAndRanksBitset implements Iterator<Tuple2<Long, Integer>> {
    private static final int START_IND = 1;
    private final long[] tidAndRanksBitset;
    private int wordInd;
    private int[] nums = new int[64];
    private int numInd = -2;

    public IteratorOverTidAndRanksBitset(long[] tidAndRanksBitset) {
        this.tidAndRanksBitset = tidAndRanksBitset;
        moveWordIndexBeforeNextNonZeroWord();
    }

    @Override
    public boolean hasNext() {
        return numInd >= -1 || wordInd + 1 < tidAndRanksBitset.length;
    }

    @Override
    public Tuple2<Long, Integer> next() {
        return new Tuple2<>(tidAndRanksBitset[0], nextRank());
    }

    private int nextRank() {
        //compute the result:
        if (numInd < -1) { //the current word is not converted to numbers
            //convert the next word to numbers
            ++wordInd;
            int base = BitArrays.base(wordInd, START_IND);
            int endNumInd = BitArrays.getWordBitsAsNumbers(nums, 0, base, tidAndRanksBitset[wordInd]);
            if (endNumInd < nums.length) {
                nums[endNumInd] = 0;
            }
            numInd = -1;
        }
        int res = nums[++numInd];

        //move just before the next value:
        if (numInd + 1 >= nums.length || nums[numInd]==0) {
            moveWordIndexBeforeNextNonZeroWord();
        }

        return res;
    }

    private void moveWordIndexBeforeNextNonZeroWord() {
        int currWordInd = START_IND;
        while (currWordInd<tidAndRanksBitset.length && tidAndRanksBitset[currWordInd] == 0) {
            ++currWordInd;
        }
        wordInd = currWordInd-1;
        numInd = -2;
    }
}

package org.openu.fimcmp.util;

import org.apache.commons.lang.ArrayUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class BitArraysTest {
    private static final int START_IND = 3;

    @Test
    public void test() {
        int[] indexes = {0, 1, 7, 63, 64, 65, 128, 192, 200, 213, 214, 1034, 1234};
        long[] words = new long[BitArrays.requiredSize(1 + indexes[indexes.length - 1], START_IND)];
        for (int bitIndex : indexes) {
            BitArrays.set(words, START_IND, bitIndex);
        }

        for (int bitIndex : indexes) {
            assertTrue("" + bitIndex, BitArrays.get(words, START_IND, bitIndex));
        }
        assertFalse(BitArrays.get(words, START_IND, 2));
        assertFalse(BitArrays.get(words, START_IND, 8));
        assertFalse(BitArrays.get(words, START_IND, 100));

        assertEquals(indexes.length, BitArrays.cardinality(words, START_IND));


        List<Integer> nums = asList(BitArrays.asNumbers(words, START_IND));
        assertThat(nums, is(asList(indexes)));

        assertThat(BitArrays.min(words, START_IND), is(0));
        assertThat(BitArrays.max(words, START_IND), is(1234));

        checkWordBitsToNums(words, START_IND, asList(indexes).subList(0, 4));
        checkWordBitsToNums(words, START_IND + 1, asList(indexes).subList(4, 6));

        assertThat(BitArrays.roundedToWordStart(0), is(0));
        assertThat(BitArrays.roundedToWordStart(1), is(0));
        assertThat(BitArrays.roundedToWordStart(63), is(0));
        assertThat(BitArrays.roundedToWordStart(64), is(64));
        assertThat(BitArrays.roundedToWordStart(65), is(64));
        assertThat(BitArrays.roundedToWordStart(127), is(64));
        assertThat(BitArrays.roundedToWordStart(128), is(128));
    }

    @Test
    public void not_should_keep_unused_bits_untouched() {
        final int[] indexes = {0, 1, 7, 63, 64, 65, 128, 192, 200, 213, 214, 1034, 1234};
        final int lastNum = indexes[indexes.length - 1];
        long[] words = new long[BitArrays.requiredSize(1 + lastNum, START_IND)];
        for (int bitIndex : indexes) {
            BitArrays.set(words, START_IND, bitIndex);
        }

        int totalBits = BitArrays.totalBitsIn(words, START_IND);
        assertThat(totalBits, is((words.length - START_IND) * 64));

        long[] wordsNegatedTillLastNum = Arrays.copyOf(words, words.length);
        BitArrays.not(wordsNegatedTillLastNum, START_IND, lastNum);
        for (int ii = 0; ii < totalBits ; ++ii) {
            boolean isOrigNumber = Arrays.binarySearch(indexes, ii) >= 0;
            boolean shouldBeSet = (isOrigNumber == (ii > lastNum));
            assertThat("At number " + ii, BitArrays.get(wordsNegatedTillLastNum, START_IND, ii), is(shouldBeSet));
        }

        long[] wordsNegatedTill128 = Arrays.copyOf(words, words.length);
        BitArrays.not(wordsNegatedTill128, START_IND, 128);
        for (int ii = 0; ii < totalBits ; ++ii) {
            boolean isOrigNumber = Arrays.binarySearch(indexes, ii) >= 0;
            boolean shouldBeSet = (isOrigNumber == (ii > 128));
            assertThat("At number " + ii, BitArrays.get(wordsNegatedTill128, START_IND, ii), is(shouldBeSet));
        }

        long[] wordsNegatedTillLastMinus1 = Arrays.copyOf(words, words.length);
        BitArrays.not(wordsNegatedTillLastMinus1, START_IND, lastNum - 1);
        for (int ii = 0; ii < totalBits ; ++ii) {
            boolean isOrigNumber = Arrays.binarySearch(indexes, ii) >= 0;
            boolean shouldBeSet = (isOrigNumber == (ii > lastNum - 1));
            assertThat("At number " + ii, BitArrays.get(wordsNegatedTillLastMinus1, START_IND, ii), is(shouldBeSet));
        }

        long[] wordsNegatedTillLastPlus1 = Arrays.copyOf(words, words.length);
        BitArrays.not(wordsNegatedTillLastPlus1, START_IND, lastNum + 1);
        for (int ii = 0; ii < totalBits ; ++ii) {
            boolean isOrigNumber = Arrays.binarySearch(indexes, ii) >= 0;
            boolean shouldBeSet = (isOrigNumber == (ii > lastNum + 1));
            assertThat("At number " + ii, BitArrays.get(wordsNegatedTillLastPlus1, START_IND, ii), is(shouldBeSet));
        }
    }

    private void checkWordBitsToNums(long[] words, int wordInd, List<Integer> expRes) {
        int[] res = BitArrays.newBufForWordNumbers();
        int nextInd = BitArrays.getWordBitsAsNumbersToArr(res, words[wordInd], START_IND, wordInd);
        assertThat(nextInd, is(expRes.size()));

        List<Integer> actRes = asList(res).subList(0, nextInd);
        assertThat(actRes, is(expRes));
    }

    @Test
    public void tmp() {
        long num = 345123456789123L;
        long right = (num & 0xFFFFFFFFL);
        int rightInt = (int) right;
        long left = (num >>> 32);
        int leftInt = (int) left;
        long restoredNum = (((long) leftInt) << 32) + rightInt;
        System.out.println(String.format("%64s", Long.toBinaryString(num)));
        System.out.println(String.format("%64s", Long.toBinaryString(right)));
        System.out.println(String.format("%64s", Long.toBinaryString(rightInt)));
        System.out.println(String.format("%32s", Long.toBinaryString(left)));
        System.out.println(String.format("%32s", Long.toBinaryString(leftInt)));
        System.out.println(String.format("%64s", Long.toBinaryString(restoredNum)));
        assertThat(restoredNum, is(num));
    }

    private static List<Integer> asList(int... nums) {
        return Arrays.asList(ArrayUtils.toObject(nums));
    }

    @Test
    public void tmp2() {
        System.out.println(1L<<10);
    }
}
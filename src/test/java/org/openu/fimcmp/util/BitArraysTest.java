package org.openu.fimcmp.util;

import static org.hamcrest.CoreMatchers.*;

import org.apache.commons.lang.ArrayUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class BitArraysTest {
    private static final int START_IND=3;

    @Test
    public void test() {
        int[] indexes = {0, 1, 7, 63, 64, 65, 128, 192, 200, 213, 214, 1034, 1234};
        long[] words = new long[BitArrays.requiredSize(indexes[indexes.length-1], START_IND)];
        for (int bitIndex : indexes) {
            BitArrays.set(words, START_IND, bitIndex);
        }

        for (int bitIndex : indexes) {
            assertTrue(""+bitIndex, BitArrays.get(words, START_IND, bitIndex));
        }
        assertFalse(BitArrays.get(words, START_IND, 2));
        assertFalse(BitArrays.get(words, START_IND, 8));
        assertFalse(BitArrays.get(words, START_IND, 100));

        assertEquals(indexes.length, BitArrays.cardinality(words, START_IND));


        List<Integer> nums = asList(BitArrays.asNumbers(words, START_IND));
        assertThat(nums, is(asList(indexes)));
    }

    private static List<Integer> asList(int[] nums) {
        return Arrays.asList(ArrayUtils.toObject(nums));
    }
}
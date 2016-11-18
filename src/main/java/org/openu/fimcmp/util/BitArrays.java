package org.openu.fimcmp.util;

/**
 * Common bit-manipulation methods on long[]. <br/>
 * Designed to be as fast as possible, assumes all arguments are correct. <br/>
 */
public class BitArrays {
    private final static int ADDRESS_BITS_PER_WORD = 6;
    private final static int BITS_PER_WORD = (1<<ADDRESS_BITS_PER_WORD);

    public static void set(long[] words, int bitSetStartInd, int bitIndex) {
        int wordIndex = wordIndex(bitSetStartInd, bitIndex);
        words[wordIndex] |= asBit(bitIndex);
    }

    public static boolean get(long[] words, int bitSetStartInd, int bitIndex) {
        int wordIndex = wordIndex(bitSetStartInd, bitIndex);
        return ((words[wordIndex] & (1L << bitIndex)) != 0);

    }

    public static int cardinality(long[] words, int bitSetStartInd) {
        int sum = 0;
        for (int i = bitSetStartInd; i < words.length; i++) {
            sum += Long.bitCount(words[i]);
        }
        return sum;
    }

    public static int[] asNumbers(long[] words, int bitSetStartInd) {
        int[] res = new int[cardinality(words, bitSetStartInd)];
        int resInd = 0;
        for (int wordIndex = bitSetStartInd; wordIndex < words.length; ++wordIndex) {
            long word = words[wordIndex];
            if (word != 0) {
                int base = (wordIndex - bitSetStartInd) * BITS_PER_WORD;
                resInd = getWordBitsAsNumbers(res, resInd, base, word);
            }
        }
        return res;
    }

    public static int getWordBitsAsNumbers(int[] res, int resInd, int base, long word) {
        long currWord = word;
        while (currWord != 0) {
            int bitIndex = Long.numberOfTrailingZeros(currWord);
            res[resInd++] = base + bitIndex;
            currWord = currWord & ~(asBit(bitIndex));
        }
        return resInd;
    }

    public static int base(int wordInd, int bitSetStartInd) {
        return BITS_PER_WORD * (wordInd - bitSetStartInd);
    }

    private static int wordIndex(int bitSetStartInd, int bitIndex) {
        return bitSetStartInd + (bitIndex >> ADDRESS_BITS_PER_WORD);
    }

    private static long asBit(int bitIndex) {
        return 1L << bitIndex;
    }
}

package org.openu.fimcmp.util;

import java.util.Arrays;

/**
 * Common bit-manipulation methods on long[]. <br/>
 * Designed to be as fast as possible, assumes all arguments are correct. <br/>
 */
public class BitArrays {
    private final static int ADDRESS_BITS_PER_WORD = 6;
    private final static int BITS_PER_WORD = (1<<ADDRESS_BITS_PER_WORD);

    @SuppressWarnings("unused")
    public static void setAll(long[] words, int bitSetStartInd, int[] bitIndexes) {
        for (int bitIndex : bitIndexes) {
            set(words, bitSetStartInd, bitIndex);
        }
    }

    public static void set(long[] words, int bitSetStartInd, int bitIndex) {
        int wordIndex = wordIndex(bitIndex, bitSetStartInd);
        words[wordIndex] |= asBit(bitIndex);
    }

    public static boolean get(long[] words, int bitSetStartInd, int bitIndex) {
        int wordIndex = wordIndex(bitIndex, bitSetStartInd);
        return ((words[wordIndex] & (1L << bitIndex)) != 0);
    }

    public static int cardinality(long[] words, int searchFromInd, int searchToIndExc) {
        int actToIndExc = Math.min(searchToIndExc, words.length);
        int sum = 0;
        for (int i = searchFromInd; i < actToIndExc; i++) {
            sum += Long.bitCount(words[i]);
        }
        return sum;
    }

    public static int cardinality(long[] words, int bitSetStartInd) {
        return cardinality(words, bitSetStartInd, words.length);
    }

    public static int min(long[] words, int startInd) {
        return min(words, startInd, startInd, words.length);
    }

    public static int min(long[] words, int bitSetStartInd, int searchFromInd, int searchToIndExc) {
        int actToIndExc = Math.min(searchToIndExc, words.length);
        for (int wordInd=searchFromInd; wordInd<actToIndExc; ++wordInd) {
            long word = words[wordInd];
            if (word != 0) {
                int base = (wordInd - bitSetStartInd) * BITS_PER_WORD;
                return base + Long.numberOfTrailingZeros(word);
            }
        }
        return -1;

    }

    public static int max(long[] words, int bitSetStartInd, int searchFromInd, int searchToIndExc) {
        int actToIndExc = Math.min(searchToIndExc, words.length);
        for (int wordInd=actToIndExc-1; wordInd>=searchFromInd; --wordInd) {
            long word = words[wordInd];
            if (word != 0) {
                int base = (wordInd - bitSetStartInd) * BITS_PER_WORD;
                return base + BITS_PER_WORD - 1 - Long.numberOfLeadingZeros(word);
            }
        }
        return -1;
    }

    public static int max(long[] words, int startInd) {
        return max(words, startInd, startInd, words.length);
    }

    public static boolean isZerosOnly(long[] words, int startInd) {
        for (int wordInd=startInd; wordInd<words.length; ++wordInd) {
            long word = words[wordInd];
            if (word != 0) {
                return false;
            }
        }
        return true;
    }

    public static long[] andReturn(long[] words1, long[] words2, int startInd, int endInd) {
        long[] words1AndRes = Arrays.copyOf(words1, words1.length);
        and(words1AndRes, words2, startInd, endInd);
        return words1AndRes;
    }

    public static void and(long[] words1AndRes, long[] words2, int startInd, int endInd) {
        int actEndInd = Math.min(words1AndRes.length, words2.length);
        int andEndInd = Math.min(actEndInd, endInd);
        for (int ii = startInd; ii<andEndInd; ++ii) {
            words1AndRes[ii] &= words2[ii];
        }

        for (int ii=andEndInd+1; ii<actEndInd; ++ii) {
            words1AndRes[ii] = 0;
        }
    }

    public static void notXor(long[] words1AndRes, int startInd1, long[] words2, int startInd2) {
        int actLen = Math.min(words1AndRes.length-startInd1, words2.length-startInd2);

        for (int ii = 0; ii<actLen; ++ii) {
            words1AndRes[startInd1 + ii] = ~(words1AndRes[startInd1 + ii] ^ words2[startInd2 + ii]);
        }

        for (int ii=startInd1 + actLen; ii<words1AndRes.length; ++ii) {
            words1AndRes[ii] = ~(words1AndRes[ii]);
        }
    }

    /**
     * Negate all the bits of the bit set from 0 to maxBit (including). <br/>
     * All the bits above the 'maxBit' will remain untouched. <br/>
     */
    public static void not(long[] wordsAndRes, int startInd, int maxBit) {
        final int maxWord = Math.min(wordIndex(maxBit, startInd), wordsAndRes.length - 1);
        for (int ii = startInd; ii < maxWord; ++ii) {
            wordsAndRes[ii] = ~(wordsAndRes[ii]);
        }

        //Only negate the bits up to and including 'maxBit'
        //Note that the 'tail' sits on the left from the bits to be negated
        if (startInd <= maxWord) {
            int tailSize = (maxWord + 1 - startInd) * BITS_PER_WORD - maxBit - 1;
            int bitsToNegate = BITS_PER_WORD - tailSize;
            long negatedBitsOnly = (~(wordsAndRes[maxWord])) << tailSize >>> tailSize;
            long tailBitsOnly = (wordsAndRes[maxWord] >>> bitsToNegate) << bitsToNegate;
            wordsAndRes[maxWord] = negatedBitsOnly | tailBitsOnly;
        }
    }

    public static void or(long[] words1AndRes, long[] words2, int startInd, int endInd) {
        int actEndInd = Math.min(words1AndRes.length, words2.length);
        int orEndInd = Math.min(actEndInd, endInd);
        for (int ii = startInd; ii<orEndInd; ++ii) {
            words1AndRes[ii] |= words2[ii];
        }
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

    public static int getWordBitsAsNumbersToArr(int[] res, long word, int startInd, int wordInd) {
        int base = (wordInd - startInd) * BITS_PER_WORD;
        return getWordBitsAsNumbers(res, 0, base, word);
    }

    private static int getWordBitsAsNumbers(int[] res, int resInd, int base, long word) {
        long currWord = word;
        while (currWord != 0) {
            int bitIndex = Long.numberOfTrailingZeros(currWord);
            res[resInd++] = base + bitIndex;
            currWord = currWord & ~(asBit(bitIndex));
        }
        return resInd;
    }

    /**
     * @return a buffer that can hold all the numbers contained in a one-word bitset
     */
    public static int[] newBufForWordNumbers() {
        return new int[BITS_PER_WORD];
    }

    public static int base(int wordInd, int bitSetStartInd) {
        return BITS_PER_WORD * (wordInd - bitSetStartInd);
    }

    public static int requiredSize(int totalBits, int bitSetStartInd) {
        int maxBitIndex = totalBits - 1;
        return 1 + wordIndex(maxBitIndex, bitSetStartInd);
    }

    public static int totalBitsIn(long[] words, int bitSetStartInd) {
        return Math.max(0, (words.length - bitSetStartInd)) * BITS_PER_WORD;
    }

    public static int wordIndex(int bitIndex, int bitSetStartInd) {
        return bitSetStartInd + (bitIndex >> ADDRESS_BITS_PER_WORD);
    }

    private static long asBit(int bitIndex) {
        return 1L << bitIndex;
    }
}

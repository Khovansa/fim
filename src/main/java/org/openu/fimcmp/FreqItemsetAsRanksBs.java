package org.openu.fimcmp;

import org.openu.fimcmp.util.BitArrays;

import java.util.ArrayList;
import java.util.List;

/**
 * Representation of frequent itemset as bit set of ranks + support
 */
public class FreqItemsetAsRanksBs {
    private static final int BITSET_START_IND = 1;

    public static long[] toBitSet(ItemsetAndTids iat, int totalFreqItems) {
        return toBitSet(iat.getSupportCount(), iat.getItemset(), totalFreqItems);
    }

    public static long[] toBitSet(int supportCnt, int[] itemset, int totalFreqItems) {
        long[] res = new long[BitArrays.requiredSize(totalFreqItems, BITSET_START_IND)];
        res[0] = supportCnt;
        for (int item : itemset) {
            BitArrays.set(res, BITSET_START_IND, item);
        }
        return res;
    }

    public static List<FreqItemset> toFreqItemsets(List<long[]> bsList, String[] rankToItem) {
        List<FreqItemset> res = new ArrayList<>(bsList.size());
        for (long[] bs : bsList) {
            int[] ranks = extractItemset(bs);
            int freq = extractSupportCnt(bs);
            res.add(FreqItemset.constructFromRanks(ranks, freq, rankToItem));
        }
        return res;
    }

    public static int extractSupportCnt(long[] bs) {
        return (int)bs[0];
    }

    public static int[] extractItemset(long[] bs) {
        return BitArrays.asNumbers(bs, BITSET_START_IND);
    }
}

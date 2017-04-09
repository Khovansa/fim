package org.openu.fimcmp;

import org.openu.fimcmp.util.BitArrays;
import org.openu.fimcmp.util.SubsetsGenerator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Representation of frequent itemset as bit set of ranks + support
 */
public class FreqItemsetAsRanksBs {
    private static final int BITSET_START_IND = 1;

    /**
     * Create multiple frequent itemset instances of the form {basicItemset, subset of 'newItems'}. <br/>
     */
    public static List<long[]> toBitSets(
            int supportCnt, int[] basicItemset, ArrayList<Integer> newItems, int totalFreqItems) {
        final int setSize = newItems.size();
        final int totalSubsets = (int) SubsetsGenerator.getNumberOfAllSubsets(setSize);
        List<int[]> allSubsetsOfIndices = new ArrayList<>(totalSubsets);
        SubsetsGenerator.generateAllSubsets(allSubsetsOfIndices, setSize);

        List<long[]> result = new ArrayList<>(totalSubsets);
        long[] basicBs = toBitSet(supportCnt, basicItemset, totalFreqItems);
        for (int[] indicesSubset : allSubsetsOfIndices) {
            long[] newBs = Arrays.copyOf(basicBs, basicBs.length);
            updateByNewItems(newBs, newItems, indicesSubset);
            result.add(newBs);
        }

        return result;
    }

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

    private static void updateByNewItems(long[] res, ArrayList<Integer> newItems, int[] indicesSubset) {
        for (int itemInd : indicesSubset) {
            int item = newItems.get(itemInd);
            BitArrays.set(res, BITSET_START_IND, item);
        }
    }
}

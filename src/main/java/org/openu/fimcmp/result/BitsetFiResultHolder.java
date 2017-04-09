package org.openu.fimcmp.result;

import org.openu.fimcmp.FreqItemset;
import org.openu.fimcmp.FreqItemsetAsRanksBs;
import org.openu.fimcmp.util.BitArrays;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Holds the frequent itemsets and their support count as a bitset. <br/>
 *
 * @see org.openu.fimcmp.FreqItemsetAsRanksBs
 */
public class BitsetFiResultHolder implements FiResultHolder {
    private final int totalFreqItems;
    private final List<long[]> freqItemsetBitsets;

    public static BitsetFiResultHolder emptyHolder() {
        return new BitsetFiResultHolder(0, 1);
    }

    public BitsetFiResultHolder(int totalFreqItems, int resultEstimation) {
        this.totalFreqItems = totalFreqItems;
        this.freqItemsetBitsets = new ArrayList<>(resultEstimation);
    }

    @Override
    public void addClosedItemset(
            int supportCnt, int[] basicItemset, List<Integer> parentEquivItems, List<Integer> equivItems) {
        if (parentEquivItems.isEmpty() && equivItems.isEmpty()) {
            //fast treatment of the most frequent case:
            addFrequentItemset(supportCnt, basicItemset);
        } else {
            //the general case
            ArrayList<Integer> newItems = getUniqueNewItems(basicItemset, parentEquivItems, equivItems);
            freqItemsetBitsets.addAll(
                    FreqItemsetAsRanksBs.toBitSets(supportCnt, basicItemset, newItems, totalFreqItems));
        }
    }

    @Override
    public void addFrequentItemset(int supportCnt, int[] itemset) {
        freqItemsetBitsets.add(FreqItemsetAsRanksBs.toBitSet(supportCnt, itemset, totalFreqItems));
    }

    @Override
    public int size() {
        return freqItemsetBitsets.size();
    }

    @Override
    public List<FreqItemset> getAllFrequentItemsets(String[] rankToItem) {
        List<FreqItemset> res = new ArrayList<>(freqItemsetBitsets.size());
        for (long[] fiAsBitset : freqItemsetBitsets) {
            int[] ranks = FreqItemsetAsRanksBs.extractItemset(fiAsBitset);
            int supp = FreqItemsetAsRanksBs.extractSupportCnt(fiAsBitset);
            res.add(FreqItemset.constructFromRanks(ranks, supp, rankToItem));
        }
        return res;
    }

    @Override
    public Iterator<long[]> fiAsBitsetIterator() {
        return freqItemsetBitsets.iterator();
    }

    private ArrayList<Integer> getUniqueNewItems(
            int[] basicItemset, List<Integer> parentEquivItems, List<Integer> equivItems) {
        ArrayList<Integer> res = new ArrayList<>(parentEquivItems.size() + equivItems.size());

        final int bsStartInd = 0;
        final int bsArrSize = BitArrays.requiredSize(totalFreqItems, bsStartInd);
        long[] itemsBs = new long[bsArrSize];

        BitArrays.setAll(itemsBs, bsStartInd, basicItemset);
        addIfNew(res, itemsBs, bsStartInd, parentEquivItems);
        addIfNew(res, itemsBs, bsStartInd, equivItems);

        return res;
    }

    private static void addIfNew(List<Integer> res, long[] itemsBs, int bsStartInd, List<Integer> possiblyNewItems) {
        for (Integer item : possiblyNewItems) {
            if (!BitArrays.get(itemsBs, bsStartInd, item)) {
                BitArrays.set(itemsBs, bsStartInd, item);
                res.add(item);
            }
        }
    }
}

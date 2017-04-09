package org.openu.fimcmp.result;

import org.openu.fimcmp.FreqItemset;
import org.openu.fimcmp.FreqItemsetAsRanksBs;
import org.openu.fimcmp.util.BitArrays;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Holds the frequent itemsets and their support count as a bitset. <br/>
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

    public void addClosedItemset(
            int supportCnt, int[] basicItemset, List<Integer> parentEquivItems, List<Integer> equivItems) {
        //fast treatment of the most frequent case:
        addFrequentItemset(supportCnt, basicItemset);
        if (parentEquivItems.isEmpty() && equivItems.isEmpty()) {
            return;
        }

        final int bsStartInd=0;
        final int bsArrSize = BitArrays.requiredSize(totalFreqItems, bsStartInd);
        long[] itemsBs = new long[bsArrSize];
        BitArrays.setAll(itemsBs, bsStartInd, basicItemset);

        //TODO - implement

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
}

package org.openu.fimcmp.result;

import org.openu.fimcmp.FreqItemset;
import org.openu.fimcmp.util.BitArrays;

import java.util.Iterator;
import java.util.List;

/**
 * 'Fake' collection of frequent itemsets that only counts the result,
 * but not actually holds them. <br/>
 */
public class CountingOnlyFiResultHolder implements FiResultHolder {
    private static final String UNSUPPORTED_ERR_MSG =
            "this is a counting-only result holder that does not actually keep the results";
    private final int totalFreqItems;
    private int size;

    public CountingOnlyFiResultHolder(int totalFreqItems) {
        this.totalFreqItems = totalFreqItems;
        this.size = 0;
    }

    public void addClosedItemset(
            int supportCnt, int[] basicItemset, List<Integer> parentEquivItems, List<Integer> equivItems) {
        //fast treatment of the most frequent case:
        if (parentEquivItems.isEmpty() && equivItems.isEmpty()) {
            size += 1;
        } else {
            int differentEqivItemsCnt = countNewItems(basicItemset, parentEquivItems, equivItems);
            size += (1L << differentEqivItemsCnt);
        }
    }

    @Override
    public void addFrequentItemset(int supportCnt, int[] itemset) {
        ++size;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public List<FreqItemset> getAllFrequentItemsets(String[] rankToItem) {
        throw new UnsupportedOperationException(UNSUPPORTED_ERR_MSG);
    }

    @Override
    public Iterator<long[]> fiAsBitsetIterator() {
        throw new UnsupportedOperationException(UNSUPPORTED_ERR_MSG);
    }

    private int countNewItems(int[] basicItemset, List<Integer> parentEquivItems, List<Integer> equivItems) {
        final int bsStartInd=0;
        long[] itemsBs = new long[BitArrays.requiredSize(totalFreqItems, bsStartInd)];
        BitArrays.setAll(itemsBs, bsStartInd, basicItemset);

        return countAndSetNewItems(itemsBs, bsStartInd, parentEquivItems) +
                countAndSetNewItems(itemsBs, bsStartInd, equivItems);
    }

    private int countAndSetNewItems(long[] itemsBs, int bsStartInd, List<Integer> equivItems) {
        int resCnt = 0;
        for (Integer item : equivItems) {
            if (!BitArrays.get(itemsBs, bsStartInd, item)) {
                ++resCnt;
                BitArrays.set(itemsBs, bsStartInd, item);
            }
        }
        return resCnt;
    }
}

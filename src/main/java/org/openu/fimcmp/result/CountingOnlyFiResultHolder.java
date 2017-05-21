package org.openu.fimcmp.result;

import org.apache.commons.collections.CollectionUtils;
import org.openu.fimcmp.itemset.FreqItemset;
import org.openu.fimcmp.util.BitArrays;
import org.openu.fimcmp.util.SubsetsGenerator;

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
    private long size;

    public CountingOnlyFiResultHolder(int totalFreqItems) {
        this.totalFreqItems = totalFreqItems;
        this.size = 0;
    }

    @Override
    public void addClosedItemset(
            int supportCnt, int[] basicItemset, List<Integer> equivItems) {
        if (CollectionUtils.isEmpty(equivItems)) {
            //fast treatment of the most frequent case:
            size += 1;
        } else {
            //the general case
            int differentEqivItemsCnt = countNewItems(basicItemset, equivItems);
            size += SubsetsGenerator.getNumberOfAllSubsets(differentEqivItemsCnt);
        }
    }

    @Override
    public void addFrequentItemset(int supportCnt, int[] itemset) {
        ++size;
    }

    @Override
    public long size() {
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

    @Override
    public FiResultHolder uniteWith(FiResultHolder otherObj) {
        CountingOnlyFiResultHolder other = (CountingOnlyFiResultHolder)otherObj;
        size += other.size;
        return this;
    }

    private int countNewItems(int[] basicItemset, List<Integer> equivItems) {
        final int bsStartInd=0;
        long[] itemsBs = new long[BitArrays.requiredSize(totalFreqItems, bsStartInd)];
        BitArrays.setAll(itemsBs, bsStartInd, basicItemset);

        return countAndSetNewItems(itemsBs, bsStartInd, equivItems);
    }

    private int countAndSetNewItems(long[] itemsBs, int bsStartInd, List<Integer> equivItems) {
        if (equivItems == null) {
            return 0;
        }

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

package org.openu.fimcmp.result;

import org.apache.commons.collections.CollectionUtils;
import org.openu.fimcmp.FreqItemset;
import org.openu.fimcmp.FreqItemsetAsRanksBs;

import java.util.*;

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
        this(totalFreqItems, new ArrayList<>(resultEstimation));
    }

    private BitsetFiResultHolder(int totalFreqItems, List<long[]> freqItemsetBitsets) {
        this.totalFreqItems = totalFreqItems;
        this.freqItemsetBitsets = freqItemsetBitsets;
    }

    @Override
    public void addClosedItemset(
            int supportCnt, int[] basicItemset, List<Integer> equivItems) {
//        System.out.println(String.format("Adding %s + %s", Arrays.toString(basicItemset), equivItems));
        if (CollectionUtils.isEmpty(equivItems)) {
            //fast treatment of the most frequent case:
            addFrequentItemset(supportCnt, basicItemset);
        } else {
            //the general case
            ArrayList<Integer> newItems = new ArrayList<>(equivItems);
            freqItemsetBitsets.addAll(FreqItemsetAsRanksBs.toBitSets(supportCnt, basicItemset, newItems, totalFreqItems));
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

    @Override
    public BitsetFiResultHolder uniteWith(FiResultHolder otherObj) {
        BitsetFiResultHolder other = (BitsetFiResultHolder)otherObj;
        if (other.freqItemsetBitsets.isEmpty()) {
            return this;
        }

        if (freqItemsetBitsets.isEmpty()) {
            //deep copy of 'other':
            List<long[]> newFiBitsets = new ArrayList<>(other.freqItemsetBitsets.size());
            for (long[] fiBitset : other.freqItemsetBitsets) {
                newFiBitsets.add(Arrays.copyOf(fiBitset, fiBitset.length));
            }
            return new BitsetFiResultHolder(totalFreqItems, newFiBitsets);
        }

        //need actual merge
        Map<FiBitsetWrapper, long[]> mergedFis =
                new LinkedHashMap<>((freqItemsetBitsets.size() + other.freqItemsetBitsets.size()) * 2);
        for (long[] fiBitset : freqItemsetBitsets) {
            mergedFis.put(new FiBitsetWrapper(fiBitset), fiBitset);
        }
        for (long[] otherBs : other.freqItemsetBitsets) {
            FiBitsetWrapper otherBsWrapper = new FiBitsetWrapper(otherBs);
            long[] thisBs = mergedFis.get(otherBsWrapper);
            if (thisBs != null) {
                FreqItemsetAsRanksBs.addSupportCntTo1st(thisBs, otherBs);
            } else {
                otherBs = Arrays.copyOf(otherBs, otherBs.length); //we might change it, so need to copy
                mergedFis.put(new FiBitsetWrapper(otherBs), otherBs);
            }
        }
        //returning 'this' since we have modified the support count in-place:
        freqItemsetBitsets.clear();
        freqItemsetBitsets.addAll(mergedFis.values());
        return this;
    }

    int getSupportForTest(int[] itemset) {
        for (long[] fiBitset : freqItemsetBitsets) {
            int[] currItemset = FreqItemsetAsRanksBs.extractItemset(fiBitset);
            if (Arrays.equals(itemset, currItemset)) {
                return FreqItemsetAsRanksBs.extractSupportCnt(fiBitset);
            }
        }
        return 0;
    }

    private static class FiBitsetWrapper {
        private final long[] fiBitset;

        FiBitsetWrapper(long[] fiBitset) {
            this.fiBitset = fiBitset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            FiBitsetWrapper that = (FiBitsetWrapper) o;
            return FreqItemsetAsRanksBs.haveSameItemset(fiBitset, that.fiBitset);
        }

        @Override
        public int hashCode() {
            return FreqItemsetAsRanksBs.itemsetHashCode(fiBitset);
        }
    }
}

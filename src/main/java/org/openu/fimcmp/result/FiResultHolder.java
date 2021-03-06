package org.openu.fimcmp.result;

import org.openu.fimcmp.itemset.FreqItemset;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 * Keeps the result frequent itemsets, i.e. the itemset and its support. <br/>
 * The in put itemset is represented as an array of frequent item ranks. <br/>
 */
public interface FiResultHolder extends Serializable {

    void addClosedItemset(int supportCnt, int[] basicItemset, List<Integer> equivItems);

    void addFrequentItemset(int supportCnt, int[] itemset);

    long size();

    List<FreqItemset> getAllFrequentItemsets(String[] rankToItem);

    Iterator<long[]> fiAsBitsetIterator();

    FiResultHolder uniteWith(FiResultHolder other);
}

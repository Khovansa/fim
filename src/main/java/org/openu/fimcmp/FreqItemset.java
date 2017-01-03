package org.openu.fimcmp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

/**
 * A simple structure to hold the final result for a single frequent itemset
 */
public class FreqItemset<T> {
    public final List<T> itemset;
    public final int freq;

    public FreqItemset(List<T> itemset, int freq) {
        this.itemset = itemset;
        this.freq = freq;
    }

    @Override
    public String toString() {
        return String.format("FI: %s: %s", new TreeSet<>(itemset), freq);
    }

    public static FreqItemset<String> constructStr(int[] itemset, int freq, String[] rankToItem) {
        List<String> resItemset = new ArrayList<>(itemset.length);
        for (int elem : itemset) {
            //TODO
//            resItemset.add(rankToItem[elem]);
            resItemset.add("" + elem);
        }
        return new FreqItemset<>(resItemset, freq);
    }
}

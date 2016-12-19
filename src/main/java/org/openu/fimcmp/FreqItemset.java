package org.openu.fimcmp;

import java.util.List;

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
        return String.format("%s: %s", itemset, freq);
    }
}

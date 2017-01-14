package org.openu.fimcmp;

import java.util.*;

/**
 * A simple structure to hold the final result for a single frequent itemset
 */
public class FreqItemset {
    public final List<String> itemset;
    public final int freq;

    public FreqItemset(List<String> itemset, int freq) {
        this.itemset = itemset;
        this.freq = freq;
    }

    public static FreqItemset constructFromRanks(int[] ranks, int freq, String[] rankToItem) {
        List<String> itemset = new ArrayList<>(ranks.length);
        for (int rank : ranks) {
            itemset.add(rankToItem[rank]);
        }
        return new FreqItemset(itemset, freq);
    }

    public boolean containsItems(String... items) {
        return itemset.containsAll(Arrays.asList(items));
    }

    @Override
    public String toString() {
        return String.format("FI: %s: %s", new TreeSet<>(itemset), freq);
    }

    public String toString(Map<String, Integer> itemToRank, int maxItemsetSize) {
        SortedSet<String> sortedItems = new TreeSet<>(itemset);
        List<Integer> ranks = new ArrayList<>(itemset.size());
        for (String item : sortedItems) {
            ranks.add(itemToRank.get(item));
        }
        SortedSet<Integer> sortedRanks = new TreeSet<>(ranks);

        int strSize1 = (int)(maxItemsetSize * 4.5 + 2); //2.5 chars on item in average
        int strSize2 = (int)(maxItemsetSize * 3.5 + 2); //1.5 chars on rank in average
        String formatStr = "%-<SIZE1>s %-<SIZE2>s %-<SIZE2>s: %s"
                .replace("<SIZE1>", "" + strSize1)
                .replace("<SIZE2>", "" + strSize2);
        return String.format(formatStr, sortedItems, ranks, sortedRanks, freq);
    }
}

package org.openu.fimcmp;

import java.util.ArrayList;
import java.util.List;

/**
 * Simple structure to hold an itemset as an array frequent items' ranks and the itemset's TIDs as a bit set. <br/>
 */
public class ItemsetAndTids {
    private final int[] itemset;
    private final long[] tidBitSet;
    private final int supportCnt;

    public ItemsetAndTids(int[] itemset, long[] tidBitSet, int supportCnt) {
        this.itemset = itemset;
        this.tidBitSet = tidBitSet;
        this.supportCnt = supportCnt;
    }

    public int[] getItemset() {
        return itemset;
    }

    public List<String> toOrigItemsetForDebug(String[] r1ToItem) {
        List<String> res = new ArrayList<>(itemset.length);
        for (int r1 : itemset) {
            res.add(r1ToItem[r1]);
        }
        return res;
    }

    public long[] getTidBitSet() {
        return tidBitSet;
    }

    public int getSupportCnt() {
        return supportCnt;
    }
}

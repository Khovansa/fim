package org.openu.fimcmp;

import org.openu.fimcmp.util.BitArrays;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Simple structure to hold an itemset as an array frequent items' ranks and the itemset's TIDs as a bit set. <br/>
 */
public class ItemsetAndTids {
    private static final int TIDS_START_IND = 0;
    private final int[] itemset;
    private final long[] tidBitSet;
    private int supportCnt;

    public ItemsetAndTids(int[] itemset, long[] tidBitSet) {
        this(itemset, tidBitSet, -1); //supportCnt unknown yet
    }

    public ItemsetAndTids(int[] itemset, long[] tidBitSet, int supportCnt) {
        this.itemset = itemset;
        this.tidBitSet = tidBitSet;
        this.supportCnt = supportCnt;
    }

    public int[] getItemset() {
        return itemset;
    }

    public int getItem(int ind) {
        return itemset[ind];
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

    public static int getTidsStartInd() {
        return TIDS_START_IND;
    }

    public void setTid(int tid) {
        BitArrays.set(tidBitSet, TIDS_START_IND, tid);
    }

    /**
     * @return either the actual support count or -1 if it has not yet been computed
     */
    public int getSupportCntIfExists() {
        return supportCnt;
    }

    public ItemsetAndTids withNewTids(int newTotalTids) {
        int[] newItemset = Arrays.copyOf(itemset, itemset.length);
        long[] newTids = new long[BitArrays.requiredSize(newTotalTids, TIDS_START_IND)];
        return new ItemsetAndTids(newItemset, newTids);
    }
}

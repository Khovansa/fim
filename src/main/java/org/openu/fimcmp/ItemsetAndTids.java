package org.openu.fimcmp;

import org.openu.fimcmp.util.Assert;
import org.openu.fimcmp.util.BitArrays;

import java.io.Serializable;
import java.util.*;

/**
 * Simple structure to hold an itemset as an array frequent items' ranks and the itemset's TIDs as a bit set. <br/>
 */
public class ItemsetAndTids implements Serializable {
    private static final int TIDS_START_IND = 0;
    private final int[] itemset;
    private final long[] tidBitSet;
    private final int supportCnt;
    private final boolean isTidsDiffSet;

    public ItemsetAndTids(int[] itemset, long[] tidBitSet, int supportCnt) {
        this(itemset, tidBitSet, supportCnt, false);
    }

    private ItemsetAndTids(int[] itemset, long[] tidBitSet, int supportCnt, boolean isTidsDiffSet) {
        this.itemset = itemset;
        this.tidBitSet = tidBitSet;
        this.supportCnt = supportCnt;
        this.isTidsDiffSet = isTidsDiffSet;
    }

    public int[] getItemset() {
        return itemset;
    }

    public boolean startsWith(int[] pref) {
        if (itemset.length < pref.length) {
            return false;
        }
        for (int ii=0; ii<pref.length; ++ii) {
            if (itemset[ii] != pref[ii]) {
                return false;
            }
        }
        return true;
    }

    public int getItem(int ind) {
        return itemset[ind];
    }

    public int getLastItem() {
        return itemset[itemset.length - 1];
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

    public boolean hasTid(int tid) {
        return BitArrays.get(tidBitSet, TIDS_START_IND, tid);
    }

    public int getSupportCount() {
        return supportCnt;
    }

    public ItemsetAndTids withNewTids(int newTotalTids) {
        int[] newItemset = Arrays.copyOf(itemset, itemset.length);
        long[] newTids = new long[BitArrays.requiredSize(newTotalTids, TIDS_START_IND)];
        return new ItemsetAndTids(newItemset, newTids, supportCnt, isTidsDiffSet);
    }

    public boolean haveCommonPrefix(int prefSize, ItemsetAndTids is2) {
        //starting from the end since the difference is expected to be there:
        for (int ii = prefSize - 1; ii >= 0; --ii) {
            if (itemset[ii] != is2.itemset[ii]) {
                return false;
            }
        }
        return true;
    }

    /**
     * 'this' is (PX, d(PX)), 'is2' is (PY, d(PY)), returning (PXY, d(PXY)) <br/>
     * Use Eclat with diffsets: <br/>
     * - Initially, (isTidsDiffSet=false): d(PXY) = tids(PX) / tids(PY) <br/>
     * - Then, when isTidsDiffSet=true and tidBitSet means diff set: <br/>
     * ---- d(PXY) = d(PY) / d(PX) <br/>
     * ---- support(PXY) = support(PX) - |d(PXY)| <br/>
     * Need to compute the new itemset PXY, d(PXY), and support(PXY)
     */
    public ItemsetAndTids computeNewFromNextDiffsetWithSamePrefixOrNull(
            ItemsetAndTids is2, int totalTids, long minSuppCount, boolean isUseDiffSets) {
        final int isLen = itemset.length;
        Assert.isTrue(isLen == is2.itemset.length);
        Assert.isTrue(itemset[isLen-1] < is2.itemset[isLen-1]);
        Assert.isTrue(isTidsDiffSet == is2.isTidsDiffSet);

        long[] newDiffSet;
        int newSupportCnt;
        if (!isUseDiffSets) {
            newDiffSet = BitArrays.andReturn(tidBitSet, is2.tidBitSet, TIDS_START_IND, totalTids);
            newSupportCnt = computeSetCardinality(newDiffSet);
        } else if (!isTidsDiffSet) {
            long[] actNewTids = BitArrays.andReturn(tidBitSet, is2.tidBitSet, TIDS_START_IND, totalTids);
            //d(PXY) = tids(PX) / tids(PY)
            newDiffSet = BitArrays.diffReturn(tidBitSet, actNewTids, TIDS_START_IND, totalTids);
//            newDiffSet = actNewTids;
            newSupportCnt = computeSetCardinality(actNewTids);
        } else {
            //d(PXY) = d(PY) / d(PX):

            try {
                newDiffSet = BitArrays.diffReturn(is2.tidBitSet, tidBitSet, TIDS_START_IND, totalTids);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
            //support(PXY) = support(PX) - |d(PXY)|:
            newSupportCnt = Math.max(0, supportCnt - computeSetCardinality(newDiffSet));
        }

        if (newSupportCnt < minSuppCount) {
            return null;
        }

        //new itemset = PXY
        int[] newItemset = new int[isLen +1];
        System.arraycopy(itemset, 0, newItemset, 0, isLen);
        newItemset[isLen] = is2.itemset[isLen-1];

        return new ItemsetAndTids(newItemset, newDiffSet, newSupportCnt, isUseDiffSets);
    }

    @Override
    public String toString() {
        return String.format("%s<%s>: %s", Arrays.toString(itemset), supportCnt,
                Arrays.toString(BitArrays.asNumbers(tidBitSet, TIDS_START_IND)));
    }

    public String toString(String[] rankToItem) {
        List<String> itemsetAsStr = new ArrayList<>(itemset.length);
        for (int item : itemset) {
            itemsetAsStr.add(rankToItem[item]);
        }
        return String.format("%-30s: %-25s<%s>: %s", new TreeSet<>(itemsetAsStr), Arrays.toString(itemset), supportCnt,
                Arrays.toString(BitArrays.asNumbers(tidBitSet, TIDS_START_IND)));
    }

    private static int computeSetCardinality(long[] tidBitSet) {
        return BitArrays.cardinality(tidBitSet, TIDS_START_IND);
    }

    private static void addAll(Set<Integer> res, int[] arrSet) {
        for (int elem : arrSet) {
            res.add(elem);
        }
    }

    private static int[] toArray(Set<Integer> input) {
        int[] res = new int[input.size()];
        int resInd = 0;
        for (Integer elem : input) {
            res[resInd++] = elem;
        }
        return res;
    }
}

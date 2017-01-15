package org.openu.fimcmp;

import org.openu.fimcmp.util.Assert;
import org.openu.fimcmp.util.BitArrays;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Collection of {@link ItemsetAndTids} objects.
 */
public class ItemsetAndTidsCollection implements Serializable {
    private static final double WORTH_SQUEEZING_RATIO = 0.8;
    private static final int MIN_LIST_SIZE_FOR_SQUEEZING = 4; //otherwise it's not worth the trouble
    private static final int TIDS_START_IND = ItemsetAndTids.getTidsStartInd();

    private final LinkedList<ItemsetAndTids> itemsetAndTidsList;
    private final int totalTids;
    private final int itemsetSize;

    public ItemsetAndTidsCollection(
            List<ItemsetAndTids> itemsetAndTidsList, int itemsetSize, int totalTids) {
        Assert.isTrue(itemsetAndTidsList != null);

        this.itemsetAndTidsList = new LinkedList<>(itemsetAndTidsList);
        this.totalTids = totalTids;
        this.itemsetSize = itemsetSize;
    }

    public ArrayList<ItemsetAndTids> getObjArrayListCopy() {
        return new ArrayList<>(itemsetAndTidsList);
    }

    public boolean isEmpty() {
        return itemsetAndTidsList.isEmpty();
    }

    public int size() {
        return itemsetAndTidsList.size();
    }

    public void sortByKm1Item() {
        final int sortItemInd = itemsetSize - 1;
        itemsetAndTidsList.sort((o1, o2) -> Integer.compare(o1.getItem(sortItemInd), o2.getItem(sortItemInd)));
    }

    public ItemsetAndTids popFirst() {
        Assert.isTrue(!isEmpty());
        Iterator<ItemsetAndTids> it = itemsetAndTidsList.iterator();
        ItemsetAndTids first = it.next();
        it.remove();
        return first;
    }

    public ItemsetAndTidsCollection joinWithHead(
            ItemsetAndTids head, List<long[]> totalResult,
            long minSuppCount, int totalFreqItems, boolean isUseDiffSets) {
        LinkedList<ItemsetAndTids> resList = new LinkedList<>();
        for (ItemsetAndTids is2 : itemsetAndTidsList) {
            ItemsetAndTids newIs =
                    head.computeNewFromNextDiffsetWithSamePrefixOrNull(is2, totalTids, minSuppCount, isUseDiffSets);
            if (newIs != null) {
                resList.add(newIs);
                totalResult.add(FreqItemsetAsRanksBs.toBitSet(newIs, totalFreqItems));
            }
        }

        return new ItemsetAndTidsCollection(resList, itemsetSize + 1, totalTids);
    }

    public int getTotalTids() {
        return totalTids;
    }

    public int getItemsetSize() {
        return itemsetSize;
    }

    /**
     * Try to shorten to TIDs bit sets by removing 0's that are the same for all itemsets. <br/>
     */
    public ItemsetAndTidsCollection squeezeIfNeeded(boolean isSqueezingEnabled) {
        if (!isSqueezingEnabled || itemsetAndTidsList.size() < MIN_LIST_SIZE_FOR_SQUEEZING) {
            return this;
        }

        long[] tidsToKeepBitSet = computeTidsPresentInAtLeastOneFi(itemsetAndTidsList, totalTids);
        final int resTotalTids = BitArrays.cardinality(tidsToKeepBitSet, TIDS_START_IND);
//        System.out.println(String.format("%-15s done squeeze check: %s (%s)", tt(sw), itemsetSize, totalTids));

        if (1.0 * resTotalTids / totalTids > WORTH_SQUEEZING_RATIO) {
            return this;
        }

//        System.out.println(String.format("%-15s start squeezing: ratio=%s", tt(sw), 1.0 * resTotalTids / totalTids));
        ArrayList<ItemsetAndTids> inTidsList = new ArrayList<>(itemsetAndTidsList);
        int[] tidsToKeep = BitArrays.asNumbers(tidsToKeepBitSet, TIDS_START_IND);
        ArrayList<ItemsetAndTids> resTidsList = initNewItemsetAndTidList(inTidsList, resTotalTids);
        int resTid = 0;
        for (int origTid : tidsToKeep) {
            setTidToEachMatchingItemset(resTidsList, resTid, inTidsList, origTid);
            ++resTid;
        }

//        System.out.println(String.format("%-15s done squeezing", tt(sw)));
        return new ItemsetAndTidsCollection(resTidsList, itemsetSize, resTotalTids);
    }

    @SuppressWarnings("unused")
    public String getFirstAsStringOrNull(String[] rankToItem) {
        if (!itemsetAndTidsList.isEmpty()) {
            return itemsetAndTidsList.iterator().next().toString(rankToItem);
        } else {
            return "";
        }
    }

    private static long[] computeTidsPresentInAtLeastOneFi(List<ItemsetAndTids> iatList, int totalTids) {
        long[] resBitSet = new long[BitArrays.requiredSize(totalTids, TIDS_START_IND)]; //all 0's
        for (ItemsetAndTids iat : iatList) {
            BitArrays.or(resBitSet, iat.getTidBitSet(), TIDS_START_IND, totalTids);
        }
        return resBitSet;
    }

    private static ArrayList<ItemsetAndTids> initNewItemsetAndTidList(
            ArrayList<ItemsetAndTids> inTidsList, int newTotalTids) {
        ArrayList<ItemsetAndTids> res = new ArrayList<>(inTidsList.size());
        for (ItemsetAndTids tids : inTidsList) {
            res.add(tids.withNewTids(newTotalTids));
        }
        return res;
    }

    private static void setTidToEachMatchingItemset(
            ArrayList<ItemsetAndTids> resIatList, int resTid, ArrayList<ItemsetAndTids> origIatList, int origTid) {
        for (int ii=0; ii<origIatList.size(); ++ii) {
            ItemsetAndTids origSet = origIatList.get(ii);
            if (origSet.hasTid(origTid)) {
                ItemsetAndTids resSet = resIatList.get(ii);
                resSet.setTid(resTid);
            }
        }
    }
}

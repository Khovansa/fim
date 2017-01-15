package org.openu.fimcmp;

import org.openu.fimcmp.util.Assert;
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

    public String getFirstAsStringOrNull(String[] rankToItem) {
        if (!itemsetAndTidsList.isEmpty()) {
            return itemsetAndTidsList.iterator().next().toString(rankToItem);
        } else {
            return "";
        }
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
                totalResult.add(newIs.getSupportCntAndItemsetBs(totalFreqItems));
            }
        }

        return new ItemsetAndTidsCollection(resList, itemsetSize + 1, totalTids);
    }

    public List<Tuple2<int[], Integer>> toResult() {
        List<Tuple2<int[], Integer>> res = new ArrayList<>(itemsetAndTidsList.size());
        for (ItemsetAndTids itemsetAndTids : itemsetAndTidsList) {
            int supportCnt = itemsetAndTids.getSupportCount();
            res.add(new Tuple2<>(itemsetAndTids.getItemset(), supportCnt));
        }
        return res;
    }

    public int getTotalTids() {
        return totalTids;
    }

    public int getItemsetSize() {
        return itemsetSize;
    }

    public boolean hasItemsetStartingWith(int[] debugPref1) {
        for (ItemsetAndTids iat : itemsetAndTidsList) {
            if (iat.startsWith(debugPref1)) {
                return true;
            }
        }
        return false;
    }
}

package org.openu.fimcmp;

import org.openu.fimcmp.util.Assert;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Collection of {@link ItemsetAndTids} objects.
 */
public class ItemsetAndTidsCollection {
    private final ArrayList<ItemsetAndTids> itemsetAndTidsList;
    private final int totalTids;
    private final int itemsetSize;

    public ItemsetAndTidsCollection(
            ArrayList<ItemsetAndTids> itemsetAndTidsList, int itemsetSize, int totalTids) {
        Assert.isTrue(itemsetAndTidsList != null);

        this.itemsetAndTidsList = itemsetAndTidsList;
        this.totalTids = totalTids;
        this.itemsetSize = itemsetSize;
    }

    public ArrayList<ItemsetAndTids> getItemsetAndTidsList() {
        return itemsetAndTidsList;
    }

    public boolean isEmpty() {
        return itemsetAndTidsList.isEmpty();
    }

    public int size() {
        return itemsetAndTidsList.size();
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

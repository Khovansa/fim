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
    private final int supportCorrection;

    public ItemsetAndTidsCollection(
            ArrayList<ItemsetAndTids> itemsetAndTidsList, int supportCorrection, int itemsetSize, int totalTids) {
        Assert.isTrue(itemsetAndTidsList != null);

        this.itemsetAndTidsList = itemsetAndTidsList;
        this.totalTids = totalTids;
        this.itemsetSize = itemsetSize;
        this.supportCorrection = supportCorrection;
    }

    public ArrayList<ItemsetAndTids> getItemsetAndTidsList() {
        return itemsetAndTidsList;
    }

    public boolean isEmpty() {
        return itemsetAndTidsList.isEmpty();
    }

    public List<Tuple2<int[], Integer>> toResult() {
        List<Tuple2<int[], Integer>> res = new ArrayList<>(itemsetAndTidsList.size());
        for (ItemsetAndTids itemsetAndTids : itemsetAndTidsList) {
            res.add(new Tuple2<>(itemsetAndTids.getItemset(), itemsetAndTids.getSupportCnt()));
        }
        return res;
    }

    public int getTotalTids() {
        return totalTids;
    }

    public int getItemsetSize() {
        return itemsetSize;
    }

    public int getSupportCorrection() {
        return supportCorrection;
    }
}

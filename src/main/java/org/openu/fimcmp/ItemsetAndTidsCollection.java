package org.openu.fimcmp;

import java.util.ArrayList;

/**
 * Collection of {@link ItemsetAndTids} objects.
 */
public class ItemsetAndTidsCollection {
    private final ArrayList<ItemsetAndTids> itemsetAndTidsList;
    private final int totalTids;
    private final int itemsetSize;
    private final int supportCorrection;

    public ItemsetAndTidsCollection(
            ArrayList<ItemsetAndTids> itemsetAndTidsList, int totalTids, int itemsetSize, int supportCorrection) {
        this.itemsetAndTidsList = itemsetAndTidsList;
        this.totalTids = totalTids;
        this.itemsetSize = itemsetSize;
        this.supportCorrection = supportCorrection;
    }

    public ArrayList<ItemsetAndTids> getItemsetAndTidsList() {
        return itemsetAndTidsList;
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

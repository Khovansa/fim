package org.openu.fimcmp.algs.eclat;

import java.io.Serializable;

/**
 * Holds all the properties for Eclat algorithm.
 */
public class EclatProperties implements Serializable {
    final long minSuppCount;
    final int totalFreqItems;
    boolean isUseDiffSets;
    boolean isSqueezingEnabled;
    boolean isCountingOnly;
    boolean isPrintIntermediateRes;
    @SuppressWarnings({"FieldCanBeLocal", "unused", "WeakerAccess"})
    String[] rankToItem;

    public EclatProperties(long minSuppCount, int totalFreqItems) {
        this.minSuppCount = minSuppCount;
        this.totalFreqItems = totalFreqItems;
    }

    public void setUseDiffSets(boolean useDiffSets) {
        isUseDiffSets = useDiffSets;
    }

    public void setSqueezingEnabled(boolean squeezingEnabled) {
        isSqueezingEnabled = squeezingEnabled;
    }

    public void setCountingOnly(boolean countingOnly) {
        isCountingOnly = countingOnly;
    }

    public void setRankToItem(String[] rankToItem) {
        this.rankToItem = rankToItem;
    }

    public void setPrintIntermediateRes(boolean printIntermediateRes) {
        isPrintIntermediateRes = printIntermediateRes;
    }
}

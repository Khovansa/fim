package org.openu.fimcmp.bigfim;

import java.io.Serializable;

/**
 * Holds all the properties for BigFim algorithm.
 */
public class BigFimProperties implements Serializable {
    public final double minSupp;
    public final int splitPrefixLen;

    //BigFim
    public int inputNumParts = 2;
    public double nextFiIncreasedSeriouslyRatio = 1.5;
    //Eclat
    public boolean isUseDiffSets;
    public boolean isSqueezingEnabled;
    public boolean isCountingOnly;
    public boolean isStatGatheringEnabled;
    public Integer maxEclatNumParts;

    public BigFimProperties(double minSupp, int splitPrefixLen) {
        this.minSupp = minSupp;
        this.splitPrefixLen = splitPrefixLen;

        this.isUseDiffSets = true;
        this.isCountingOnly = true;
        this.isStatGatheringEnabled = true;
        this.maxEclatNumParts = 1;
    }
}

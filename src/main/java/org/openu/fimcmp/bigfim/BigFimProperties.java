package org.openu.fimcmp.bigfim;

import java.io.Serializable;

/**
 * Holds all the properties for BigFim algorithm.
 */
public class BigFimProperties implements Serializable {
    public final double minSupp;
    public final int prefixLenToStartEclat;

    //BigFim
    public int inputNumParts = 2;
    public boolean isPersistInput = false;
    public boolean isPrintFks = true;
    public double currToPrevResSignificantIncreaseRatio = 2.0;
    //Eclat
    public boolean isUseDiffSets;
    public boolean isSqueezingEnabled;
    public boolean isCountingOnly;
    public boolean isStatGatheringEnabled;
    public Integer maxEclatNumParts;

    public BigFimProperties(double minSupp, int prefixLenToStartEclat) {
        this.minSupp = minSupp;
        this.prefixLenToStartEclat = prefixLenToStartEclat;

        this.isUseDiffSets = true;
        this.isCountingOnly = true;
        this.isStatGatheringEnabled = true;
        this.maxEclatNumParts = 1;
    }
}

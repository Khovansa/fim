package org.openu.fimcmp.bigfim;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.io.Serializable;

/**
 * Holds all the properties for BigFim algorithm.
 */
public class BigFimAlgProperties implements Serializable {
    /**
     * Min support as a ratio of itemset frequency to the total number of transactions
     */
    public final double minSupp;
    /**
     * Determines when to stop Apriori an switch to Eclat. <br/>
     * E.g. prefixLenToStartEclat=2 means that the Apriori should compute F3 (so that the prefix size is 2). <br/>
     * Note that the algorithm could still continue with Apriori if it decides that the dataset is 'sparse',
     * see {@link #currToPrevResSignificantIncreaseRatio}.
     */
    public final int prefixLenToStartEclat;

    //BigFim
    /**
     * How many partitions to use to read the input file. <br/>
     * In the actual distributed environment this is the number of physical machines holding different parts of the file.
     */
    public int inputNumParts = 2;
    public boolean isPersistInput = false;
    public boolean isPrintFks = true;
    /**
     * Decides whether the itemset is 'sparse' enough to continue with Apriori despite
     * the {@link #prefixLenToStartEclat} restriction. <br/>
     * The idea is that Apriori is very fast on sparse datasets. <br/>
     */
    public double currToPrevResSignificantIncreaseRatio = 2.0;
    //Eclat
    public boolean isUseDiffSets = true;
    public boolean isSqueezingEnabled = false;
    public boolean isCountingOnly = true;
    public boolean isStatGatheringEnabled = true;
    public Integer maxEclatNumParts;

    public BigFimAlgProperties(double minSupp, int prefixLenToStartEclat) {
        this.minSupp = minSupp;
        this.prefixLenToStartEclat = prefixLenToStartEclat;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
    }
}

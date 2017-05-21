package org.openu.fimcmp.algbase;

import java.io.Serializable;

/**
 * Properties relevant to all algorithms
 */
@SuppressWarnings("WeakerAccess")
public class CommonAlgProperties implements Serializable {
    /**
     * Min support as a ratio of itemset frequency to the total number of transactions
     */
    public final double minSupp;

    /**
     * How many partitions to use to read the input file. <br/>
     * In the actual distributed environment this is the number of physical machines holding different parts of the file.
     */
    public int inputNumParts = 2;

    public boolean isPersistInput = false;

    public boolean isPrintIntermediateRes = true;

    protected CommonAlgProperties(double minSupp) {
        this.minSupp = minSupp;
    }
}

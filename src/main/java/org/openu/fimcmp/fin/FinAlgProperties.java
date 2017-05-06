package org.openu.fimcmp.fin;

import org.openu.fimcmp.algbase.AlgBaseProperties;

/**
 * Properties for FIN+ algorithm
 */
public class FinAlgProperties extends AlgBaseProperties {
    protected FinAlgProperties(double minSupp) {
        super(minSupp);
    }

    /**
     * The required itemset length of the nodes processed sequentially on the driver machine.
     * E.g. '1' for individual items. <br/>
     * Note that each node will contain sons, i.e. '1' means a node for an individual frequent
     * item + its sons representing frequent pairs.
     */
    public int requiredItemsetLenForSeqProcessing = 2;

    enum RunType {SEQ_PURE_JAVA, SEQ_SPARK, PAR_SPARK}
    public RunType runType;
}

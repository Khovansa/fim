package org.openu.fimcmp.algs.fin;

import org.openu.fimcmp.algs.algbase.CommonAlgProperties;

/**
 * Properties for FIN+ algorithm
 */
@SuppressWarnings("WeakerAccess")
public class FinAlgProperties extends CommonAlgProperties {
    public FinAlgProperties(double minSupp) {
        super(minSupp);
    }

    enum RunType {SEQ_PURE_JAVA, SEQ_SPARK, PAR_SPARK}
    public RunType runType;

    /**
     * The required itemset length of the nodes processed sequentially on the driver machine.
     * E.g. '1' for individual items. <br/>
     * Note that each node will contain sons, i.e. '1' means a node for an individual frequent
     * item + its sons representing frequent pairs.
     */
    public int requiredItemsetLenForSeqProcessing = 1;

    public boolean isCountingOnly = true;
    public boolean isPrintAllFis = false;
}

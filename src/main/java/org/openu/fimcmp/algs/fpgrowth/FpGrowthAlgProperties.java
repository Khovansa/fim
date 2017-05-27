package org.openu.fimcmp.algs.fpgrowth;

import org.openu.fimcmp.algs.algbase.CommonAlgProperties;

/**
 * Holds all the properties for FpGrowth algorithm.
 */
@SuppressWarnings({"WeakerAccess"})
public class FpGrowthAlgProperties extends CommonAlgProperties {

    public FpGrowthAlgProperties(double minSupp) {
        super(minSupp);
    }
}

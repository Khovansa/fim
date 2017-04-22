package org.openu.fimcmp.algbase;

import java.io.Serializable;

/**
 * Holds counts related to the input transactions
 */
class TrsCount implements Serializable {
    final long totalTrs;
    final long minSuppCnt;

    TrsCount(long totalTrs, long minSuppCnt) {
        this.totalTrs = totalTrs;
        this.minSuppCnt = minSuppCnt;
    }
}

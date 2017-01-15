package org.openu.fimcmp.bigfim;

/**
 * Holds counts related to the input transactions
 */
class TrsCount {
    final long totalTrs;
    final long minSuppCnt;

    TrsCount(long totalTrs, long minSuppCnt) {
        this.totalTrs = totalTrs;
        this.minSuppCnt = minSuppCnt;
    }
}

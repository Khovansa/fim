package org.openu.fimcmp.apriori;

/**
 *
 */
interface NextSizeItemsetGenHelper {
    boolean isGoodNextSizeItemset(int item, int currItemsetRank);

    long[] getFkBitSet(int item);

    int getTotalCurrSizeRanks();
}

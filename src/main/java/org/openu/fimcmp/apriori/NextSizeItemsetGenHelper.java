package org.openu.fimcmp.apriori;

/**
 *
 */
interface NextSizeItemsetGenHelper {

    long[] getCurrRanksForNextSizeCandsBitSet(int item);

    int getTotalCurrSizeRanks();

    int getTotalFreqItems();
}

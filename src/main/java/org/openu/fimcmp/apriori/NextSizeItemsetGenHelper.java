package org.openu.fimcmp.apriori;

/**
 *
 */
interface NextSizeItemsetGenHelper {

    long[] getFkBitSet(int item);

    int getTotalCurrSizeRanks();

    int getTotalFreqItems();
}

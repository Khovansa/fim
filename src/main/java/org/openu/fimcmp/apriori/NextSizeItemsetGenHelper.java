package org.openu.fimcmp.apriori;

import org.openu.fimcmp.util.BitArrays;

import java.io.Serializable;

/**
 * Helps in next-size candidate itemsets in Apriori. <br/>
 *
 * The idea is that an itemset of size (k+1) {newItem, i1...ik} can be frequent only if all its subsets of size k
 * that include 'newItem' are frequent. <br/>
 */
public class NextSizeItemsetGenHelper implements Serializable {
    private final int totalFreqItems;
    private final int totalCurrSizeRanks;
    //nextSizeCandsR1ToRk[item][rankK] = true <-> itemset (item, k-FI from rankK) has chance to be frequent:
    private final long[][] nextSizeCandsR1ToRk;

    public static NextSizeItemsetGenHelper construct(
            FiRanksToFromItems mappersTillK, int totalFreqItems, int totalCurrSizeRanks) {
        long[][] nextSizeCandsR1ToRk = constructNextSizeCands(mappersTillK, totalFreqItems, totalCurrSizeRanks);
        return new NextSizeItemsetGenHelper(totalFreqItems, totalCurrSizeRanks, nextSizeCandsR1ToRk);
    }

    long[] getCurrRanksForNextSizeCandsBitSet(int item) {
        return nextSizeCandsR1ToRk[item];
    }

    int getTotalCurrSizeRanks() {
        return totalCurrSizeRanks;
    }

    int getTotalFreqItems() {
        return totalFreqItems;
    }

    //construct a bit set (item, k-FI as rank) -> whether has chance to be frequent:
    private static long[][] constructNextSizeCands(FiRanksToFromItems mappersTillK, int totalItems, int totalFks) {
        long[][] r1ToFkBitSet = new long[totalItems][BitArrays.requiredSize(totalFks, 0)];

        for (int item1 = 0; item1 < totalItems; ++item1) {
            long[] fkBitSet = r1ToFkBitSet[item1];
            for (int kFiRank = 0; kFiRank < totalFks; ++kFiRank) {
                if (mappersTillK.couldBeFrequentAndOrdered(item1, kFiRank)) {
                    BitArrays.set(fkBitSet, 0, kFiRank);
                }
            }
        }

        return r1ToFkBitSet;
    }

    private NextSizeItemsetGenHelper(int totalFreqItems, int totalCurrSizeRanks, long[][] nextSizeCandsR1ToRk) {
        this.totalFreqItems = totalFreqItems;
        this.totalCurrSizeRanks = totalCurrSizeRanks;
        this.nextSizeCandsR1ToRk = nextSizeCandsR1ToRk;
    }
}

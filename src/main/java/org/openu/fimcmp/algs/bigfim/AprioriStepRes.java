package org.openu.fimcmp.algs.bigfim;

import org.apache.commons.lang3.StringUtils;
import org.openu.fimcmp.itemset.FreqItemset;
import org.openu.fimcmp.algs.algbase.F1Context;
import org.openu.fimcmp.algs.apriori.CurrSizeFiRanks;
import org.openu.fimcmp.algs.apriori.FiRanksToFromItems;
import org.openu.fimcmp.algs.apriori.NextSizeItemsetGenHelper;
import org.openu.fimcmp.algs.apriori.TidsGenHelper;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Auxiliary class to hold results of a single Apriori step invoked from BigFimAlg
 */
class AprioriStepRes {
    final int kk;
    private final FiRanksToFromItems prevSizeAllRanks;
    private final List<int[]> fkAsArrays;
    private final List<int[]> fk;
    final CurrSizeFiRanks currSizeRanks;
    final FiRanksToFromItems currSizeAllRanks;

    AprioriStepRes(
            int kk, List<int[]> fkAsArrays,
            FiRanksToFromItems prevSizeAllRanks, int fkm1Size, F1Context cxt) {
        this.kk = kk;
        this.fkAsArrays = fkAsArrays;
        this.prevSizeAllRanks = prevSizeAllRanks;

        this.fk = cxt.apr.fkAsArraysToFilteredRankPairs(fkAsArrays);
        this.currSizeRanks = CurrSizeFiRanks.construct(this.fk, cxt.totalFreqItems, fkm1Size);
        this.currSizeAllRanks = prevSizeAllRanks.toNextSize(currSizeRanks);
    }

    int getFkSize() {
        return fk.size();
    }

    List<long[]> getItemsetBitsets(F1Context cxt) {
        return cxt.apr.fkAsArraysToItemsetBitsets(fkAsArrays, prevSizeAllRanks, cxt.totalFreqItems);
    }

    NextSizeItemsetGenHelper computeNextSizeGenHelper(int totalFreqItems) {
        return NextSizeItemsetGenHelper.construct(currSizeAllRanks, totalFreqItems, fk.size());
    }

    TidsGenHelper constructTidGenHelper(long totalTrs) {
        return currSizeRanks.constructTidGenHelper(fk, (int) totalTrs);
    }

    void print(F1Context cxt, boolean isPrintFks) {
        cxt.pp(String.format("F%s size: %s", kk, fk.size()));
        final int maxSampleSize = 10;

        if (isPrintFks) {
            List<FreqItemset> fkRes = cxt.apr.fkAsArraysToResItemsets(fkAsArrays, cxt.rankToItem, prevSizeAllRanks);
            fkRes = fkRes.stream()
                    .sorted((fi1, fi2) -> Integer.compare(fi2.freq, fi1.freq))
                    .collect(Collectors.toList());
            int sampleSize = Math.min(maxSampleSize, fkRes.size());
            fkRes = fkRes.subList(0, sampleSize);
            cxt.pp(String.format("F%s (sample size %s):\n%s", kk, sampleSize, StringUtils.join(fkRes, "\n")));
        }
    }
}

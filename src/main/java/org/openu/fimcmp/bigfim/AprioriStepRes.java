package org.openu.fimcmp.bigfim;

import org.apache.commons.lang3.StringUtils;
import org.openu.fimcmp.FreqItemset;
import org.openu.fimcmp.apriori.CurrSizeFiRanks;
import org.openu.fimcmp.apriori.FiRanksToFromItems;
import org.openu.fimcmp.apriori.NextSizeItemsetGenHelper;
import org.openu.fimcmp.apriori.TidsGenHelper;

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

    AprioriStepRes(int kk, List<int[]> fkAsArrays, FiRanksToFromItems prevSizeAllRanks, int fkm1Size, AprContext cxt) {
        this.kk = kk;
        this.fkAsArrays = fkAsArrays;
        this.prevSizeAllRanks = prevSizeAllRanks;

        this.fk = cxt.apr.fkAsArraysToRankPairs(fkAsArrays);
        this.currSizeRanks = CurrSizeFiRanks.construct(fk, cxt.totalFreqItems, fkm1Size);
        this.currSizeAllRanks = prevSizeAllRanks.toNextSize(currSizeRanks);
    }

    int getFkSize() {
        return fk.size();
    }

    List<long[]> getItemsetBitsets(AprContext cxt) {
        return cxt.apr.fkAsArraysToItemsetBitsets(fkAsArrays, prevSizeAllRanks, cxt.totalFreqItems);
    }

    NextSizeItemsetGenHelper computeNextSizeGenHelper(int totalFreqItems) {
        return NextSizeItemsetGenHelper.construct(currSizeAllRanks, totalFreqItems, fk.size());
    }

    TidsGenHelper constructTidGenHelper(long totalTrs) {
        return currSizeRanks.constructTidGenHelper(fk, (int) totalTrs);
    }

    void print(AprContext cxt, boolean isPrintFks) {
        cxt.pp(String.format("F%s size: %s", kk, fk.size()));

        if (isPrintFks) {
            List<FreqItemset> fkRes = cxt.apr.fkAsArraysToResItemsets(fkAsArrays, cxt.rankToItem, prevSizeAllRanks);
            fkRes = fkRes.stream()
                    .sorted((fi1, fi2) -> Integer.compare(fi2.freq, fi1.freq))
                    .collect(Collectors.toList());
            fkRes = fkRes.subList(0, Math.min(10, fkRes.size()));
            cxt.pp(String.format("F%s\n: %s", kk, StringUtils.join(fkRes, "\n")));
        }
    }
}

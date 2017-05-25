package org.openu.fimcmp.algs.bigfim;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.openu.fimcmp.itemset.FreqItemset;
import org.openu.fimcmp.itemset.FreqItemsetAsRanksBs;
import org.openu.fimcmp.result.FiResultHolder;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Holds the result of BigFim computation
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class BigFimResult {
    private final Map<String, Integer> itemToRank;
    private final String[] rankToItem;
    private final ArrayList<List<long[]>> aprioriFis;
    private final JavaRDD<FiResultHolder> optionalEclatFis;

    public BigFimResult(
            Map<String, Integer> itemToRank,
            String[] rankToItem,
            ArrayList<List<long[]>> aprioriFis,
            JavaRDD<FiResultHolder> optionalEclatFis) {
        this.itemToRank = itemToRank;
        this.rankToItem = rankToItem;
        this.aprioriFis = aprioriFis;
        this.optionalEclatFis = optionalEclatFis;
    }

    public long getTotalResultsCount() {
        long aprioriResCnt = getAprioriResCount();
        long eclatResCnt = getEclatResCount();
        return aprioriResCnt + eclatResCnt;
    }

    public long getAprioriResCount() {
        long res = 0;
        for (List<long[]> oneStepRes : aprioriFis) {
            res += oneStepRes.size();
        }
        return res;
    }

    public long getEclatResCount() {
        return (optionalEclatFis != null) ? optionalEclatFis.map(FiResultHolder::size).reduce((x, y) -> x+y) : 0L;
    }

    public List<FreqItemset> getAprioriFisOfLength(int itemsetLen) {
        return FreqItemsetAsRanksBs.toFreqItemsets(aprioriFis.get(itemsetLen-1), rankToItem);
    }

    public List<FreqItemset> getAllAprioriFis() {
        List<FreqItemset> res = new ArrayList<>((int)getAprioriResCount());
        for (List<long[]> oneStepFis : aprioriFis) {
            res.addAll(FreqItemsetAsRanksBs.toFreqItemsets(oneStepFis, rankToItem));
        }
        return res;
    }

    public List<FreqItemset> getEclatFis(long maxResCnt, boolean isSort) {
        if (optionalEclatFis == null) {
            return Collections.emptyList();
        }

        maxResCnt = Math.min(maxResCnt, getEclatResCount());
        JavaPairRDD<Integer, long[]> supportAndRanksRdd = optionalEclatFis
                .flatMap(FiResultHolder::fiAsBitsetIterator)
                .mapToPair(p -> new Tuple2<>(FreqItemsetAsRanksBs.extractSupportCnt(p), p));
        if (isSort) {
            supportAndRanksRdd = supportAndRanksRdd.sortByKey(false);
        }

        List<long[]> resAsBs = supportAndRanksRdd.values().take((int)maxResCnt);
        return FreqItemsetAsRanksBs.toFreqItemsets(resAsBs, rankToItem);
    }

    public void printCounts(StopWatch sw) {
        long aprioriCnt = getAprioriResCount();
        long eclatCnt = getEclatResCount();
        BigFimAlg.pp(sw, String.format(
                "Total results: %s (Apriori: %s, Eclat: %s)", (aprioriCnt + eclatCnt), aprioriCnt, eclatCnt));
    }

    public void printFreqItemsets(List<FreqItemset> fis, int maxRes) {
        maxRes = Math.min(maxRes, fis.size());
        fis = fis.subList(0, maxRes);

        int maxItemsetSize = 0;
        for (FreqItemset fi : fis) {
            maxItemsetSize = Math.max(maxItemsetSize, fi.itemset.size());
        }

        for (FreqItemset fi : fis) {
            print(fi.toString(itemToRank, maxItemsetSize));
        }
    }

    private static void print(String msg) {
        System.out.println(msg);
    }
}

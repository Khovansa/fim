package org.openu.fimcmp.bigfim;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.openu.fimcmp.FreqItemset;
import org.openu.fimcmp.FreqItemsetAsRanksBs;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Holds the result of BigFim computation
 */
public class BigFimResult {
    private final Map<String, Integer> itemToRank;
    private final String[] rankToItem;
    private final ArrayList<List<long[]>> aprioriFis;
    private final JavaRDD<List<long[]>> optionalEclatFis;

    public BigFimResult(
            Map<String, Integer> itemToRank,
            String[] rankToItem,
            ArrayList<List<long[]>> aprioriFis,
            JavaRDD<List<long[]>> optionalEclatFis) {
        this.itemToRank = itemToRank;
        this.rankToItem = rankToItem;
        this.aprioriFis = aprioriFis;
        this.optionalEclatFis = optionalEclatFis;
    }

    public int getTotalResultsCount() {
        int aprioriResCnt = getAprioriResCount();
        int eclatResCnt = getEclatResCount();
        return aprioriResCnt + eclatResCnt;
    }

    public int getAprioriResCount() {
        int res = 0;
        for (List<long[]> oneStepRes : aprioriFis) {
            res += oneStepRes.size();
        }
        return res;
    }

    public int getEclatResCount() {
        return (optionalEclatFis != null) ? optionalEclatFis.map(List::size).reduce((x, y) -> x+y) : 0;
    }

    public List<FreqItemset> getAprioriFisOfLength(int itemsetLen) {
        return FreqItemsetAsRanksBs.toFreqItemsets(aprioriFis.get(itemsetLen-1), rankToItem);
    }

    public List<FreqItemset> getAllAprioriFis() {
        List<FreqItemset> res = new ArrayList<>(getAprioriResCount());
        for (List<long[]> oneStepFis : aprioriFis) {
            res.addAll(FreqItemsetAsRanksBs.toFreqItemsets(oneStepFis, rankToItem));
        }
        return res;
    }

    public List<FreqItemset> getEclatFis(int maxResCnt, boolean isSort) {
        if (optionalEclatFis == null) {
            return Collections.emptyList();
        }

        maxResCnt = Math.min(maxResCnt, getEclatResCount());
        JavaPairRDD<Integer, long[]> supportAndRanksRdd = optionalEclatFis
                .flatMap(List::iterator)
                .mapToPair(p -> new Tuple2<>(FreqItemsetAsRanksBs.extractSupportCnt(p), p));
        if (isSort) {
            supportAndRanksRdd = supportAndRanksRdd.sortByKey(false);
        }

        List<long[]> resAsBs = supportAndRanksRdd.values().take(maxResCnt);
        return FreqItemsetAsRanksBs.toFreqItemsets(resAsBs, rankToItem);
    }

    public void printCounts() {
        int aprioriCnt = getAprioriResCount();
        int eclatCnt = getEclatResCount();
        print(String.format("Total results: %s (Apriori: %s, Eclat: %s", (aprioriCnt + eclatCnt), aprioriCnt, eclatCnt));
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

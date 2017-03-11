package org.openu.fimcmp.bigfim;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.openu.fimcmp.BasicOps;
import org.openu.fimcmp.FreqItemsetAsRanksBs;
import org.openu.fimcmp.apriori.AprioriAlg;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Auxiliary class to hold all the context of Apriori algorithm execution by BigFimAlg
 */
class AprContext {
    final AprioriAlg<String> apr;
    private final List<Tuple2<String, Integer>> sortedF1;
    final TrsCount cnts;
    final int totalFreqItems;
    final Map<String, Integer> itemToRank;
    final String[] rankToItem;
    private final StopWatch sw;

    AprContext(AprioriAlg<String> apr, List<Tuple2<String, Integer>> sortedF1, TrsCount cnts, StopWatch sw) {
        this.apr = apr;

        this.sortedF1 = sortedF1;
        this.totalFreqItems = sortedF1.size();
        this.itemToRank = BasicOps.itemToRank(BasicOps.toItems(sortedF1));
        this.rankToItem = BasicOps.getRankToItem(itemToRank);

        this.cnts = cnts;
        this.sw = sw;
    }

    JavaRDD<int[]> computeRddRanks1(JavaRDD<String[]> trs) {
        SerToRanks1 toRanks1 = new SerToRanks1(itemToRank);
        JavaRDD<int[]> res = trs.map(toRanks1::getMappedFilteredAndSortedTrs);
        res = res.persist(StorageLevel.MEMORY_ONLY_SER());

        pp("Filtered and saved RDD ranks 1");

        return res;
    }

    //Auxiliary - required since the 'AprContext' is not serializable:
    private static class SerToRanks1 implements Serializable {
        final Map<String, Integer> itemToRank;

        private SerToRanks1(Map<String, Integer> itemToRank) {
            this.itemToRank = itemToRank;
        }

        int[] getMappedFilteredAndSortedTrs(String[] tr) {
            return BasicOps.getMappedFilteredAndSortedTrs(tr, itemToRank);
        }
    }

    List<long[]> freqItemRanksAsItemsetBs() {
        List<long[]> res = new ArrayList<>(totalFreqItems);
        for (Tuple2<String, Integer> itemWithSupp : sortedF1) {
            int rank = itemToRank.get(itemWithSupp._1);
            int[] itemset = new int[]{rank};
            res.add(FreqItemsetAsRanksBs.toBitSet(itemWithSupp._2, itemset, totalFreqItems));
        }
        return res;
    }

    void pp(Object msg) {
        BigFimAlg.pp(sw, msg);
    }
}

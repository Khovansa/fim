package org.openu.fimcmp.algbase;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.openu.fimcmp.FreqItemsetAsRanksBs;
import org.openu.fimcmp.apriori.AprioriAlg;
import org.openu.fimcmp.result.FiResultHolder;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Auxiliary class to hold all the context related to F1 and item to rank mapping.
 */
public class F1Context {
    private final StopWatch sw;
    private final List<Tuple2<String, Integer>> sortedF1;

    public final AprioriAlg<String> apr;
    public final long totalTrs;
    public final long minSuppCnt;
    public final int totalFreqItems;
    public final Map<String, Integer> itemToRank;
    public final String[] rankToItem;

    F1Context(AprioriAlg<String> apr, List<Tuple2<String, Integer>> sortedF1, TrsCount cnts, StopWatch sw) {
        this.apr = apr;
        this.sortedF1 = sortedF1;
        this.totalFreqItems = sortedF1.size();
        this.itemToRank = BasicOps.itemToRank(BasicOps.toItems(sortedF1));
        this.rankToItem = BasicOps.getRankToItem(itemToRank);

        this.totalTrs = cnts.totalTrs;
        this.minSuppCnt = cnts.minSuppCnt;

        this.sw = sw;
    }

    public JavaRDD<int[]> computeRddRanks1(JavaRDD<String[]> trs) {
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

    public List<long[]> freqItemRanksAsItemsetBs() {
        List<long[]> res = new ArrayList<>(totalFreqItems);
        for (Tuple2<String, Integer> itemWithSupp : sortedF1) {
            int rank = itemToRank.get(itemWithSupp._1);
            int[] itemset = new int[]{rank};
            res.add(FreqItemsetAsRanksBs.toBitSet(itemWithSupp._2, itemset, totalFreqItems));
        }
        return res;
    }

    public void updateByF1(FiResultHolder resultHolder) {
        for (Tuple2<String, Integer> itemWithSupp : sortedF1) {
            int rank = itemToRank.get(itemWithSupp._1);
            int[] itemset = new int[]{rank};
            int supportCnt = itemWithSupp._2;
            resultHolder.addFrequentItemset(supportCnt, itemset);
        }
    }

    public void pp(Object msg) {
        AlgBase.pp(sw, msg);
    }

    public void printRankToItem() {
        for (int rank=0; rank<rankToItem.length; ++rank) {
            System.out.println(String.format("%-10s %s", rank, rankToItem[rank]));
        }
    }
}

package org.openu.fimcmp.apriori;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.junit.Before;
import org.junit.Test;
import org.openu.fimcmp.*;
import org.openu.fimcmp.util.BitArrays;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AprioriAlgIT extends AlgITBase {
    @SuppressWarnings("FieldCanBeLocal")
    private AprioriAlg<String> apr;

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void test() throws Exception {
        runIt();
//        Thread.sleep(100_000_000);
    }

    private void runIt() throws Exception {
        final PrepStepOutputAsArr prep = prepareAsArr("pumsb.dat", 0.4, false, 2);
//        final PrepStepOutputAsArr prep = prepareAsArr("my.small.txt", 0.1, false, 2);
        apr = new AprioriAlg<>(prep.minSuppCount);
        List<String> sortedF1 = apr.computeF1(prep.trs);
        final int totalFreqItems = sortedF1.size();
        pp("F1 size = " + totalFreqItems);
        pp(sortedF1);
        Map<String, Integer> itemToRank = BasicOps.itemToRank(sortedF1);
        //from now on, the items are [0, sortedF1.size), 0 denotes the most frequent item

        JavaRDD<int[]> filteredTrs = prep.trs.map(t -> BasicOps.getMappedFilteredAndSortedTrs(t, itemToRank));
        filteredTrs = filteredTrs.persist(StorageLevel.MEMORY_ONLY_SER());
//        filteredTrs = filteredTrs.persist(StorageLevel.MEMORY_AND_DISK_SER());
        pp("filtered and saved");

        List<int[]> f2AsArrays = apr.computeF2_Part(filteredTrs, totalFreqItems);
        pp("F2 as arrays size: " + f2AsArrays.size());
        List<int[]> f2 = apr.fkAsArraysToRankPairs(f2AsArrays, prep.minSuppCount);
        pp("F2 size: " + f2.size());
        FiRanksToFromItems fiRanksToFromItemsR1 = new FiRanksToFromItems();
        List<FreqItemset<String>> f2Res =
                apr.fkAsArraysToResItemsets(prep.minSuppCount, f2AsArrays, itemToRank, fiRanksToFromItemsR1);
        f2Res = f2Res.stream().sorted((fi1, fi2) -> Integer.compare(fi2.freq, fi1.freq)).collect(Collectors.toList());
        pp("F2: " + StringUtils.join(f2Res.subList(0, Math.min(3, f2Res.size())), "\n"));

        CurrSizeFiRanks preprocessedF2 = CurrSizeFiRanks.construct(f2, totalFreqItems, totalFreqItems);
        FiRanksToFromItems fiRanksToFromItemsR2 = fiRanksToFromItemsR1.toNextSize(preprocessedF2);
        NextSizeItemsetGenHelper f3GenHelper = NextSizeItemsetGenHelper.construct(
                fiRanksToFromItemsR2, totalFreqItems, f2.size());
        JavaRDD<Tuple2<int[], long[]>> ranks1And2 = apr.toRddOfRanks1And2(filteredTrs, preprocessedF2);
        ranks1And2 = ranks1And2.persist(StorageLevel.MEMORY_ONLY_SER());
//        ranks1And2 = ranks1And2.persist(StorageLevel.MEMORY_AND_DISK_SER());
        pp("zzz");
        List<int[]> f3AsArrays = apr.computeFk_Part(3, ranks1And2, f3GenHelper);
        pp("F3 as arrays size: " + f3AsArrays.size());
        List<int[]> f3 = apr.fkAsArraysToRankPairs(f3AsArrays, prep.minSuppCount);
        pp("F3 size: " + f3.size());
        List<FreqItemset<String>> f3Res = apr.fkAsArraysToResItemsets(
                prep.minSuppCount, f3AsArrays, itemToRank, fiRanksToFromItemsR2);
        f3Res = f3Res.stream().sorted((fi1, fi2) -> Integer.compare(fi2.freq, fi1.freq)).collect(Collectors.toList());
        pp("F3: " + StringUtils.join(f3Res.subList(0, Math.min(10, f3Res.size())), "\n"));

        CurrSizeFiRanks preprocessedF3 = CurrSizeFiRanks.construct(f3, totalFreqItems, f2.size());
        JavaRDD<Tuple2<int[], long[]>> ranks1And3 = apr.toRddOfRanks1AndK(ranks1And2, preprocessedF3);
//        ranks1And3 = ranks1And3.persist(StorageLevel.MEMORY_AND_DISK_SER());

        TidsGenHelper tidsGenHelper = preprocessedF3.constructTidGenHelper(f3, (int) prep.totalTrs);

        JavaRDD<long[]> kRanksBsRdd = ranks1And3.map(r1And3 -> r1And3._2);
        filteredTrs.unpersist();
        kRanksBsRdd = kRanksBsRdd.persist(StorageLevel.MEMORY_AND_DISK_SER());
//        ranks1And2.unpersist();
        FiRanksToFromItems fiRanksToFromItemsR3 = fiRanksToFromItemsR2.toNextSize(preprocessedF3);
        PairRanks r3ToR2AndR1 = fiRanksToFromItemsR3.constructRkToRkm1AndR1ForMaxK();

        pp("Starting collecting the TIDs");
        JavaRDD<long[][]> rankToTidBsRdd = apr.computeCurrRankToTidBitSet_Part(kRanksBsRdd, tidsGenHelper);
        rankToTidBsRdd = rankToTidBsRdd.persist(StorageLevel.MEMORY_AND_DISK_SER());
        long[][] rankKToTids = apr.mergePartitions(rankToTidBsRdd, tidsGenHelper);
        ranks1And2.unpersist();
        kRanksBsRdd.unpersist();

        pp("TIDs:");
        for (int rankK = 0, cnt = 0; rankK < 500 && cnt < 20; ++rankK) {
            if (rankKToTids[rankK] != null) {
                ++cnt;
                System.out.println(Arrays.toString(TidMergeSet.describeAsList(rankKToTids[rankK])));
            }
        }

        JavaPairRDD<Integer, List<long[]>> prefRkToTidSets = apr.groupTidSetsByRankKm1(rankToTidBsRdd, r3ToR2AndR1);
        JavaPairRDD<Integer, ItemsetAndTidsCollection> prefRkToEclatInput =
                prefRkToTidSets.mapValues(tidSets ->
                        TidMergeSet.mergeTidSetsWithSameRankDropMetadata(tidSets, tidsGenHelper, fiRanksToFromItemsR3));
        pp("Num parts: " + prefRkToTidSets.getNumPartitions());
        ItemsetAndTidsCollection eclatInput1 = prefRkToEclatInput.sortByKey().first()._2;
        pp("Itemset size: " + eclatInput1.getItemsetSize());
        pp("Total tids: " + eclatInput1.getTotalTids());
        pp("Elems size: " + eclatInput1.getItemsetAndTidsList().size());
        ItemsetAndTids firstEclatElem = eclatInput1.getItemsetAndTidsList().get(0);
        pp("Support cnt #1: " + firstEclatElem.getSupportCnt());
        pp("Support cnt #1 (cardinality): " + BitArrays.cardinality(firstEclatElem.getTidBitSet(), 0));
        String[] r1ToItem = BasicOps.getRankToItem(itemToRank);
        pp("Itemset #1: " + firstEclatElem.toOrigItemsetForDebug(r1ToItem));
//        pp("TIDs of itemset #1: " + Arrays.toString(BitArrays.asNumbers(firstEclatElem.getTidBitSet(), 0)));

        pp("r2 -> r3s:");
        List<Tuple2<List<String>, List<List<String>>>> r2ToR3sList = prefRkToTidSets.sortByKey()
                .map(t -> new Tuple2<>(
                        fiRanksToFromItemsR3.getOrigItemsetByRank(t._1, 2, r1ToItem),
                        fiRanksToFromItemsR3.toOrigItemsetsForDebug(t._2, 3, r1ToItem, 30)))
                .collect().subList(0, 20);
        for (Tuple2<List<String>, List<List<String>>> r2ToR3s : r2ToR3sList) {
            System.out.println(String.format("%-20s -> %s", r2ToR3s._1, r2ToR3s._2));
        }

//        pp("zzz");
//        List<int[]> f4AsArrays = apr.computeFk_Part(4, ranks1And3, preprocessedF3);
//        pp("F4 as arrays size: "+f4AsArrays.size());
//        List<int[]> f4 = apr.fkAsArraysToRankPairs(f4AsArrays);
//        pp("F4 size: "+f4.size());
//        List<FreqItemset<String>> f4Res =
//          apr.fkAsArraysToResItemsets(f4AsArrays, itemToRank, new new FiRanksToFromItems(preprocessedF3, preprocessedF2));
//        f4Res = f4Res.stream().sorted((fi1, fi2) -> Integer.compare(fi2.freq, fi1.freq)).collect(Collectors.toList());
//        pp("F4: " + StringUtils.join(f4Res.subList(0, Math.min(3, f4Res.size())), "\n"));
    }
}
